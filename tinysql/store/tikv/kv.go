// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	pd "github.com/pingcap-incubator/tinykv/scheduler/client"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

type storeCache struct {
	sync.Mutex
	cache map[string]*TinykvStore
}

var mc storeCache

// Driver implements engine Driver.
type Driver struct {
}

func createEtcdKV(addrs []string) (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:        addrs,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cli, nil
}

// Open opens or creates an TiKV storage with given path.
// Path example: tikv://etcd-node1:port,etcd-node2:port?cluster=1&disableGC=false
func (d Driver) Open(path string) (kv.Storage, error) {
	mc.Lock()
	defer mc.Unlock()

	etcdAddrs, disableGC, err := parsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pdCli, err := pd.NewClient(etcdAddrs, pd.SecurityOption{})

	if err != nil {
		return nil, errors.Trace(err)
	}

	// FIXME: uuid will be a very long and ugly string, simplify it.
	uuid := fmt.Sprintf("tikv-%v", pdCli.GetClusterID(context.TODO()))
	if store, ok := mc.cache[uuid]; ok {
		return store, nil
	}

	spkv, err := NewEtcdSafePointKV(etcdAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, err := newTikvStore(uuid, &codecPDClient{pdCli}, spkv, newRPCClient(), !disableGC)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.etcdAddrs = etcdAddrs

	mc.cache[uuid] = s
	return s, nil
}

// EtcdBackend is used for judging a storage is a real TiKV.
type EtcdBackend interface {
	EtcdAddrs() []string
	TLSConfig() *tls.Config
}

// update oracle's lastTS every 2000ms.
var oracleUpdateInterval = 2000

type TinykvStore struct {
	clusterID    uint64
	uuid         string
	oracle       oracle.Oracle
	client       Client
	PdClient     pd.Client
	regionCache  *RegionCache
	lockResolver *LockResolver
	etcdAddrs    []string
	mock         bool
	enableGC     bool

	kv        SafePointKV
	safePoint uint64
	spTime    time.Time
	spMutex   sync.RWMutex  // this is used to update safePoint and spTime
	closed    chan struct{} // this is used to nofity when the store is closed

	replicaReadSeed uint32 // this is used to load balance followers / learners when replica read is enabled
}

func GetRegionCacheFromStore(store Storage) *RegionCache {
	store, ok := store.(*TinykvStore)
	if ok {
		return store.GetRegionCache()
	}
	return nil
}

func (s *TinykvStore) UpdateSPCache(cachedSP uint64, cachedTime time.Time) {
	s.spMutex.Lock()
	s.safePoint = cachedSP
	s.spTime = cachedTime
	s.spMutex.Unlock()
}

func (s *TinykvStore) CheckVisibility(startTime uint64) error {
	s.spMutex.RLock()
	cachedSafePoint := s.safePoint
	cachedTime := s.spTime
	s.spMutex.RUnlock()
	diff := time.Since(cachedTime)

	if diff > (GcSafePointCacheInterval - gcCPUTimeInaccuracyBound) {
		return ErrPDServerTimeout.GenWithStackByArgs("start timestamp may fall behind safe point")
	}

	if startTime < cachedSafePoint {
		t1 := oracle.GetTimeFromTS(startTime)
		t2 := oracle.GetTimeFromTS(cachedSafePoint)
		return ErrGCTooEarly.GenWithStackByArgs(t1, t2)
	}

	return nil
}

func newTikvStore(uuid string, pdClient pd.Client, spkv SafePointKV, client Client, enableGC bool) (*TinykvStore, error) {
	o, err := oracles.NewPdOracle(pdClient, time.Duration(oracleUpdateInterval)*time.Millisecond)
	if err != nil {
		return nil, errors.Trace(err)
	}
	store := &TinykvStore{
		clusterID:       pdClient.GetClusterID(context.TODO()),
		uuid:            uuid,
		oracle:          o,
		client:          client,
		PdClient:        pdClient,
		regionCache:     NewRegionCache(pdClient),
		kv:              spkv,
		safePoint:       0,
		spTime:          time.Now(),
		closed:          make(chan struct{}),
		replicaReadSeed: rand.Uint32(),
	}
	store.lockResolver = newLockResolver(store)
	store.enableGC = enableGC

	go store.runSafePointChecker()

	return store, nil
}

func (s *TinykvStore) EtcdAddrs() []string {
	return s.etcdAddrs
}

func (s *TinykvStore) runSafePointChecker() {
	d := gcSafePointUpdateInterval
	for {
		select {
		case spCachedTime := <-time.After(d):
			cachedSafePoint, err := loadSafePoint(s.GetSafePointKV())
			if err == nil {
				s.UpdateSPCache(cachedSafePoint, spCachedTime)
				d = gcSafePointUpdateInterval
			} else {

				logutil.BgLogger().Error("fail to load safepoint from pd", zap.Error(err))
				d = gcSafePointQuickRepeatInterval
			}
		case <-s.Closed():
			return
		}
	}
}

func (s *TinykvStore) Begin() (kv.Transaction, error) {
	txn, err := newTiKVTxn(s)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return txn, nil
}

// BeginWithStartTS begins a transaction with startTS.
func (s *TinykvStore) BeginWithStartTS(startTS uint64) (kv.Transaction, error) {
	txn, err := newTikvTxnWithStartTS(s, startTS)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return txn, nil
}

func (s *TinykvStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	snapshot := newTiKVSnapshot(s, ver)

	return snapshot, nil
}

func (s *TinykvStore) Close() error {
	mc.Lock()
	defer mc.Unlock()

	delete(mc.cache, s.uuid)
	s.oracle.Close()
	s.PdClient.Close()

	close(s.closed)
	if err := s.client.Close(); err != nil {
		return errors.Trace(err)
	}

	s.regionCache.Close()
	return nil
}

func (s *TinykvStore) UUID() string {
	return s.uuid
}

func (s *TinykvStore) CurrentVersion() (kv.Version, error) {
	bo := NewBackoffer(context.Background(), tsoMaxBackoff)
	startTS, err := s.getTimestampWithRetry(bo)
	if err != nil {
		return kv.NewVersion(0), errors.Trace(err)
	}
	return kv.NewVersion(startTS), nil
}

func (s *TinykvStore) getTimestampWithRetry(bo *Backoffer) (uint64, error) {
	for {
		startTS, err := s.oracle.GetTimestamp(bo.ctx)
		// mockGetTSErrorInRetry should wait MockCommitErrorOnce first, then will run into retry() logic.
		// Then mockGetTSErrorInRetry will return retryable error when first retry.
		// Before PR #8743, we don't cleanup txn after meet error such as error like: PD server timeout
		// This may cause duplicate data to be written.
		failpoint.Inject("mockGetTSErrorInRetry", func(val failpoint.Value) {
			if val.(bool) && !kv.IsMockCommitErrorEnable() {
				err = ErrPDServerTimeout.GenWithStackByArgs("mock PD timeout")
			}
		})

		if err == nil {
			return startTS, nil
		}
		err = bo.Backoff(BoPDRPC, errors.Errorf("get timestamp failed: %v", err))
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
}

func (s *TinykvStore) GetClient() kv.Client {
	return &CopClient{
		store: s,
	}
}

func (s *TinykvStore) GetOracle() oracle.Oracle {
	return s.oracle
}

func (s *TinykvStore) Name() string {
	return "TiKV"
}

func (s *TinykvStore) Describe() string {
	return "TiKV is a distributed transactional key-value database"
}

func (s *TinykvStore) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	return nil, kv.ErrNotImplemented
}

func (s *TinykvStore) SupportDeleteRange() (supported bool) {
	return !s.mock
}

func (s *TinykvStore) SendReq(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	sender := NewRegionRequestSender(s.regionCache, s.client)
	return sender.SendReq(bo, req, regionID, timeout)
}

func (s *TinykvStore) GetRegionCache() *RegionCache {
	return s.regionCache
}

func (s *TinykvStore) GetLockResolver() *LockResolver {
	return s.lockResolver
}

func (s *TinykvStore) Closed() <-chan struct{} {
	return s.closed
}

func (s *TinykvStore) GetSafePointKV() SafePointKV {
	return s.kv
}

func (s *TinykvStore) SetOracle(oracle oracle.Oracle) {
	s.oracle = oracle
}

func (s *TinykvStore) SetTiKVClient(client Client) {
	s.client = client
}

func (s *TinykvStore) GetTiKVClient() (client Client) {
	return s.client
}

func parsePath(path string) (etcdAddrs []string, disableGC bool, err error) {
	var u *url.URL
	u, err = url.Parse(path)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	if strings.ToLower(u.Scheme) != "tikv" {
		err = errors.Errorf("Uri scheme expected[tikv] but found [%s]", u.Scheme)
		logutil.BgLogger().Error("parsePath error", zap.Error(err))
		return
	}
	switch strings.ToLower(u.Query().Get("disableGC")) {
	case "true":
		disableGC = true
	case "false", "":
	default:
		err = errors.New("disableGC flag should be true/false")
		return
	}
	etcdAddrs = strings.Split(u.Host, ",")
	return
}

func init() {
	mc.cache = make(map[string]*TinykvStore)
	rand.Seed(time.Now().UnixNano())
}
