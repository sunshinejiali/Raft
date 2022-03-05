// Copyright 2015 PingCAP, Inc.
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

// Note: All the tests in this file will be executed sequentially.

package executor_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testkit"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, logutil.EmptyFileLogConfig, false))
	TestingT(t)
}

var _ = Suite(&seqTestSuite{})

type seqTestSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	domain    *domain.Domain
	*parser.Parser
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in executor test")

func (s *seqTestSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		s.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(s.cluster)
		s.mvccStore = mocktikv.MustNewMVCCStore()
		store, err := mockstore.NewMockTikvStore(
			mockstore.WithCluster(s.cluster),
			mockstore.WithMVCCStore(s.mvccStore),
		)
		c.Assert(err, IsNil)
		s.store = store
		session.SetSchemaLease(0)
		session.DisableStats4Test()
	}
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.domain = d
}

func (s *seqTestSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

func (s *seqTestSuite) TestEarlyClose(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table earlyclose (id int primary key)")

	N := 100
	// Insert N rows.
	var values []string
	for i := 0; i < N; i++ {
		values = append(values, fmt.Sprintf("(%d)", i))
	}
	tk.MustExec("insert earlyclose values " + strings.Join(values, ","))

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("earlyclose"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	// Split the table.
	s.cluster.SplitTable(s.mvccStore, tblID, N/2)

	ctx := context.Background()
	for i := 0; i < N/2; i++ {
		rss, err1 := tk.Se.Execute(ctx, "select * from earlyclose order by id")
		c.Assert(err1, IsNil)
		rs := rss[0]
		req := rs.NewChunk()
		err = rs.Next(ctx, req)
		c.Assert(err, IsNil)
		rs.Close()
	}

	// Goroutine should not leak when error happen.
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/handleTaskOnceError", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/handleTaskOnceError"), IsNil)
	}()
	rss, err := tk.Se.Execute(ctx, "select * from earlyclose")
	c.Assert(err, IsNil)
	rs := rss[0]
	req := rs.NewChunk()
	err = rs.Next(ctx, req)
	c.Assert(err, NotNil)
	rs.Close()
}

// TestIndexDoubleReadClose checks that when a index double read returns before reading all the rows, the goroutine doesn't
// leak. For testing distsql with multiple regions, we need to manually split a mock TiKV.
func (s *seqTestSuite) TestIndexDoubleReadClose(c *C) {
	if _, ok := s.store.GetClient().(*tikv.CopClient); !ok {
		// Make sure the store is tikv store.
		return
	}
	originSize := atomic.LoadInt32(&executor.LookupTableTaskChannelSize)
	atomic.StoreInt32(&executor.LookupTableTaskChannelSize, 1)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_index_lookup_size = '10'")
	tk.MustExec("use test")
	tk.MustExec("create table dist (id int primary key, c_idx int, c_col int, index (c_idx))")

	// Insert 100 rows.
	var values []string
	for i := 0; i < 100; i++ {
		values = append(values, fmt.Sprintf("(%d, %d, %d)", i, i, i))
	}
	tk.MustExec("insert dist values " + strings.Join(values, ","))

	rs, err := tk.Exec("select * from dist where c_idx between 0 and 100")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	err = rs.Next(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(err, IsNil)
	keyword := "pickAndExecTask"
	rs.Close()
	time.Sleep(time.Millisecond * 10)
	c.Check(checkGoroutineExists(keyword), IsFalse)
	atomic.StoreInt32(&executor.LookupTableTaskChannelSize, originSize)
}

func checkGoroutineExists(keyword string) bool {
	buf := new(bytes.Buffer)
	profile := pprof.Lookup("goroutine")
	profile.WriteTo(buf, 1)
	str := buf.String()
	return strings.Contains(str, keyword)
}

func (s *seqTestSuite) TestMaxDeltaSchemaCount(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	c.Assert(variable.GetMaxDeltaSchemaCount(), Equals, int64(variable.DefTiDBMaxDeltaSchemaCount))
	gvc := domain.GetDomain(tk.Se).GetGlobalVarsCache()
	gvc.Disable()

	tk.MustExec("set @@global.tidb_max_delta_schema_count= -1")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_max_delta_schema_count value: '-1'"))
	// Make sure a new session will load global variables.
	tk.Se = nil
	tk.MustExec("use test")
	c.Assert(variable.GetMaxDeltaSchemaCount(), Equals, int64(100))
	tk.MustExec(fmt.Sprintf("set @@global.tidb_max_delta_schema_count= %v", uint64(math.MaxInt64)))
	tk.MustQuery("show warnings;").Check(testkit.Rows(fmt.Sprintf("Warning 1292 Truncated incorrect tidb_max_delta_schema_count value: '%d'", uint64(math.MaxInt64))))
	tk.Se = nil
	tk.MustExec("use test")
	c.Assert(variable.GetMaxDeltaSchemaCount(), Equals, int64(16384))
	_, err := tk.Exec("set @@global.tidb_max_delta_schema_count= invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@global.tidb_max_delta_schema_count= 2048")
	tk.Se = nil
	tk.MustExec("use test")
	c.Assert(variable.GetMaxDeltaSchemaCount(), Equals, int64(2048))
	tk.MustQuery("select @@global.tidb_max_delta_schema_count").Check(testkit.Rows("2048"))
}
