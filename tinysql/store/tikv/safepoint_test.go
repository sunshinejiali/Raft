// Copyright 2017 PingCAP, Inc.
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
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
)

type testSafePointSuite struct {
	OneByOneSuite
	store  *TinykvStore
	prefix string
}

var _ = Suite(&testSafePointSuite{})

func (s *testSafePointSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	s.store = NewTestStore(c).(*TinykvStore)
	s.prefix = fmt.Sprintf("seek_%d", time.Now().Unix())
}

func (s *testSafePointSuite) TearDownSuite(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testSafePointSuite) beginTxn(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testSafePointSuite) waitUntilErrorPlugIn(t uint64) {
	for {
		saveSafePoint(s.store.GetSafePointKV(), t+10)
		cachedTime := time.Now()
		newSafePoint, err := loadSafePoint(s.store.GetSafePointKV())
		if err == nil {
			s.store.UpdateSPCache(newSafePoint, cachedTime)
			break
		}
		time.Sleep(time.Second)
	}
}

func (s *testSafePointSuite) TestSafePoint(c *C) {
	txn := s.beginTxn(c)
	for i := 0; i < 10; i++ {
		err := txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
		c.Assert(err, IsNil)
	}
	err := txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// for txn get
	txn2 := s.beginTxn(c)
	_, err = txn2.Get(context.TODO(), encodeKey(s.prefix, s08d("key", 0)))
	c.Assert(err, IsNil)

	s.waitUntilErrorPlugIn(txn2.startTS)

	_, geterr2 := txn2.Get(context.TODO(), encodeKey(s.prefix, s08d("key", 0)))
	c.Assert(geterr2, NotNil)
	isFallBehind := terror.ErrorEqual(errors.Cause(geterr2), ErrGCTooEarly)
	isMayFallBehind := terror.ErrorEqual(errors.Cause(geterr2), ErrPDServerTimeout.GenWithStackByArgs("start timestamp may fall behind safe point"))
	isBehind := isFallBehind || isMayFallBehind
	c.Assert(isBehind, IsTrue)

	// for txn seek
	txn3 := s.beginTxn(c)

	s.waitUntilErrorPlugIn(txn3.startTS)

	_, seekerr := txn3.Iter(encodeKey(s.prefix, ""), nil)
	c.Assert(seekerr, NotNil)
	isFallBehind = terror.ErrorEqual(errors.Cause(geterr2), ErrGCTooEarly)
	isMayFallBehind = terror.ErrorEqual(errors.Cause(geterr2), ErrPDServerTimeout.GenWithStackByArgs("start timestamp may fall behind safe point"))
	isBehind = isFallBehind || isMayFallBehind
	c.Assert(isBehind, IsTrue)
}
