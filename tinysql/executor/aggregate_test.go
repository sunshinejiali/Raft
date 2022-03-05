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

package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

type testSuiteAgg struct {
	*baseTestSuite
	testData testutil.TestData
}

func (s *testSuiteAgg) SetUpSuite(c *C) {
	s.baseTestSuite.SetUpSuite(c)
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "agg_suite")
	c.Assert(err, IsNil)
}

func (s *testSuiteAgg) TearDownSuite(c *C) {
	s.baseTestSuite.TearDownSuite(c)
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testSuiteAgg) TestSelectDistinct(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "select_distinct_test")

	tk.MustExec("begin")
	r := tk.MustQuery("select distinct name from select_distinct_test;")
	r.Check(testkit.Rows("hello"))
	tk.MustExec("commit")

}

func (s *testSuiteAgg) TestAggPushDown(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("alter table t add index idx(a, b, c)")
	// test for empty table
	tk.MustQuery("select count(a) from t group by a;").Check(testkit.Rows())
	tk.MustQuery("select count(a) from t;").Check(testkit.Rows("0"))
	// test for one row
	tk.MustExec("insert t values(0,0,0)")
	tk.MustQuery("select distinct b from t").Check(testkit.Rows("0"))
	tk.MustQuery("select count(b) from t group by a;").Check(testkit.Rows("1"))
	// test for rows
	tk.MustExec("insert t values(1,1,1),(3,3,6),(3,2,5),(2,1,4),(1,1,3),(1,1,2);")
	tk.MustQuery("select count(a) from t where b>0 group by a, b;").Sort().Check(testkit.Rows("1", "1", "1", "3"))
	tk.MustQuery("select count(a) from t where b>0 group by a, b order by a;").Check(testkit.Rows("3", "1", "1", "1"))
	tk.MustQuery("select count(a) from t where b>0 group by a, b order by a limit 1;").Check(testkit.Rows("3"))
}

func (s *testSuiteAgg) TestAggEliminator(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustQuery("select min(a), min(a) from t").Check(testkit.Rows("<nil> <nil>"))
	tk.MustExec("insert into t values(1, -1), (2, -2), (3, 1), (4, NULL)")
	tk.MustQuery("select max(a) from t").Check(testkit.Rows("4"))
	tk.MustQuery("select min(b) from t").Check(testkit.Rows("-2"))
	tk.MustQuery("select max(b*b) from t").Check(testkit.Rows("4"))
	tk.MustQuery("select min(b*b) from t").Check(testkit.Rows("1"))
}

func (s *testSuiteAgg) TestInjectProjBelowTopN(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (i int);")
	tk.MustExec("insert into t values (1), (1), (1),(2),(3),(2),(3),(2),(3);")
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}
