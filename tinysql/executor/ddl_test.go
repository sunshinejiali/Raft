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

package executor_test

import (
	"fmt"
	. "github.com/pingcap/check"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
	"math"
	"strings"
)

func (s *testSuite6) TestCreateDropDatabase(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists drop_test;")
	tk.MustExec("drop database if exists drop_test;")
	tk.MustExec("create database drop_test;")
	tk.MustExec("use drop_test;")
	tk.MustExec("drop database drop_test;")
	_, err := tk.Exec("drop table t;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
	err = tk.ExecToErr("select * from t;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())

	_, err = tk.Exec("drop database mysql")
	c.Assert(err, NotNil)
}

func (s *testSuite6) TestCreateDropTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists drop_test (a int)")
	tk.MustExec("drop table if exists drop_test")
	tk.MustExec("create table drop_test (a int)")
	tk.MustExec("drop table drop_test")

	_, err := tk.Exec("drop table mysql.gc_delete_range")
	c.Assert(err, NotNil)
}

func (s *testSuite6) TestCreateDropIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists drop_test (a int)")
	tk.MustExec("create index idx_a on drop_test (a)")
	tk.MustExec("drop index idx_a on drop_test")
	tk.MustExec("drop table drop_test")
}

func (s *testSuite6) TestAddNotNullColumnNoDefault(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table nn (c1 int)")
	tk.MustExec("insert nn values (1), (2)")
	tk.MustExec("alter table nn add column c2 int not null")

	tbl, err := domain.GetDomain(tk.Se).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("nn"))
	c.Assert(err, IsNil)
	col2 := tbl.Meta().Columns[1]
	c.Assert(col2.DefaultValue, IsNil)
	c.Assert(col2.OriginDefaultValue, Equals, "0")

	tk.MustQuery("select * from nn").Check(testkit.Rows("1 0", "2 0"))
	_, err = tk.Exec("insert nn (c1) values (3)")
	c.Check(err, NotNil)
	tk.MustExec("set sql_mode=''")
	tk.MustExec("insert nn (c1) values (3)")
	tk.MustQuery("select * from nn").Check(testkit.Rows("1 0", "2 0", "3 0"))
}

func (s *testSuite6) TestAlterTableModifyColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists mc")
	tk.MustExec("create table mc(c1 int, c2 varchar(10), c3 bit)")
	_, err := tk.Exec("alter table mc modify column c1 short")
	c.Assert(err, NotNil)
	tk.MustExec("alter table mc modify column c1 bigint")

	_, err = tk.Exec("alter table mc modify column c2 blob")
	c.Assert(err, NotNil)

	_, err = tk.Exec("alter table mc modify column c2 varchar(8)")
	c.Assert(err, NotNil)
	tk.MustExec("alter table mc modify column c2 varchar(11)")
	tk.MustExec("alter table mc modify column c2 text(13)")
	tk.MustExec("alter table mc modify column c2 text")
	tk.MustExec("alter table mc modify column c3 bit")
	result := tk.MustQuery("show create table mc")
	createSQL := result.Rows()[0][1]
	expected := "CREATE TABLE `mc` (\n  `c1` bigint(20) DEFAULT NULL,\n  `c2` text DEFAULT NULL,\n  `c3` bit(1) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createSQL, Equals, expected)
}

func (s *testSuite6) TestTooLargeIdentifierLength(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// for database.
	dbName1, dbName2 := strings.Repeat("a", mysql.MaxDatabaseNameLength), strings.Repeat("a", mysql.MaxDatabaseNameLength+1)
	tk.MustExec(fmt.Sprintf("create database %s", dbName1))
	tk.MustExec(fmt.Sprintf("drop database %s", dbName1))
	_, err := tk.Exec(fmt.Sprintf("create database %s", dbName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[ddl:1059]Identifier name '%s' is too long", dbName2))

	// for table.
	tk.MustExec("use test")
	tableName1, tableName2 := strings.Repeat("b", mysql.MaxTableNameLength), strings.Repeat("b", mysql.MaxTableNameLength+1)
	tk.MustExec(fmt.Sprintf("create table %s(c int)", tableName1))
	tk.MustExec(fmt.Sprintf("drop table %s", tableName1))
	_, err = tk.Exec(fmt.Sprintf("create table %s(c int)", tableName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[ddl:1059]Identifier name '%s' is too long", tableName2))

	// for column.
	tk.MustExec("drop table if exists t;")
	columnName1, columnName2 := strings.Repeat("c", mysql.MaxColumnNameLength), strings.Repeat("c", mysql.MaxColumnNameLength+1)
	tk.MustExec(fmt.Sprintf("create table t(%s int)", columnName1))
	tk.MustExec("drop table t")
	_, err = tk.Exec(fmt.Sprintf("create table t(%s int)", columnName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[ddl:1059]Identifier name '%s' is too long", columnName2))

	// for index.
	tk.MustExec("create table t(c int);")
	indexName1, indexName2 := strings.Repeat("d", mysql.MaxIndexIdentifierLen), strings.Repeat("d", mysql.MaxIndexIdentifierLen+1)
	tk.MustExec(fmt.Sprintf("create index %s on t(c)", indexName1))
	tk.MustExec(fmt.Sprintf("drop index %s on t", indexName1))
	_, err = tk.Exec(fmt.Sprintf("create index %s on t(c)", indexName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[ddl:1059]Identifier name '%s' is too long", indexName2))

	// for create table with index.
	tk.MustExec("drop table t;")
	_, err = tk.Exec(fmt.Sprintf("create table t(c int, index %s(c));", indexName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[ddl:1059]Identifier name '%s' is too long", indexName2))
}

func (s *testSuite6) TestMaxHandleAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")
	tk.MustExec(fmt.Sprintf("insert into t values(%v, 1)", math.MaxInt64))
	tk.MustExec(fmt.Sprintf("insert into t values(%v, 1)", math.MinInt64))
	tk.MustExec("alter table t add index idx_b(b)")

	tk.MustExec("create table t1(a bigint UNSIGNED PRIMARY KEY, b int)")
	tk.MustExec(fmt.Sprintf("insert into t1 values(%v, 1)", uint64(math.MaxUint64)))
	tk.MustExec(fmt.Sprintf("insert into t1 values(%v, 1)", 0))
	tk.MustExec("alter table t1 add index idx_b(b)")

}

func (s *testSuite6) TestSetDDLReorgWorkerCnt(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	err := ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgWorkerCounter(), Equals, int32(variable.DefTiDBDDLReorgWorkerCount))
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1")
	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgWorkerCounter(), Equals, int32(1))
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 100")
	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgWorkerCounter(), Equals, int32(100))
	_, err = tk.Exec("set @@global.tidb_ddl_reorg_worker_cnt = invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 100")
	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgWorkerCounter(), Equals, int32(100))
	_, err = tk.Exec("set @@global.tidb_ddl_reorg_worker_cnt = -1")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 100")
	res := tk.MustQuery("select @@global.tidb_ddl_reorg_worker_cnt")
	res.Check(testkit.Rows("100"))

	res = tk.MustQuery("select @@global.tidb_ddl_reorg_worker_cnt")
	res.Check(testkit.Rows("100"))
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 100")
	res = tk.MustQuery("select @@global.tidb_ddl_reorg_worker_cnt")
	res.Check(testkit.Rows("100"))
}

func (s *testSuite6) TestSetDDLReorgBatchSize(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	err := ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgBatchSize(), Equals, int32(variable.DefTiDBDDLReorgBatchSize))

	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 1")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_ddl_reorg_batch_size value: '1'"))
	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgBatchSize(), Equals, int32(variable.MinDDLReorgBatchSize))
	tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_reorg_batch_size = %v", variable.MaxDDLReorgBatchSize+1))
	tk.MustQuery("show warnings;").Check(testkit.Rows(fmt.Sprintf("Warning 1292 Truncated incorrect tidb_ddl_reorg_batch_size value: '%d'", variable.MaxDDLReorgBatchSize+1)))
	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgBatchSize(), Equals, int32(variable.MaxDDLReorgBatchSize))
	_, err = tk.Exec("set @@global.tidb_ddl_reorg_batch_size = invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 100")
	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgBatchSize(), Equals, int32(100))
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = -1")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_ddl_reorg_batch_size value: '-1'"))

	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 100")
	res := tk.MustQuery("select @@global.tidb_ddl_reorg_batch_size")
	res.Check(testkit.Rows("100"))

	res = tk.MustQuery("select @@global.tidb_ddl_reorg_batch_size")
	res.Check(testkit.Rows(fmt.Sprintf("%v", 100)))
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 1000")
	res = tk.MustQuery("select @@global.tidb_ddl_reorg_batch_size")
	res.Check(testkit.Rows("1000"))
}

func (s *testSuite6) TestSetDDLErrorCountLimit(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	err := ddlutil.LoadDDLVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLErrorCountLimit(), Equals, int64(variable.DefTiDBDDLErrorCountLimit))

	tk.MustExec("set @@global.tidb_ddl_error_count_limit = -1")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_ddl_error_count_limit value: '-1'"))
	err = ddlutil.LoadDDLVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLErrorCountLimit(), Equals, int64(0))
	tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %v", uint64(math.MaxInt64)+1))
	tk.MustQuery("show warnings;").Check(testkit.Rows(fmt.Sprintf("Warning 1292 Truncated incorrect tidb_ddl_error_count_limit value: '%d'", uint64(math.MaxInt64)+1)))
	err = ddlutil.LoadDDLVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLErrorCountLimit(), Equals, int64(math.MaxInt64))
	_, err = tk.Exec("set @@global.tidb_ddl_error_count_limit = invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 100")
	err = ddlutil.LoadDDLVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLErrorCountLimit(), Equals, int64(100))
	res := tk.MustQuery("select @@global.tidb_ddl_error_count_limit")
	res.Check(testkit.Rows("100"))
}
