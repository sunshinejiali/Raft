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

package core

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testExpressionSuite{})

type testExpressionSuite struct {
	*parser.Parser
	ctx sessionctx.Context
}

func (s *testExpressionSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	s.ctx = mock.NewContext()
}

func (s *testExpressionSuite) TearDownSuite(c *C) {
}

func (s *testExpressionSuite) parseExpr(c *C, expr string) ast.ExprNode {
	st, err := s.ParseOneStmt("select "+expr, "", "")
	c.Assert(err, IsNil)
	stmt := st.(*ast.SelectStmt)
	return stmt.Fields.Fields[0].Expr
}

type testCase struct {
	exprStr   string
	resultStr string
}

func (s *testExpressionSuite) runTests(c *C, tests []testCase) {
	for _, tt := range tests {
		expr := s.parseExpr(c, tt.exprStr)
		val, err := evalAstExpr(s.ctx, expr)
		c.Assert(err, IsNil)
		valStr := fmt.Sprintf("%v", val.GetValue())
		c.Assert(valStr, Equals, tt.resultStr, Commentf("for %s", tt.exprStr))
	}
}

func (s *testExpressionSuite) TestBetween(c *C) {
	defer testleak.AfterTest(c)()
	tests := []testCase{
		{exprStr: "1 between 2 and 3", resultStr: "0"},
		{exprStr: "1 not between 2 and 3", resultStr: "1"},
	}
	s.runTests(c, tests)
}

func (s *testExpressionSuite) TestPatternIn(c *C) {
	defer testleak.AfterTest(c)()
	tests := []testCase{
		{
			exprStr:   "1 not in (1, 2, 3)",
			resultStr: "0",
		},
		{
			exprStr:   "1 in (1, 2, 3)",
			resultStr: "1",
		},
		{
			exprStr:   "1 in (2, 3)",
			resultStr: "0",
		},
		{
			exprStr:   "NULL in (2, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "NULL not in (2, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "NULL in (NULL, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "1 in (1, NULL)",
			resultStr: "1",
		},
		{
			exprStr:   "1 in (NULL, 1)",
			resultStr: "1",
		},
		{
			exprStr:   "2 in (1, NULL)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "(-(23)++46/51*+51) in (+23)",
			resultStr: "0",
		},
	}
	s.runTests(c, tests)
}

func (s *testExpressionSuite) TestIsNull(c *C) {
	defer testleak.AfterTest(c)()
	tests := []testCase{
		{
			exprStr:   "1 IS NULL",
			resultStr: "0",
		},
		{
			exprStr:   "1 IS NOT NULL",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NULL",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT NULL",
			resultStr: "0",
		},
	}
	s.runTests(c, tests)
}

func (s *testExpressionSuite) TestCompareRow(c *C) {
	defer testleak.AfterTest(c)()
	tests := []testCase{
		{
			exprStr:   "row(1,2,3)=row(1,2,3)",
			resultStr: "1",
		},
		{
			exprStr:   "row(1,2,3)=row(1+3,2,3)",
			resultStr: "0",
		},
		{
			exprStr:   "row(1,2,3)<>row(1,2,3)",
			resultStr: "0",
		},
		{
			exprStr:   "row(1,2,3)<>row(1+3,2,3)",
			resultStr: "1",
		},
		{
			exprStr:   "row(1+3,2,3)<>row(1+3,2,3)",
			resultStr: "0",
		},
		{
			exprStr:   "row(1,2,3)<row(1,NULL,3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "row(1,2,3)<row(2,NULL,3)",
			resultStr: "1",
		},
		{
			exprStr:   "row(1,2,3)>=row(0,NULL,3)",
			resultStr: "1",
		},
		{
			exprStr:   "row(1,2,3)<=row(2,NULL,3)",
			resultStr: "1",
		},
	}
	s.runTests(c, tests)
}
