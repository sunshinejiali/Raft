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

package core

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/types"
)

var _ = Suite(&testPlanBuilderSuite{})

func (s *testPlanBuilderSuite) SetUpSuite(c *C) {
}

type testPlanBuilderSuite struct {
}

func (s *testPlanBuilderSuite) TestShow(c *C) {
	node := &ast.ShowStmt{}
	tps := []ast.ShowStmtType{
		ast.ShowDatabases,
		ast.ShowTables,
		ast.ShowWarnings,
		ast.ShowVariables,
		ast.ShowCreateTable,
		ast.ShowCreateDatabase,
	}
	for _, tp := range tps {
		node.Tp = tp
		schema, _ := buildShowSchema(node)
		for _, col := range schema.Columns {
			c.Assert(col.RetType.Flen, Greater, 0)
		}
	}
}

func (s *testPlanBuilderSuite) TestGetPathByIndexName(c *C) {
	tblInfo := &model.TableInfo{
		Indices:    make([]*model.IndexInfo, 0),
		PKIsHandle: true,
	}

	accessPath := []*util.AccessPath{
		{IsTablePath: true},
		{Index: &model.IndexInfo{Name: model.NewCIStr("idx")}},
	}

	path := getPathByIndexName(accessPath, model.NewCIStr("idx"), tblInfo)
	c.Assert(path, NotNil)
	c.Assert(path, Equals, accessPath[1])

	path = getPathByIndexName(accessPath, model.NewCIStr("primary"), tblInfo)
	c.Assert(path, NotNil)
	c.Assert(path, Equals, accessPath[0])

	path = getPathByIndexName(accessPath, model.NewCIStr("not exists"), tblInfo)
	c.Assert(path, IsNil)

	tblInfo = &model.TableInfo{
		Indices:    make([]*model.IndexInfo, 0),
		PKIsHandle: false,
	}

	path = getPathByIndexName(accessPath, model.NewCIStr("primary"), tblInfo)
	c.Assert(path, IsNil)
}

func (s *testPlanBuilderSuite) TestRewriterPool(c *C) {
	builder := NewPlanBuilder(MockContext(), nil)

	// Make sure PlanBuilder.getExpressionRewriter() provides clean rewriter from pool.
	// First, pick one rewriter from the pool and make it dirty.
	builder.rewriterCounter++
	dirtyRewriter := builder.getExpressionRewriter(context.TODO(), nil)
	dirtyRewriter.asScalar = true
	dirtyRewriter.aggrMap = make(map[*ast.AggregateFuncExpr]int)
	dirtyRewriter.preprocess = func(ast.Node) ast.Node { return nil }
	dirtyRewriter.insertPlan = &Insert{}
	dirtyRewriter.ctxStack = make([]expression.Expression, 2)
	dirtyRewriter.ctxNameStk = make([]*types.FieldName, 2)
	builder.rewriterCounter--
	// Then, pick again and check if it's cleaned up.
	builder.rewriterCounter++
	cleanRewriter := builder.getExpressionRewriter(context.TODO(), nil)
	c.Assert(cleanRewriter, Equals, dirtyRewriter) // Rewriter should be reused.
	c.Assert(cleanRewriter.asScalar, Equals, false)
	c.Assert(cleanRewriter.aggrMap, IsNil)
	c.Assert(cleanRewriter.preprocess, IsNil)
	c.Assert(cleanRewriter.insertPlan, IsNil)
	c.Assert(len(cleanRewriter.ctxStack), Equals, 0)
	builder.rewriterCounter--
}
