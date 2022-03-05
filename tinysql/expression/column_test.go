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

package expression

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

func (s *testEvaluatorSuite) TestColumnHashCode(c *C) {
	col1 := &Column{
		UniqueID: 12,
	}
	c.Assert(col1.HashCode(nil), DeepEquals, []byte{0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc})

	col2 := &Column{
		UniqueID: 2,
	}
	c.Assert(col2.HashCode(nil), DeepEquals, []byte{0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2})
}

func (s *testEvaluatorSuite) TestColumn2Expr(c *C) {
	cols := make([]*Column, 0, 5)
	for i := 0; i < 5; i++ {
		cols = append(cols, &Column{UniqueID: int64(i)})
	}

	exprs := Column2Exprs(cols)
	for i := range exprs {
		c.Assert(exprs[i].Equal(nil, cols[i]), IsTrue)
	}
}

func (s *testEvaluatorSuite) TestColInfo2Col(c *C) {
	col0, col1 := &Column{ID: 0}, &Column{ID: 1}
	cols := []*Column{col0, col1}
	colInfo := &model.ColumnInfo{ID: 0}
	res := ColInfo2Col(cols, colInfo)
	c.Assert(res.Equal(nil, col1), IsTrue)

	colInfo.ID = 3
	res = ColInfo2Col(cols, colInfo)
	c.Assert(res, IsNil)
}

func (s *testEvaluatorSuite) TestIndexInfo2Cols(c *C) {
	col0 := &Column{UniqueID: 0, ID: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	col1 := &Column{UniqueID: 1, ID: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	colInfo0 := &model.ColumnInfo{ID: 0, Name: model.NewCIStr("0")}
	colInfo1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("1")}
	indexCol0, indexCol1 := &model.IndexColumn{Name: model.NewCIStr("0")}, &model.IndexColumn{Name: model.NewCIStr("1")}
	indexInfo := &model.IndexInfo{Columns: []*model.IndexColumn{indexCol0, indexCol1}}

	cols := []*Column{col0}
	colInfos := []*model.ColumnInfo{colInfo0}
	resCols, lengths := IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	c.Assert(len(resCols), Equals, 1)
	c.Assert(len(lengths), Equals, 1)
	c.Assert(resCols[0].Equal(nil, col0), IsTrue)

	cols = []*Column{col1}
	colInfos = []*model.ColumnInfo{colInfo1}
	resCols, lengths = IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	c.Assert(len(resCols), Equals, 0)
	c.Assert(len(lengths), Equals, 0)

	cols = []*Column{col0, col1}
	colInfos = []*model.ColumnInfo{colInfo0, colInfo1}
	resCols, lengths = IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	c.Assert(len(resCols), Equals, 2)
	c.Assert(len(lengths), Equals, 2)
	c.Assert(resCols[0].Equal(nil, col0), IsTrue)
	c.Assert(resCols[1].Equal(nil, col1), IsTrue)
}

func (s *testEvaluatorSuite) TestPadCharToFullLength(c *C) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.PadCharToFullLength = true

	ft := types.NewFieldType(mysql.TypeString)
	ft.Flen = 10
	col := &Column{RetType: ft, Index: 0}
	input := chunk.New([]*types.FieldType{ft}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendString(0, "xy")
	}
	result, err := newBuffer(types.ETString, 1024)
	c.Assert(err, IsNil)
	c.Assert(col.VecEvalString(ctx, input, result), IsNil)

	it := chunk.NewIterator4Chunk(input)
	for row, i := it.Begin(), 0; row != it.End(); row, i = it.Next(), i+1 {
		v, _, err := col.EvalString(ctx, row)
		c.Assert(err, IsNil)
		c.Assert(len(v), Equals, ft.Flen)
		c.Assert(v, Equals, result.GetString(i))
	}
}
