// Copyright 2019 PingCAP, Inc.
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
	"math"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
)

var vecBuiltinArithmeticCases = map[string][]vecExprBenchCase{
	ast.Minus: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{&rangeInt64Gener{-100000, 100000}, &rangeInt64Gener{-100000, 100000}}},
	},
	ast.Div: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}, geners: []dataGenerator{nil, &rangeRealGener{0, 0, 0}}},
	},
	ast.Mul: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, geners: []dataGenerator{&rangeInt64Gener{-10000, 10000}, &rangeInt64Gener{-10000, 10000}}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt}, childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeInt24, Flag: mysql.UnsignedFlag}, {Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners: []dataGenerator{
				&rangeInt64Gener{begin: 0, end: 10000},
				&rangeInt64Gener{begin: 0, end: 10000},
			},
		},
	},
	ast.Plus: {
		{retEvalType: types.ETReal, childrenTypes: []types.EvalType{types.ETReal, types.ETReal}},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			geners: []dataGenerator{
				&rangeInt64Gener{begin: math.MinInt64 / 2, end: math.MaxInt64 / 2},
				&rangeInt64Gener{begin: math.MinInt64 / 2, end: math.MaxInt64 / 2},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners: []dataGenerator{
				&rangeInt64Gener{begin: 0, end: math.MaxInt64},
				&rangeInt64Gener{begin: 0, end: math.MaxInt64},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong},
				{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag}},
			geners: []dataGenerator{
				&rangeInt64Gener{begin: 0, end: math.MaxInt64},
				&rangeInt64Gener{begin: 0, end: math.MaxInt64},
			},
		},
		{retEvalType: types.ETInt, childrenTypes: []types.EvalType{types.ETInt, types.ETInt},
			childrenFieldTypes: []*types.FieldType{{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag},
				{Tp: mysql.TypeLonglong}},
			geners: []dataGenerator{
				&rangeInt64Gener{begin: 0, end: math.MaxInt64},
				&rangeInt64Gener{begin: 0, end: math.MaxInt64},
			},
		},
	},
}

func (s *testEvaluatorSuite) TestVectorizedBuiltinArithmeticFunc(c *C) {
	testVectorizedBuiltinFunc(c, vecBuiltinArithmeticCases)
}

func BenchmarkVectorizedBuiltinArithmeticFunc(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltinArithmeticCases)
}
