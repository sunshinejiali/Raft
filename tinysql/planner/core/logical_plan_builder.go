// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distribute under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strings"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
)

const (
	// TiDBMergeJoin is hint enforce merge join.
	TiDBMergeJoin = "tidb_smj"
	// HintSMJ is hint enforce merge join.
	HintSMJ = "sm_join"
	// TiDBHashJoin is hint enforce hash join.
	TiDBHashJoin = "tidb_hj"
	// HintHJ is hint enforce hash join.
	HintHJ = "hash_join"
	// HintUseIndex is hint enforce using some indexes.
	HintUseIndex = "use_index"
	// HintIgnoreIndex is hint enforce ignoring some indexes.
	HintIgnoreIndex = "ignore_index"
)

func (la *LogicalAggregation) collectGroupByColumns() {
	la.groupByCols = la.groupByCols[:0]
	for _, item := range la.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			la.groupByCols = append(la.groupByCols, col)
		}
	}
}

func (b *PlanBuilder) buildAggregation(ctx context.Context, p LogicalPlan, aggFuncList []*ast.AggregateFuncExpr, gbyItems []expression.Expression) (LogicalPlan, map[int]int, error) {
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagPushDownAgg
	// We may apply aggregation eliminate optimization.
	// So we add the flagMaxMinEliminate to try to convert max/min to topn and flagPushDownTopN to handle the newly added topn operator.
	b.optFlag = b.optFlag | flagMaxMinEliminate
	b.optFlag = b.optFlag | flagPushDownTopN
	// when we eliminate the max and min we may add `is not null` filter.
	b.optFlag = b.optFlag | flagPredicatePushDown
	b.optFlag = b.optFlag | flagEliminateAgg
	b.optFlag = b.optFlag | flagEliminateProjection

	plan4Agg := LogicalAggregation{AggFuncs: make([]*aggregation.AggFuncDesc, 0, len(aggFuncList))}.Init(b.ctx)
	schema4Agg := expression.NewSchema(make([]*expression.Column, 0, len(aggFuncList)+p.Schema().Len())...)
	names := make(types.NameSlice, 0, len(aggFuncList)+p.Schema().Len())
	// aggIdxMap maps the old index to new index after applying common aggregation functions elimination.
	aggIndexMap := make(map[int]int)

	for i, aggFunc := range aggFuncList {
		newArgList := make([]expression.Expression, 0, len(aggFunc.Args))
		for _, arg := range aggFunc.Args {
			newArg, np, err := b.rewrite(ctx, arg, p, nil, true)
			if err != nil {
				return nil, nil, err
			}
			p = np
			newArgList = append(newArgList, newArg)
		}
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, aggFunc.F, newArgList)
		if err != nil {
			return nil, nil, err
		}
		combined := false
		for j, oldFunc := range plan4Agg.AggFuncs {
			if oldFunc.Equal(b.ctx, newFunc) {
				aggIndexMap[i] = j
				combined = true
				break
			}
		}
		if !combined {
			position := len(plan4Agg.AggFuncs)
			aggIndexMap[i] = position
			plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
			schema4Agg.Append(&expression.Column{
				UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  newFunc.RetTp,
			})
			names = append(names, types.EmptyName)
		}
	}
	for i, col := range p.Schema().Columns {
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col})
		if err != nil {
			return nil, nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
		newCol, _ := col.Clone().(*expression.Column)
		newCol.RetType = newFunc.RetTp
		schema4Agg.Append(newCol)
		names = append(names, p.OutputNames()[i])
	}
	plan4Agg.names = names
	plan4Agg.SetChildren(p)
	plan4Agg.GroupByItems = gbyItems
	plan4Agg.SetSchema(schema4Agg)
	plan4Agg.collectGroupByColumns()
	return plan4Agg, aggIndexMap, nil
}

func (b *PlanBuilder) buildResultSetNode(ctx context.Context, node ast.ResultSetNode) (p LogicalPlan, err error) {
	switch x := node.(type) {
	case *ast.Join:
		return b.buildJoin(ctx, x)
	case *ast.TableSource:
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			p, err = b.buildSelect(ctx, v)
		case *ast.TableName:
			p, err = b.buildDataSource(ctx, v, &x.AsName)
		default:
			err = ErrUnsupportedType.GenWithStackByArgs(v)
		}
		if err != nil {
			return nil, err
		}

		for _, name := range p.OutputNames() {
			if name.Hidden {
				continue
			}
			if x.AsName.L != "" {
				name.TblName = x.AsName
			}
		}
		// Duplicate column name in one table is not allowed.
		// "select * from (select 1, 1) as a;" is duplicate
		dupNames := make(map[string]struct{}, len(p.Schema().Columns))
		for _, name := range p.OutputNames() {
			colName := name.ColName.O
			if _, ok := dupNames[colName]; ok {
				return nil, ErrDupFieldName.GenWithStackByArgs(colName)
			}
			dupNames[colName] = struct{}{}
		}
		return p, nil
	case *ast.SelectStmt:
		return b.buildSelect(ctx, x)
	default:
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.ResultSetNode(%T) for buildResultSetNode()", x)
	}
}

// pushDownConstExpr checks if the condition is from filter condition, if true, push it down to both
// children of join, whatever the join type is; if false, push it down to inner child of outer join,
// and both children of non-outer-join.
func (p *LogicalJoin) pushDownConstExpr(expr expression.Expression, leftCond []expression.Expression,
	rightCond []expression.Expression, filterCond bool) ([]expression.Expression, []expression.Expression) {
	switch p.JoinType {
	case LeftOuterJoin:
		if filterCond {
			leftCond = append(leftCond, expr)
			// Append the expr to right join condition instead of `rightCond`, to make it able to be
			// pushed down to children of join.
			p.RightConditions = append(p.RightConditions, expr)
		} else {
			rightCond = append(rightCond, expr)
		}
	case RightOuterJoin:
		if filterCond {
			rightCond = append(rightCond, expr)
			p.LeftConditions = append(p.LeftConditions, expr)
		} else {
			leftCond = append(leftCond, expr)
		}
	case InnerJoin:
		leftCond = append(leftCond, expr)
		rightCond = append(rightCond, expr)
	}
	return leftCond, rightCond
}

func (p *LogicalJoin) extractOnCondition(conditions []expression.Expression, deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	return p.ExtractOnCondition(conditions, p.children[0].Schema(), p.children[1].Schema(), deriveLeft, deriveRight)
}

// ExtractOnCondition divide conditions in CNF of join node into 4 groups.
// These conditions can be where conditions, join conditions, or collection of both.
// If deriveLeft/deriveRight is set, we would try to derive more conditions for left/right plan.
func (p *LogicalJoin) ExtractOnCondition(
	conditions []expression.Expression,
	leftSchema *expression.Schema,
	rightSchema *expression.Schema,
	deriveLeft bool,
	deriveRight bool) (eqCond []*expression.ScalarFunction, leftCond []expression.Expression,
	rightCond []expression.Expression, otherCond []expression.Expression) {
	for _, expr := range conditions {
		binop, ok := expr.(*expression.ScalarFunction)
		if ok && len(binop.GetArgs()) == 2 {
			ctx := binop.GetCtx()
			arg0, lOK := binop.GetArgs()[0].(*expression.Column)
			arg1, rOK := binop.GetArgs()[1].(*expression.Column)
			if lOK && rOK {
				leftCol := leftSchema.RetrieveColumn(arg0)
				rightCol := rightSchema.RetrieveColumn(arg1)
				if leftCol == nil || rightCol == nil {
					leftCol = leftSchema.RetrieveColumn(arg1)
					rightCol = rightSchema.RetrieveColumn(arg0)
					arg0, arg1 = arg1, arg0
				}
				if leftCol != nil && rightCol != nil {
					if deriveLeft {
						if isNullRejected(ctx, leftSchema, expr) && !mysql.HasNotNullFlag(leftCol.RetType.Flag) {
							notNullExpr := expression.BuildNotNullExpr(ctx, leftCol)
							leftCond = append(leftCond, notNullExpr)
						}
					}
					if deriveRight {
						if isNullRejected(ctx, rightSchema, expr) && !mysql.HasNotNullFlag(rightCol.RetType.Flag) {
							notNullExpr := expression.BuildNotNullExpr(ctx, rightCol)
							rightCond = append(rightCond, notNullExpr)
						}
					}
					if binop.FuncName.L == ast.EQ {
						cond := expression.NewFunctionInternal(ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), arg0, arg1)
						eqCond = append(eqCond, cond.(*expression.ScalarFunction))
						continue
					}
				}
			}
		}
		columns := expression.ExtractColumns(expr)
		// `columns` may be empty, if the condition is like `correlated_column op constant`, or `constant`,
		// push this kind of constant condition down according to join type.
		if len(columns) == 0 {
			leftCond, rightCond = p.pushDownConstExpr(expr, leftCond, rightCond, deriveLeft || deriveRight)
			continue
		}
		allFromLeft, allFromRight := true, true
		for _, col := range columns {
			if !leftSchema.Contains(col) {
				allFromLeft = false
			}
			if !rightSchema.Contains(col) {
				allFromRight = false
			}
		}
		if allFromRight {
			rightCond = append(rightCond, expr)
		} else if allFromLeft {
			leftCond = append(leftCond, expr)
		} else {
			// Relax expr to two supersets: leftRelaxedCond and rightRelaxedCond, the expression now is
			// `expr AND leftRelaxedCond AND rightRelaxedCond`. Motivation is to push filters down to
			// children as much as possible.
			if deriveLeft {
				leftRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, leftSchema)
				if leftRelaxedCond != nil {
					leftCond = append(leftCond, leftRelaxedCond)
				}
			}
			if deriveRight {
				rightRelaxedCond := expression.DeriveRelaxedFiltersFromDNF(expr, rightSchema)
				if rightRelaxedCond != nil {
					rightCond = append(rightCond, rightRelaxedCond)
				}
			}
			otherCond = append(otherCond, expr)
		}
	}
	return
}

// extractTableAlias returns table alias of the LogicalPlan's columns.
// It will return nil when there are multiple table alias, because the alias is only used to check if
// the logicalPlan match some optimizer hints, and hints are not expected to take effect in this case.
func extractTableAlias(p Plan) *hintTableInfo {
	if len(p.OutputNames()) > 0 && p.OutputNames()[0].TblName.L != "" {
		firstName := p.OutputNames()[0]
		for _, name := range p.OutputNames() {
			if name.TblName.L != firstName.TblName.L || name.DBName.L != firstName.DBName.L {
				return nil
			}
		}
		return &hintTableInfo{dbName: firstName.DBName, tblName: firstName.TblName}
	}
	return nil
}

func (p *LogicalJoin) setPreferredJoinType(hintInfo *tableHintInfo) {
	if hintInfo == nil {
		return
	}

	lhsAlias := extractTableAlias(p.children[0])
	rhsAlias := extractTableAlias(p.children[1])
	if hintInfo.ifPreferMergeJoin(lhsAlias, rhsAlias) {
		p.preferJoinType |= preferMergeJoin
	}
	if hintInfo.ifPreferHashJoin(lhsAlias, rhsAlias) {
		p.preferJoinType |= preferHashJoin
	}

	// set hintInfo for further usage if this hint info can be used.
	if p.preferJoinType != 0 {
		p.hintInfo = hintInfo
	}

	if containDifferentJoinTypes(p.preferJoinType) {
		errMsg := "Join hints are conflict, you can only specify one type of join"
		warning := ErrInternal.GenWithStack(errMsg)
		p.ctx.GetSessionVars().StmtCtx.AppendWarning(warning)
	}
}

func resetNotNullFlag(schema *expression.Schema, start, end int) {
	for i := start; i < end; i++ {
		col := *schema.Columns[i]
		newFieldType := *col.RetType
		newFieldType.Flag &= ^mysql.NotNullFlag
		col.RetType = &newFieldType
		schema.Columns[i] = &col
	}
}

func (b *PlanBuilder) buildJoin(ctx context.Context, joinNode *ast.Join) (LogicalPlan, error) {
	// We will construct a "Join" node for some statements like "INSERT",
	// "DELETE", "UPDATE", "REPLACE". For this scenario "joinNode.Right" is nil
	// and we only build the left "ResultSetNode".
	if joinNode.Right == nil {
		return b.buildResultSetNode(ctx, joinNode.Left)
	}

	b.optFlag = b.optFlag | flagPredicatePushDown

	leftPlan, err := b.buildResultSetNode(ctx, joinNode.Left)
	if err != nil {
		return nil, err
	}

	rightPlan, err := b.buildResultSetNode(ctx, joinNode.Right)
	if err != nil {
		return nil, err
	}

	handleMap1 := b.handleHelper.popMap()
	handleMap2 := b.handleHelper.popMap()
	b.handleHelper.mergeAndPush(handleMap1, handleMap2)

	joinPlan := LogicalJoin{StraightJoin: b.inStraightJoin}.Init(b.ctx)
	joinPlan.SetChildren(leftPlan, rightPlan)
	joinPlan.SetSchema(expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema()))
	joinPlan.names = make([]*types.FieldName, leftPlan.Schema().Len()+rightPlan.Schema().Len())
	copy(joinPlan.names, leftPlan.OutputNames())
	copy(joinPlan.names[leftPlan.Schema().Len():], rightPlan.OutputNames())

	// Set join type.
	switch joinNode.Tp {
	case ast.LeftJoin:
		// left outer join need to be checked elimination
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		joinPlan.JoinType = LeftOuterJoin
		resetNotNullFlag(joinPlan.schema, leftPlan.Schema().Len(), joinPlan.schema.Len())
	case ast.RightJoin:
		// right outer join need to be checked elimination
		b.optFlag = b.optFlag | flagEliminateOuterJoin
		joinPlan.JoinType = RightOuterJoin
		resetNotNullFlag(joinPlan.schema, 0, leftPlan.Schema().Len())
	default:
		b.optFlag = b.optFlag | flagJoinReOrder
		joinPlan.JoinType = InnerJoin
	}

	// Set preferred join algorithm if some join hints is specified by user.
	joinPlan.setPreferredJoinType(b.TableHints())

	if joinNode.On != nil {
		b.curClause = onClause
		onExpr, newPlan, err := b.rewrite(ctx, joinNode.On.Expr, joinPlan, nil, false)
		if err != nil {
			return nil, err
		}
		if newPlan != joinPlan {
			return nil, errors.New("ON condition doesn't support subqueries yet")
		}
		onCondition := expression.SplitCNFItems(onExpr)
		joinPlan.attachOnConds(onCondition)
	} else if joinPlan.JoinType == InnerJoin {
		// If a inner join without "ON" or "USING" clause, it's a cartesian
		// product over the join tables.
		joinPlan.cartesianJoin = true
	}

	return joinPlan, nil
}

func (b *PlanBuilder) buildSelection(ctx context.Context, p LogicalPlan, where ast.ExprNode, AggMapper map[*ast.AggregateFuncExpr]int) (LogicalPlan, error) {
	b.optFlag = b.optFlag | flagPredicatePushDown
	if b.curClause != havingClause {
		b.curClause = whereClause
	}

	conditions := splitWhere(where)
	expressions := make([]expression.Expression, 0, len(conditions))
	selection := LogicalSelection{}.Init(b.ctx)
	for _, cond := range conditions {
		expr, np, err := b.rewrite(ctx, cond, p, AggMapper, false)
		if err != nil {
			return nil, err
		}
		p = np
		if expr == nil {
			continue
		}
		cnfItems := expression.SplitCNFItems(expr)
		for _, item := range cnfItems {
			if con, ok := item.(*expression.Constant); ok {
				ret, _, err := expression.EvalBool(b.ctx, expression.CNFExprs{con}, chunk.Row{})
				if err != nil || ret {
					continue
				}
				// If there is condition which is always false, return dual plan directly.
				dual := LogicalTableDual{}.Init(b.ctx)
				dual.names = p.OutputNames()
				dual.SetSchema(p.Schema())
				return dual, nil
			}
			expressions = append(expressions, item)
		}
	}
	if len(expressions) == 0 {
		return p, nil
	}
	selection.Conditions = expressions
	selection.SetChildren(p)
	return selection, nil
}

// buildProjectionFieldNameFromColumns builds the field name, table name and database name when field expression is a column reference.
func (b *PlanBuilder) buildProjectionFieldNameFromColumns(origField *ast.SelectField, colNameField *ast.ColumnNameExpr, name *types.FieldName) (colName, origColName, tblName, origTblName, dbName model.CIStr) {
	origTblName, origColName, dbName = name.OrigTblName, name.OrigColName, name.DBName
	if origField.AsName.L == "" {
		colName = colNameField.Name.Name
	} else {
		colName = origField.AsName
	}
	if tblName.L == "" {
		tblName = name.TblName
	} else {
		tblName = colNameField.Name.Table
	}
	return
}

// buildProjectionFieldNameFromExpressions builds the field name when field expression is a normal expression.
func (b *PlanBuilder) buildProjectionFieldNameFromExpressions(ctx context.Context, field *ast.SelectField) (model.CIStr, error) {
	if agg, ok := field.Expr.(*ast.AggregateFuncExpr); ok && agg.F == ast.AggFuncFirstRow {
		// When the query is select t.a from t group by a; The Column Name should be a but not t.a;
		return agg.Args[0].(*ast.ColumnNameExpr).Name.Name, nil
	}

	innerExpr := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	valueExpr, isValueExpr := innerExpr.(*driver.ValueExpr)

	// Non-literal: Output as inputed, except that comments need to be removed.
	if !isValueExpr {
		return model.NewCIStr(parser.SpecFieldPattern.ReplaceAllStringFunc(field.Text(), parser.TrimComment)), nil
	}

	// Literal: Need special processing
	switch valueExpr.Kind() {
	case types.KindString:
		projName := valueExpr.GetString()
		projOffset := valueExpr.GetProjectionOffset()
		if projOffset >= 0 {
			projName = projName[:projOffset]
		}
		// See #3686, #3994:
		// For string literals, string content is used as column name. Non-graph initial characters are trimmed.
		fieldName := strings.TrimLeftFunc(projName, func(r rune) bool {
			return !unicode.IsOneOf(mysql.RangeGraph, r)
		})
		return model.NewCIStr(fieldName), nil
	case types.KindNull:
		// See #4053, #3685
		return model.NewCIStr("NULL"), nil
	case types.KindInt64:
		// See #9683
		// TRUE or FALSE can be a int64
		if mysql.HasIsBooleanFlag(valueExpr.Type.Flag) {
			if i := valueExpr.GetValue().(int64); i == 0 {
				return model.NewCIStr("FALSE"), nil
			}
			return model.NewCIStr("TRUE"), nil
		}
		fallthrough

	default:
		fieldName := field.Text()
		fieldName = strings.TrimLeft(fieldName, "\t\n +(")
		fieldName = strings.TrimRight(fieldName, "\t\n )")
		return model.NewCIStr(fieldName), nil
	}
}

// buildProjectionField builds the field object according to SelectField in projection.
func (b *PlanBuilder) buildProjectionField(ctx context.Context, p LogicalPlan, field *ast.SelectField, expr expression.Expression) (*expression.Column, *types.FieldName, error) {
	var origTblName, tblName, origColName, colName, dbName model.CIStr
	innerNode := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	col, isCol := expr.(*expression.Column)
	if colNameField, ok := innerNode.(*ast.ColumnNameExpr); ok && isCol {
		// Field is a column reference.
		idx := p.Schema().ColumnIndex(col)
		name := p.OutputNames()[idx]
		colName, origColName, tblName, origTblName, dbName = b.buildProjectionFieldNameFromColumns(field, colNameField, name)
	} else if field.AsName.L != "" {
		// Field has alias.
		colName = field.AsName
	} else {
		// Other: field is an expression.
		var err error
		if colName, err = b.buildProjectionFieldNameFromExpressions(ctx, field); err != nil {
			return nil, nil, err
		}
	}
	name := &types.FieldName{
		TblName:     tblName,
		OrigTblName: origTblName,
		ColName:     colName,
		OrigColName: origColName,
		DBName:      dbName,
	}
	if isCol {
		return col, name, nil
	}
	newCol := &expression.Column{
		UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  expr.GetType(),
	}
	return newCol, name, nil
}

// buildProjection returns a Projection plan and non-aux columns length.
func (b *PlanBuilder) buildProjection(ctx context.Context, p LogicalPlan, fields []*ast.SelectField, mapper map[*ast.AggregateFuncExpr]int) (LogicalPlan, int, error) {
	b.optFlag |= flagEliminateProjection
	b.curClause = fieldList
	proj := LogicalProjection{Exprs: make([]expression.Expression, 0, len(fields))}.Init(b.ctx)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(fields))...)
	oldLen := 0
	newNames := make([]*types.FieldName, 0, len(fields))
	for _, field := range fields {
		if !field.Auxiliary {
			oldLen++
		}

		newExpr, np, err := b.rewriteWithPreprocess(ctx, field.Expr, p, mapper, true, nil)
		if err != nil {
			return nil, 0, err
		}

		p = np
		proj.Exprs = append(proj.Exprs, newExpr)

		col, name, err := b.buildProjectionField(ctx, p, field, newExpr)
		if err != nil {
			return nil, 0, err
		}
		schema.Append(col)
		newNames = append(newNames, name)
	}
	proj.SetSchema(schema)
	proj.SetChildren(p)
	proj.names = newNames
	return proj, oldLen, nil
}

func (b *PlanBuilder) buildDistinct(child LogicalPlan, length int) (*LogicalAggregation, error) {
	b.optFlag = b.optFlag | flagBuildKeyInfo
	b.optFlag = b.optFlag | flagPushDownAgg
	plan4Agg := LogicalAggregation{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, child.Schema().Len()),
		GroupByItems: expression.Column2Exprs(child.Schema().Clone().Columns[:length]),
	}.Init(b.ctx)
	plan4Agg.collectGroupByColumns()
	for _, col := range child.Schema().Columns {
		aggDesc, err := aggregation.NewAggFuncDesc(b.ctx, ast.AggFuncFirstRow, []expression.Expression{col})
		if err != nil {
			return nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, aggDesc)
	}
	plan4Agg.SetChildren(child)
	plan4Agg.SetSchema(child.Schema().Clone())
	plan4Agg.names = child.OutputNames()
	// Distinct will be rewritten as first_row, we reset the type here since the return type
	// of first_row is not always the same as the column arg of first_row.
	for i, col := range plan4Agg.schema.Columns {
		col.RetType = plan4Agg.AggFuncs[i].RetTp
	}
	return plan4Agg, nil
}

// ByItems wraps a "by" item.
type ByItems struct {
	Expr expression.Expression
	Desc bool
}

// String implements fmt.Stringer interface.
func (by *ByItems) String() string {
	if by.Desc {
		return fmt.Sprintf("%s true", by.Expr)
	}
	return by.Expr.String()
}

// Clone makes a copy of ByItems.
func (by *ByItems) Clone() *ByItems {
	return &ByItems{Expr: by.Expr.Clone(), Desc: by.Desc}
}

func (b *PlanBuilder) buildSort(ctx context.Context, p LogicalPlan, byItems []*ast.ByItem, aggMapper map[*ast.AggregateFuncExpr]int) (*LogicalSort, error) {
	b.curClause = orderByClause
	sort := LogicalSort{}.Init(b.ctx)
	exprs := make([]*ByItems, 0, len(byItems))
	for _, item := range byItems {
		it, np, err := b.rewriteWithPreprocess(ctx, item.Expr, p, aggMapper, true, nil)
		if err != nil {
			return nil, err
		}

		p = np
		exprs = append(exprs, &ByItems{Expr: it, Desc: item.Desc})
	}
	sort.ByItems = exprs
	sort.SetChildren(p)
	return sort, nil
}

// getUintFromNode gets uint64 value from ast.Node.
// For ordinary statement, node should be uint64 constant value.
func getUintFromNode(ctx sessionctx.Context, n ast.Node) (uVal uint64, isNull bool, isExpectedType bool) {
	var val interface{}
	switch v := n.(type) {
	case *driver.ValueExpr:
		val = v.GetValue()
	default:
		return 0, false, false
	}
	switch v := val.(type) {
	case uint64:
		return v, false, true
	case int64:
		if v >= 0 {
			return uint64(v), false, true
		}
	case string:
		sc := ctx.GetSessionVars().StmtCtx
		uVal, err := types.StrToUint(sc, v)
		if err != nil {
			return 0, false, false
		}
		return uVal, false, true
	}
	return 0, false, false
}

func extractLimitCountOffset(ctx sessionctx.Context, limit *ast.Limit) (count uint64,
	offset uint64, err error) {
	var isExpectedType bool
	if limit.Count != nil {
		count, _, isExpectedType = getUintFromNode(ctx, limit.Count)
		if !isExpectedType {
			return 0, 0, ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	if limit.Offset != nil {
		offset, _, isExpectedType = getUintFromNode(ctx, limit.Offset)
		if !isExpectedType {
			return 0, 0, ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	return count, offset, nil
}

func (b *PlanBuilder) buildLimit(src LogicalPlan, limit *ast.Limit) (LogicalPlan, error) {
	b.optFlag = b.optFlag | flagPushDownTopN
	var (
		offset, count uint64
		err           error
	)
	if count, offset, err = extractLimitCountOffset(b.ctx, limit); err != nil {
		return nil, err
	}

	if count > math.MaxUint64-offset {
		count = math.MaxUint64 - offset
	}
	if offset+count == 0 {
		tableDual := LogicalTableDual{RowCount: 0}.Init(b.ctx)
		tableDual.schema = src.Schema()
		tableDual.names = src.OutputNames()
		return tableDual, nil
	}
	li := LogicalLimit{
		Offset: offset,
		Count:  count,
	}.Init(b.ctx)
	li.SetChildren(src)
	return li, nil
}

// colMatch means that if a match b, e.g. t.a can match test.t.a but test.t.a can't match t.a.
// Because column a want column from database test exactly.
func colMatch(a *ast.ColumnName, b *ast.ColumnName) bool {
	if a.Schema.L == "" || a.Schema.L == b.Schema.L {
		if a.Table.L == "" || a.Table.L == b.Table.L {
			return a.Name.L == b.Name.L
		}
	}
	return false
}

func matchField(f *ast.SelectField, col *ast.ColumnNameExpr, ignoreAsName bool) bool {
	// if col specify a table name, resolve from table source directly.
	if col.Name.Table.L == "" {
		if f.AsName.L == "" || ignoreAsName {
			if curCol, isCol := f.Expr.(*ast.ColumnNameExpr); isCol {
				return curCol.Name.Name.L == col.Name.Name.L
			} else if _, isFunc := f.Expr.(*ast.FuncCallExpr); isFunc {
				// Fix issue 7331
				// If there are some function calls in SelectField, we check if
				// ColumnNameExpr in GroupByClause matches one of these function calls.
				// Example: select concat(k1,k2) from t group by `concat(k1,k2)`,
				// `concat(k1,k2)` matches with function call concat(k1, k2).
				return strings.ToLower(f.Text()) == col.Name.Name.L
			}
			// a expression without as name can't be matched.
			return false
		}
		return f.AsName.L == col.Name.Name.L
	}
	return false
}

func resolveFromSelectFields(v *ast.ColumnNameExpr, fields []*ast.SelectField, ignoreAsName bool) (index int, err error) {
	var matchedExpr ast.ExprNode
	index = -1
	for i, field := range fields {
		if field.Auxiliary {
			continue
		}
		if matchField(field, v, ignoreAsName) {
			curCol, isCol := field.Expr.(*ast.ColumnNameExpr)
			if !isCol {
				return i, nil
			}
			if matchedExpr == nil {
				matchedExpr = curCol
				index = i
			} else if !colMatch(matchedExpr.(*ast.ColumnNameExpr).Name, curCol.Name) &&
				!colMatch(curCol.Name, matchedExpr.(*ast.ColumnNameExpr).Name) {
				return -1, ErrAmbiguous.GenWithStackByArgs(curCol.Name.Name.L, clauseMsg[fieldList])
			}
		}
	}
	return
}

// havingAndOrderbyExprResolver visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type havingAndOrderbyExprResolver struct {
	inAggFunc    bool
	inExpr       bool
	orderBy      bool
	err          error
	p            LogicalPlan
	selectFields []*ast.SelectField
	aggMapper    map[*ast.AggregateFuncExpr]int
	colMapper    map[*ast.ColumnNameExpr]int
	gbyItems     []*ast.ByItem
	curClause    clauseCode
}

// Enter implements Visitor interface.
func (a *havingAndOrderbyExprResolver) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = true
	case *ast.ColumnNameExpr, *ast.ColumnName:
	default:
		a.inExpr = true
	}
	return n, false
}

func (a *havingAndOrderbyExprResolver) resolveFromPlan(v *ast.ColumnNameExpr, p LogicalPlan) (int, error) {
	idx, err := expression.FindFieldName(p.OutputNames(), v.Name)
	if err != nil {
		return -1, err
	}
	if idx < 0 {
		return -1, nil
	}
	col := p.Schema().Columns[idx]
	name := p.OutputNames()[idx]
	newColName := &ast.ColumnName{
		Schema: name.DBName,
		Table:  name.TblName,
		Name:   name.ColName,
	}
	for i, field := range a.selectFields {
		if c, ok := field.Expr.(*ast.ColumnNameExpr); ok && colMatch(c.Name, newColName) {
			return i, nil
		}
	}
	sf := &ast.SelectField{
		Expr:      &ast.ColumnNameExpr{Name: newColName},
		Auxiliary: true,
	}
	sf.Expr.SetType(col.GetType())
	a.selectFields = append(a.selectFields, sf)
	return len(a.selectFields) - 1, nil
}

// Leave implements Visitor interface.
func (a *havingAndOrderbyExprResolver) Leave(n ast.Node) (node ast.Node, ok bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = false
		a.aggMapper[v] = len(a.selectFields)
		a.selectFields = append(a.selectFields, &ast.SelectField{
			Auxiliary: true,
			Expr:      v,
			AsName:    model.NewCIStr(fmt.Sprintf("sel_agg_%d", len(a.selectFields))),
		})
	case *ast.ColumnNameExpr:
		resolveFieldsFirst := true
		if a.inAggFunc || (a.orderBy && a.inExpr) || a.curClause == fieldList {
			resolveFieldsFirst = false
		}
		if !a.inAggFunc && !a.orderBy {
			for _, item := range a.gbyItems {
				if col, ok := item.Expr.(*ast.ColumnNameExpr); ok &&
					(colMatch(v.Name, col.Name) || colMatch(col.Name, v.Name)) {
					resolveFieldsFirst = false
					break
				}
			}
		}
		var index int
		if resolveFieldsFirst {
			index, a.err = resolveFromSelectFields(v, a.selectFields, false)
			if a.err != nil {
				return node, false
			}
			if index == -1 {
				if a.orderBy {
					index, a.err = a.resolveFromPlan(v, a.p)
				} else {
					index, a.err = resolveFromSelectFields(v, a.selectFields, true)
				}
			}
		} else {
			// We should ignore the err when resolving from schema. Because we could resolve successfully
			// when considering select fields.
			var err error
			index, err = a.resolveFromPlan(v, a.p)
			_ = err
			if index == -1 && a.curClause != fieldList {
				index, a.err = resolveFromSelectFields(v, a.selectFields, false)
			}
		}
		if a.err != nil {
			return node, false
		}
		if index == -1 {
			a.err = ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), clauseMsg[a.curClause])
			return node, false
		}
		if a.inAggFunc {
			return a.selectFields[index].Expr, true
		}
		a.colMapper[v] = index
	}
	return n, true
}

// resolveHavingAndOrderBy will process aggregate functions and resolve the columns that don't exist in select fields.
// If we found some columns that are not in select fields, we will append it to select fields and update the colMapper.
// When we rewrite the order by / having expression, we will find column in map at first.
func (b *PlanBuilder) resolveHavingAndOrderBy(sel *ast.SelectStmt, p LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, map[*ast.AggregateFuncExpr]int, error) {
	extractor := &havingAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
	}
	if sel.GroupBy != nil {
		extractor.gbyItems = sel.GroupBy.Items
	}
	// Extract agg funcs from having clause.
	if sel.Having != nil {
		extractor.curClause = havingClause
		n, ok := sel.Having.Expr.Accept(extractor)
		if !ok {
			return nil, nil, errors.Trace(extractor.err)
		}
		sel.Having.Expr = n.(ast.ExprNode)
	}
	havingAggMapper := extractor.aggMapper
	extractor.aggMapper = make(map[*ast.AggregateFuncExpr]int)
	extractor.orderBy = true
	extractor.inExpr = false
	// Extract agg funcs from order by clause.
	if sel.OrderBy != nil {
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				return nil, nil, errors.Trace(extractor.err)
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	return havingAggMapper, extractor.aggMapper, nil
}

func (b *PlanBuilder) extractAggFuncs(fields []*ast.SelectField) ([]*ast.AggregateFuncExpr, map[*ast.AggregateFuncExpr]int) {
	extractor := &AggregateFuncExtractor{}
	for _, f := range fields {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	aggList := extractor.AggFuncs
	totalAggMapper := make(map[*ast.AggregateFuncExpr]int)

	for i, agg := range aggList {
		totalAggMapper[agg] = i
	}
	return aggList, totalAggMapper
}

// gbyResolver resolves group by items from select fields.
type gbyResolver struct {
	ctx    sessionctx.Context
	fields []*ast.SelectField
	schema *expression.Schema
	names  []*types.FieldName
	err    error
	inExpr bool

	exprDepth int // exprDepth is the depth of current expression in expression tree.
}

func (g *gbyResolver) Enter(inNode ast.Node) (ast.Node, bool) {
	g.exprDepth++
	switch inNode.(type) {
	case *driver.ValueExpr, *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.ColumnName:
	default:
		g.inExpr = true
	}
	return inNode, false
}

func (g *gbyResolver) Leave(inNode ast.Node) (ast.Node, bool) {
	extractor := &AggregateFuncExtractor{}
	switch v := inNode.(type) {
	case *ast.ColumnNameExpr:
		idx, err := expression.FindFieldName(g.names, v.Name)
		if idx < 0 || !g.inExpr {
			var index int
			index, g.err = resolveFromSelectFields(v, g.fields, false)
			if g.err != nil {
				return inNode, false
			}
			if idx >= 0 {
				return inNode, true
			}
			if index != -1 {
				ret := g.fields[index].Expr
				ret.Accept(extractor)
				if len(extractor.AggFuncs) != 0 {
					err = ErrIllegalReference.GenWithStackByArgs(v.Name.OrigColName(), "reference to group function")
				} else {
					return ret, true
				}
			}
			g.err = err
			return inNode, false
		}
	case *ast.ValuesExpr:
		if v.Column == nil {
			g.err = ErrUnknownColumn.GenWithStackByArgs("", "VALUES() function")
		}
	}
	return inNode, true
}

func (b *PlanBuilder) resolveGbyExprs(ctx context.Context, p LogicalPlan, gby *ast.GroupByClause, fields []*ast.SelectField) (LogicalPlan, []expression.Expression, error) {
	b.curClause = groupByClause
	exprs := make([]expression.Expression, 0, len(gby.Items))
	resolver := &gbyResolver{
		ctx:    b.ctx,
		fields: fields,
		schema: p.Schema(),
		names:  p.OutputNames(),
	}
	for _, item := range gby.Items {
		resolver.inExpr = false
		retExpr, _ := item.Expr.Accept(resolver)
		if resolver.err != nil {
			return nil, nil, errors.Trace(resolver.err)
		}
		item.Expr = retExpr.(ast.ExprNode)

		itemExpr := retExpr.(ast.ExprNode)
		expr, np, err := b.rewrite(ctx, itemExpr, p, nil, true)
		if err != nil {
			return nil, nil, err
		}

		exprs = append(exprs, expr)
		p = np
	}
	return p, exprs, nil
}

func (b *PlanBuilder) unfoldWildStar(p LogicalPlan, selectFields []*ast.SelectField) (resultList []*ast.SelectField, err error) {
	for i, field := range selectFields {
		if field.WildCard == nil {
			resultList = append(resultList, field)
			continue
		}
		if field.WildCard.Table.L == "" && i > 0 {
			return nil, ErrInvalidWildCard
		}
		dbName := field.WildCard.Schema
		tblName := field.WildCard.Table
		findTblNameInSchema := false
		for i, name := range p.OutputNames() {
			col := p.Schema().Columns[i]
			if (dbName.L == "" || dbName.L == name.DBName.L) &&
				(tblName.L == "" || tblName.L == name.TblName.L) &&
				col.ID != model.ExtraHandleID {
				findTblNameInSchema = true
				colName := &ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Schema: name.DBName,
						Table:  name.TblName,
						Name:   name.ColName,
					}}
				colName.SetType(col.GetType())
				field := &ast.SelectField{Expr: colName}
				field.SetText(name.ColName.O)
				resultList = append(resultList, field)
			}
		}
		if !findTblNameInSchema {
			return nil, ErrBadTable.GenWithStackByArgs(tblName)
		}
	}
	return resultList, nil
}

func (b *PlanBuilder) pushTableHints(hints []*ast.TableOptimizerHint) {
	var (
		sortMergeTables, hashJoinTables []hintTableInfo
		indexHintList                   []indexHintInfo
	)
	for _, hint := range hints {
		switch hint.HintName.L {
		case TiDBMergeJoin, HintSMJ:
			sortMergeTables = append(sortMergeTables, tableNames2HintTableInfo(b.ctx, hint.Tables)...)
		case TiDBHashJoin, HintHJ:
			hashJoinTables = append(hashJoinTables, tableNames2HintTableInfo(b.ctx, hint.Tables)...)
		case HintUseIndex:
			if len(hint.Tables) != 0 {
				dbName := hint.Tables[0].DBName
				if dbName.L == "" {
					dbName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
				}
				indexHintList = append(indexHintList, indexHintInfo{
					dbName:  dbName,
					tblName: hint.Tables[0].TableName,
					indexHint: &ast.IndexHint{
						IndexNames: hint.Indexes,
						HintType:   ast.HintUse,
						HintScope:  ast.HintForScan,
					},
				})
			}
		case HintIgnoreIndex:
			if len(hint.Tables) != 0 {
				dbName := hint.Tables[0].DBName
				if dbName.L == "" {
					dbName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
				}
				indexHintList = append(indexHintList, indexHintInfo{
					dbName:  dbName,
					tblName: hint.Tables[0].TableName,
					indexHint: &ast.IndexHint{
						IndexNames: hint.Indexes,
						HintType:   ast.HintIgnore,
						HintScope:  ast.HintForScan,
					},
				})
			}

		default:
			// ignore hints that not implemented
		}
	}
	b.tableHintInfo = append(b.tableHintInfo, tableHintInfo{
		sortMergeJoinTables: sortMergeTables,
		hashJoinTables:      hashJoinTables,
		indexHintList:       indexHintList,
	})
}

func (b *PlanBuilder) popTableHints() {
	hintInfo := b.tableHintInfo[len(b.tableHintInfo)-1]
	b.appendUnmatchedJoinHintWarning(HintSMJ, TiDBMergeJoin, hintInfo.sortMergeJoinTables)
	b.appendUnmatchedJoinHintWarning(HintHJ, TiDBHashJoin, hintInfo.hashJoinTables)
	b.tableHintInfo = b.tableHintInfo[:len(b.tableHintInfo)-1]
}

func (b *PlanBuilder) appendUnmatchedJoinHintWarning(joinType string, joinTypeAlias string, hintTables []hintTableInfo) {
	unMatchedTables := extractUnmatchedTables(hintTables)
	if len(unMatchedTables) == 0 {
		return
	}
	if len(joinTypeAlias) != 0 {
		joinTypeAlias = fmt.Sprintf(" or %s", restore2JoinHint(joinTypeAlias, hintTables))
	}

	errMsg := fmt.Sprintf("There are no matching table names for (%s) in optimizer hint %s%s. Maybe you can use the table alias name",
		strings.Join(unMatchedTables, ", "), restore2JoinHint(joinType, hintTables), joinTypeAlias)
	b.ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack(errMsg))
}

// TableHints returns the *tableHintInfo of PlanBuilder.
func (b *PlanBuilder) TableHints() *tableHintInfo {
	if len(b.tableHintInfo) == 0 {
		return nil
	}
	return &(b.tableHintInfo[len(b.tableHintInfo)-1])
}

func (b *PlanBuilder) buildSelect(ctx context.Context, sel *ast.SelectStmt) (p LogicalPlan, err error) {
	b.pushTableHints(sel.TableHints)
	defer func() {
		// table hints are only visible in the current SELECT statement.
		b.popTableHints()
	}()

	if sel.SelectStmtOpts != nil {
		origin := b.inStraightJoin
		b.inStraightJoin = sel.SelectStmtOpts.StraightJoin
		defer func() { b.inStraightJoin = origin }()
	}

	var (
		aggFuncs                      []*ast.AggregateFuncExpr
		havingMap, orderMap, totalMap map[*ast.AggregateFuncExpr]int
		gbyCols                       []expression.Expression
	)

	if sel.From != nil {
		p, err = b.buildResultSetNode(ctx, sel.From.TableRefs)
		if err != nil {
			return nil, err
		}
	} else {
		p = b.buildTableDual()
	}

	originalFields := sel.Fields.Fields
	sel.Fields.Fields, err = b.unfoldWildStar(p, sel.Fields.Fields)
	if err != nil {
		return nil, err
	}

	if sel.GroupBy != nil {
		p, gbyCols, err = b.resolveGbyExprs(ctx, p, sel.GroupBy, sel.Fields.Fields)
		if err != nil {
			return nil, err
		}
	}

	// We must resolve having and order by clause before build projection,
	// because when the query is "select a+1 as b from t having sum(b) < 0", we must replace sum(b) to sum(a+1),
	// which only can be done before building projection and extracting Agg functions.
	havingMap, orderMap, err = b.resolveHavingAndOrderBy(sel, p)
	if err != nil {
		return nil, err
	}

	if sel.Where != nil {
		p, err = b.buildSelection(ctx, p, sel.Where, nil)
		if err != nil {
			return nil, err
		}
	}

	b.handleHelper.popMap()
	b.handleHelper.pushMap(nil)

	hasAgg := b.detectSelectAgg(sel)
	if hasAgg {
		aggFuncs, totalMap = b.extractAggFuncs(sel.Fields.Fields)
		var aggIndexMap map[int]int
		p, aggIndexMap, err = b.buildAggregation(ctx, p, aggFuncs, gbyCols)
		if err != nil {
			return nil, err
		}
		for k, v := range totalMap {
			totalMap[k] = aggIndexMap[v]
		}
	}

	var oldLen int
	p, oldLen, err = b.buildProjection(ctx, p, sel.Fields.Fields, totalMap)
	if err != nil {
		return nil, err
	}

	if sel.Having != nil {
		b.curClause = havingClause
		p, err = b.buildSelection(ctx, p, sel.Having.Expr, havingMap)
		if err != nil {
			return nil, err
		}
	}

	if sel.Distinct {
		p, err = b.buildDistinct(p, oldLen)
		if err != nil {
			return nil, err
		}
	}

	if sel.OrderBy != nil {
		p, err = b.buildSort(ctx, p, sel.OrderBy.Items, orderMap)
		if err != nil {
			return nil, err
		}
	}

	if sel.Limit != nil {
		p, err = b.buildLimit(p, sel.Limit)
		if err != nil {
			return nil, err
		}
	}

	sel.Fields.Fields = originalFields
	if oldLen != p.Schema().Len() {
		proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.Init(b.ctx)
		proj.SetChildren(p)
		schema := expression.NewSchema(p.Schema().Clone().Columns[:oldLen]...)
		for _, col := range schema.Columns {
			col.UniqueID = b.ctx.GetSessionVars().AllocPlanColumnID()
		}
		proj.names = p.OutputNames()[:oldLen]
		proj.SetSchema(schema)
		return proj, nil
	}

	return p, nil
}

func (b *PlanBuilder) buildTableDual() *LogicalTableDual {
	b.handleHelper.pushMap(nil)
	return LogicalTableDual{RowCount: 1}.Init(b.ctx)
}

func (ds *DataSource) newExtraHandleSchemaCol() *expression.Column {
	return &expression.Column{
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		UniqueID: ds.ctx.GetSessionVars().AllocPlanColumnID(),
		ID:       model.ExtraHandleID,
		OrigName: fmt.Sprintf("%v.%v.%v", ds.DBName, ds.tableInfo.Name, model.ExtraHandleName),
	}
}

// getStatsTable gets statistics information for a table specified by "tableID".
// A pseudo statistics table is returned in any of the following scenario:
// 1. tidb-server started and statistics handle has not been initialized.
// 2. table row count from statistics is zero.
// 3. statistics is outdated.
func getStatsTable(ctx sessionctx.Context, tblInfo *model.TableInfo, pid int64) *statistics.Table {
	statsHandle := domain.GetDomain(ctx).StatsHandle()

	// 1. tidb-server started and statistics handle has not been initialized.
	if statsHandle == nil {
		return statistics.PseudoTable(tblInfo)
	}

	var statsTbl *statistics.Table
	if pid != tblInfo.ID {
		statsTbl = statsHandle.GetPartitionStats(tblInfo, pid)
	} else {
		statsTbl = statsHandle.GetTableStats(tblInfo)
	}

	// 2. table row count from statistics is zero.
	if statsTbl.Count == 0 {
		return statistics.PseudoTable(tblInfo)
	}

	// 3. statistics is outdated.
	if statsTbl.IsOutdated() {
		tbl := *statsTbl
		tbl.Pseudo = true
		statsTbl = &tbl

	}
	return statsTbl
}

func (b *PlanBuilder) buildDataSource(ctx context.Context, tn *ast.TableName, asName *model.CIStr) (LogicalPlan, error) {
	dbName := tn.Schema
	if dbName.L == "" {
		dbName = model.NewCIStr(b.ctx.GetSessionVars().CurrentDB)
	}

	tbl, err := b.is.TableByName(dbName, tn.Name)
	if err != nil {
		return nil, err
	}

	tableInfo := tbl.Meta()

	if tbl.Type().IsVirtualTable() {
		return b.buildMemTable(ctx, dbName, tableInfo)
	}

	tblName := *asName
	if tblName.L == "" {
		tblName = tn.Name
	}
	possiblePaths, err := b.getPossibleAccessPaths(tn.IndexHints, tbl, dbName, tblName)
	if err != nil {
		return nil, err
	}

	columns := tbl.Cols()
	ds := DataSource{
		DBName:              dbName,
		TableAsName:         asName,
		table:               tbl,
		tableInfo:           tableInfo,
		statisticTable:      getStatsTable(b.ctx, tbl.Meta(), tbl.Meta().ID),
		indexHints:          tn.IndexHints,
		possibleAccessPaths: possiblePaths,
		Columns:             make([]*model.ColumnInfo, 0, len(columns)),
		TblCols:             make([]*expression.Column, 0, len(columns)),
	}.Init(b.ctx)

	var handleCol *expression.Column
	schema := expression.NewSchema(make([]*expression.Column, 0, len(columns))...)
	names := make([]*types.FieldName, 0, len(columns))
	for i, col := range columns {
		ds.Columns = append(ds.Columns, col.ToInfo())
		names = append(names, &types.FieldName{
			DBName:      dbName,
			TblName:     tableInfo.Name,
			ColName:     col.Name,
			OrigTblName: tableInfo.Name,
			OrigColName: col.Name,
		})
		newCol := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			ID:       col.ID,
			RetType:  &col.FieldType,
			OrigName: names[i].String(),
		}

		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			handleCol = newCol
		}
		schema.Append(newCol)
		ds.TblCols = append(ds.TblCols, newCol)
	}
	// We append an extra handle column to the schema when the handle
	// column is not the primary key of "ds".
	if handleCol == nil {
		ds.Columns = append(ds.Columns, model.NewExtraHandleColInfo())
		handleCol = ds.newExtraHandleSchemaCol()
		schema.Append(handleCol)
		names = append(names, &types.FieldName{
			DBName:      dbName,
			TblName:     tableInfo.Name,
			ColName:     model.ExtraHandleName,
			OrigColName: model.ExtraHandleName,
		})
		ds.TblCols = append(ds.TblCols, handleCol)
	}
	if handleCol != nil {
		ds.handleCol = handleCol
		handleMap := make(map[int64][]*expression.Column)
		handleMap[tableInfo.ID] = []*expression.Column{handleCol}
		b.handleHelper.pushMap(handleMap)
	} else {
		b.handleHelper.pushMap(nil)
	}
	ds.SetSchema(schema)
	ds.names = names

	// Init FullIdxCols, FullIdxColLens for accessPaths.
	for _, path := range ds.possibleAccessPaths {
		if !path.IsTablePath {
			path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)
		}
	}

	var result LogicalPlan = ds

	// If this SQL is executed in a non-readonly transaction, we need a
	// "UnionScan" operator to read the modifications of former SQLs, which is
	// buffered in tidb-server memory.
	txn, err := b.ctx.Txn(false)
	if err != nil {
		return nil, err
	}
	if txn.Valid() && !txn.IsReadOnly() {
		us := LogicalUnionScan{handleCol: handleCol}.Init(b.ctx)
		us.SetChildren(ds)
		result = us
	}
	return result, nil
}

func (b *PlanBuilder) buildMemTable(ctx context.Context, dbName model.CIStr, tableInfo *model.TableInfo) (LogicalPlan, error) {
	// We can use the `tableInfo.Columns` directly because the memory table has
	// a stable schema and there is no online DDL on the memory table.
	schema := expression.NewSchema(make([]*expression.Column, 0, len(tableInfo.Columns))...)
	names := make([]*types.FieldName, 0, len(tableInfo.Columns))
	var handleCol *expression.Column
	for _, col := range tableInfo.Columns {
		names = append(names, &types.FieldName{
			DBName:      dbName,
			TblName:     tableInfo.Name,
			ColName:     col.Name,
			OrigTblName: tableInfo.Name,
			OrigColName: col.Name,
		})
		// NOTE: Rewrite the expression if memory table supports generated columns in the future
		newCol := &expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			ID:       col.ID,
			RetType:  &col.FieldType,
		}
		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			handleCol = newCol
		}
		schema.Append(newCol)
	}

	if handleCol != nil {
		handleMap := make(map[int64][]*expression.Column)
		handleMap[tableInfo.ID] = []*expression.Column{handleCol}
		b.handleHelper.pushMap(handleMap)
	} else {
		b.handleHelper.pushMap(nil)
	}

	// NOTE: Add a `LogicalUnionScan` if we support update memory table in the future
	p := LogicalMemTable{
		dbName:    dbName,
		tableInfo: tableInfo,
	}.Init(b.ctx)
	p.SetSchema(schema)
	p.names = names
	return p, nil
}

func getTableOffset(names []*types.FieldName, handleName *types.FieldName) (int, error) {
	for i, name := range names {
		if name.DBName.L == handleName.DBName.L && name.TblName.L == handleName.TblName.L {
			return i, nil
		}
	}
	return -1, errors.Errorf("Couldn't get column information when do update/delete")
}

// TblColPosInfo represents an mapper from column index to handle index.
type TblColPosInfo struct {
	TblID int64
	// Start and End represent the ordinal range [Start, End) of the consecutive columns.
	Start, End int
	// HandleOrdinal represents the ordinal of the handle column.
	HandleOrdinal int
}

// TblColPosInfoSlice attaches the methods of sort.Interface to []TblColPosInfos sorting in increasing order.
type TblColPosInfoSlice []TblColPosInfo

// Len implements sort.Interface#Len.
func (c TblColPosInfoSlice) Len() int {
	return len(c)
}

// Swap implements sort.Interface#Swap.
func (c TblColPosInfoSlice) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

// Less implements sort.Interface#Less.
func (c TblColPosInfoSlice) Less(i, j int) bool {
	return c[i].Start < c[j].Start
}

// FindHandle finds the ordinal of the corresponding handle column.
func (c TblColPosInfoSlice) FindHandle(colOrdinal int) (int, bool) {
	if len(c) == 0 {
		return 0, false
	}
	// find the smallest index of the range that its start great than colOrdinal.
	// @see https://godoc.org/sort#Search
	rangeBehindOrdinal := sort.Search(len(c), func(i int) bool { return c[i].Start > colOrdinal })
	if rangeBehindOrdinal == 0 {
		return 0, false
	}
	return c[rangeBehindOrdinal-1].HandleOrdinal, true
}

// buildColumns2Handle builds columns to handle mapping.
func buildColumns2Handle(
	names []*types.FieldName,
	tblID2Handle map[int64][]*expression.Column,
	tblID2Table map[int64]table.Table,
	onlyWritableCol bool,
) (TblColPosInfoSlice, error) {
	var cols2Handles TblColPosInfoSlice
	for tblID, handleCols := range tblID2Handle {
		tbl := tblID2Table[tblID]
		var tblLen int
		if onlyWritableCol {
			tblLen = len(tbl.WritableCols())
		} else {
			tblLen = len(tbl.Cols())
		}
		for _, handleCol := range handleCols {
			offset, err := getTableOffset(names, names[handleCol.Index])
			if err != nil {
				return nil, err
			}
			end := offset + tblLen
			cols2Handles = append(cols2Handles, TblColPosInfo{tblID, offset, end, handleCol.Index})
		}
	}
	sort.Sort(cols2Handles)
	return cols2Handles, nil
}

// extractDefaultExpr extract a `DefaultExpr` from `ExprNode`,
// If it is a `DEFAULT` function like `DEFAULT(a)`, return nil.
// Only if it is `DEFAULT` keyword, it will return the `DefaultExpr`.
func extractDefaultExpr(node ast.ExprNode) *ast.DefaultExpr {
	if expr, ok := node.(*ast.DefaultExpr); ok && expr.Name == nil {
		return expr
	}
	return nil
}

func (b *PlanBuilder) buildDelete(ctx context.Context, delete *ast.DeleteStmt) (Plan, error) {
	p, err := b.buildResultSetNode(ctx, delete.TableRefs.TableRefs)
	if err != nil {
		return nil, err
	}
	oldSchema := p.Schema()
	oldLen := oldSchema.Len()

	if delete.Where != nil {
		p, err = b.buildSelection(ctx, p, delete.Where, nil)
		if err != nil {
			return nil, err
		}
	}

	if delete.Order != nil {
		p, err = b.buildSort(ctx, p, delete.Order.Items, nil)
		if err != nil {
			return nil, err
		}
	}

	if delete.Limit != nil {
		p, err = b.buildLimit(p, delete.Limit)
		if err != nil {
			return nil, err
		}
	}

	proj := LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns[:oldLen])}.Init(b.ctx)
	proj.SetChildren(p)
	proj.SetSchema(oldSchema.Clone())
	proj.names = p.OutputNames()[:oldLen]
	p = proj

	del := Delete{}.Init(b.ctx)

	del.names = p.OutputNames()
	del.SelectPlan, err = DoOptimize(ctx, b.optFlag, p)
	if err != nil {
		return nil, err
	}

	tblID2Handle, err := resolveIndicesForTblID2Handle(b.handleHelper.tailMap(), del.SelectPlan.Schema())
	if err != nil {
		return nil, err
	}
	tblID2table := make(map[int64]table.Table)
	for id := range tblID2Handle {
		tblID2table[id], _ = b.is.TableByID(id)
	}
	del.TblColPosInfos, err = buildColumns2Handle(del.names, tblID2Handle, tblID2table, false)
	return del, err
}

func resolveIndicesForTblID2Handle(tblID2Handle map[int64][]*expression.Column, schema *expression.Schema) (map[int64][]*expression.Column, error) {
	newMap := make(map[int64][]*expression.Column, len(tblID2Handle))
	for i, cols := range tblID2Handle {
		for _, col := range cols {
			resolvedCol, err := col.ResolveIndices(schema)
			if err != nil {
				return nil, err
			}
			newMap[i] = append(newMap[i], resolvedCol.(*expression.Column))
		}
	}
	return newMap, nil
}

func getInnerFromParenthesesAndUnaryPlus(expr ast.ExprNode) ast.ExprNode {
	if pexpr, ok := expr.(*ast.ParenthesesExpr); ok {
		return getInnerFromParenthesesAndUnaryPlus(pexpr.Expr)
	}
	if uexpr, ok := expr.(*ast.UnaryOperationExpr); ok && uexpr.Op == opcode.Plus {
		return getInnerFromParenthesesAndUnaryPlus(uexpr.V)
	}
	return expr
}

// containDifferentJoinTypes checks whether `preferJoinType` contains different
// join types.
func containDifferentJoinTypes(preferJoinType uint) bool {
	return bits.OnesCount(preferJoinType) > 1
}
