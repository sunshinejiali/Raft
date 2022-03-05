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
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

var (
	_ LogicalPlan = &LogicalJoin{}
	_ LogicalPlan = &LogicalAggregation{}
	_ LogicalPlan = &LogicalProjection{}
	_ LogicalPlan = &LogicalSelection{}
	_ LogicalPlan = &LogicalTableDual{}
	_ LogicalPlan = &DataSource{}
	_ LogicalPlan = &TiKVSingleGather{}
	_ LogicalPlan = &LogicalTableScan{}
	_ LogicalPlan = &LogicalIndexScan{}
	_ LogicalPlan = &LogicalSort{}
	_ LogicalPlan = &LogicalLimit{}
)

// JoinType contains CrossJoin, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin, SemiJoin.
type JoinType int

const (
	// InnerJoin means inner join.
	InnerJoin JoinType = iota
	// LeftOuterJoin means left join.
	LeftOuterJoin
	// RightOuterJoin means right join.
	RightOuterJoin
)

// IsOuterJoin returns if this joiner is a outer joiner
func (tp JoinType) IsOuterJoin() bool {
	return tp == LeftOuterJoin || tp == RightOuterJoin
}

func (tp JoinType) String() string {
	switch tp {
	case InnerJoin:
		return "inner join"
	case LeftOuterJoin:
		return "left outer join"
	case RightOuterJoin:
		return "right outer join"
	}
	return "unsupported join type"
}

const (
	preferHashJoin uint = 1 << iota
	preferMergeJoin
)

// LogicalJoin is the logical join plan.
type LogicalJoin struct {
	logicalSchemaProducer

	JoinType      JoinType
	reordered     bool
	cartesianJoin bool
	StraightJoin  bool

	// hintInfo stores the join algorithm hint information specified by client.
	hintInfo       *tableHintInfo
	preferJoinType uint

	EqualConditions []*expression.ScalarFunction
	LeftConditions  expression.CNFExprs
	RightConditions expression.CNFExprs
	OtherConditions expression.CNFExprs

	LeftJoinKeys    []*expression.Column
	RightJoinKeys   []*expression.Column
	leftProperties  [][]*expression.Column
	rightProperties [][]*expression.Column

	// DefaultValues is only used for left/right outer join, which is values the inner row's should be when the outer table
	// doesn't match any inner table's row.
	// That it's nil just means the default values is a slice of NULL.
	// Currently, only `aggregation push down` phase will set this.
	DefaultValues []types.Datum

	// equalCondOutCnt indicates the estimated count of joined rows after evaluating `EqualConditions`.
	equalCondOutCnt float64
}

func (p *LogicalJoin) attachOnConds(onConds []expression.Expression) {
	eq, left, right, other := p.extractOnCondition(onConds, false, false)
	p.EqualConditions = append(eq, p.EqualConditions...)
	p.LeftConditions = append(left, p.LeftConditions...)
	p.RightConditions = append(right, p.RightConditions...)
	p.OtherConditions = append(other, p.OtherConditions...)
}

// LogicalProjection represents a select fields plan.
type LogicalProjection struct {
	logicalSchemaProducer

	Exprs []expression.Expression
}

// LogicalAggregation represents an aggregate plan.
type LogicalAggregation struct {
	logicalSchemaProducer

	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression
	// groupByCols stores the columns that are group-by items.
	groupByCols []*expression.Column

	possibleProperties [][]*expression.Column
	inputCount         float64 // inputCount is the input count of this plan.
}

// IsPartialModeAgg returns if all of the AggFuncs are partialMode.
func (la *LogicalAggregation) IsPartialModeAgg() bool {
	// Since all of the AggFunc share the same AggMode, we only need to check the first one.
	return la.AggFuncs[0].Mode == aggregation.Partial1Mode
}

// GetGroupByCols returns the groupByCols. If the groupByCols haven't be collected,
// this method would collect them at first. If the GroupByItems have been changed,
// we should explicitly collect GroupByColumns before this method.
func (la *LogicalAggregation) GetGroupByCols() []*expression.Column {
	if la.groupByCols == nil {
		la.collectGroupByColumns()
	}
	return la.groupByCols
}

// LogicalSelection represents a where or having predicate.
type LogicalSelection struct {
	baseLogicalPlan

	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []expression.Expression
}

// LogicalTableDual represents a dual table plan.
type LogicalTableDual struct {
	logicalSchemaProducer

	RowCount int
}

// LogicalMemTable represents a memory table or virtual table
type LogicalMemTable struct {
	logicalSchemaProducer

	dbName    model.CIStr
	tableInfo *model.TableInfo
}

// LogicalUnionScan is only used in non read-only txn.
type LogicalUnionScan struct {
	baseLogicalPlan

	conditions []expression.Expression

	handleCol *expression.Column
}

// DataSource represents a tableScan without condition push down.
type DataSource struct {
	logicalSchemaProducer

	indexHints []*ast.IndexHint
	table      table.Table
	tableInfo  *model.TableInfo
	Columns    []*model.ColumnInfo
	DBName     model.CIStr

	TableAsName *model.CIStr
	// pushedDownConds are the conditions that will be pushed down to coprocessor.
	pushedDownConds []expression.Expression
	// allConds contains all the filters on this table. For now it's maintained
	// in predicate push down and used only in partition pruning.
	allConds []expression.Expression

	statisticTable *statistics.Table
	tableStats     *property.StatsInfo

	// possibleAccessPaths stores all the possible access path for physical plan, including table scan.
	possibleAccessPaths []*util.AccessPath

	// handleCol represents the handle column for the datasource, either the
	// int primary key column or extra handle column.
	handleCol *expression.Column
	// TblCols contains the original columns of table before being pruned, and it
	// is used for estimating table scan cost.
	TblCols []*expression.Column
	// TblColHists contains the Histogram of all original table columns,
	// it is converted from statisticTable, and used for IO/network cost estimating.
	TblColHists *statistics.HistColl
}

// TiKVSingleGather is a leaf logical operator of TiDB layer to gather
// tuples from TiKV regions.
type TiKVSingleGather struct {
	logicalSchemaProducer
	Source *DataSource
	// IsIndexGather marks if this TiKVSingleGather gathers tuples from an IndexScan.
	// in implementation phase, we need this flag to determine whether to generate
	// PhysicalTableReader or PhysicalIndexReader.
	IsIndexGather bool
	Index         *model.IndexInfo
}

// LogicalTableScan is the logical table scan operator for TiKV.
type LogicalTableScan struct {
	logicalSchemaProducer
	Source      *DataSource
	Handle      *expression.Column
	AccessConds expression.CNFExprs
	Ranges      []*ranger.Range
}

// LogicalIndexScan is the logical index scan operator for TiKV.
type LogicalIndexScan struct {
	logicalSchemaProducer
	// DataSource should be read-only here.
	Source       *DataSource
	IsDoubleRead bool

	EqCondCount int
	AccessConds expression.CNFExprs
	Ranges      []*ranger.Range

	Index          *model.IndexInfo
	Columns        []*model.ColumnInfo
	FullIdxCols    []*expression.Column
	FullIdxColLens []int
	IdxCols        []*expression.Column
	IdxColLens     []int
}

// MatchIndexProp checks if the indexScan can match the required property.
func (p *LogicalIndexScan) MatchIndexProp(prop *property.PhysicalProperty) (match bool) {
	if prop.IsEmpty() {
		return true
	}
	if all, _ := prop.AllSameOrder(); !all {
		return false
	}
	for i, col := range p.IdxCols {
		if col.Equal(nil, prop.Items[0].Col) {
			return matchIndicesProp(p.IdxCols[i:], p.IdxColLens[i:], prop.Items)
		} else if i >= p.EqCondCount {
			break
		}
	}
	return false
}

// getTablePath finds the TablePath from a group of accessPaths.
func getTablePath(paths []*util.AccessPath) *util.AccessPath {
	for _, path := range paths {
		if path.IsTablePath {
			return path
		}
	}
	return nil
}

func (ds *DataSource) buildTableGather() LogicalPlan {
	ts := LogicalTableScan{Source: ds, Handle: ds.getHandleCol()}.Init(ds.ctx)
	ts.SetSchema(ds.Schema())
	sg := TiKVSingleGather{Source: ds, IsIndexGather: false}.Init(ds.ctx)
	sg.SetSchema(ds.Schema())
	sg.SetChildren(ts)
	return sg
}

func (ds *DataSource) buildIndexGather(path *util.AccessPath) LogicalPlan {
	is := LogicalIndexScan{
		Source:         ds,
		IsDoubleRead:   false,
		Index:          path.Index,
		FullIdxCols:    path.FullIdxCols,
		FullIdxColLens: path.FullIdxColLens,
		IdxCols:        path.IdxCols,
		IdxColLens:     path.IdxColLens,
	}.Init(ds.ctx)

	is.Columns = make([]*model.ColumnInfo, len(ds.Columns))
	copy(is.Columns, ds.Columns)
	is.SetSchema(ds.Schema())
	is.IdxCols, is.IdxColLens = expression.IndexInfo2PrefixCols(is.Columns, is.schema.Columns, is.Index)

	sg := TiKVSingleGather{
		Source:        ds,
		IsIndexGather: true,
		Index:         path.Index,
	}.Init(ds.ctx)
	sg.SetSchema(ds.Schema())
	sg.SetChildren(is)
	return sg
}

// Convert2Gathers builds logical TiKVSingleGathers from DataSource.
func (ds *DataSource) Convert2Gathers() (gathers []LogicalPlan) {
	tg := ds.buildTableGather()
	gathers = append(gathers, tg)
	for _, path := range ds.possibleAccessPaths {
		if !path.IsTablePath {
			path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)
			path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.schema.Columns, path.Index)
			// If index columns can cover all of the needed columns, we can use a IndexGather + IndexScan.
			if isCoveringIndex(ds.schema.Columns, path.FullIdxCols, path.FullIdxColLens, ds.tableInfo.PKIsHandle) {
				gathers = append(gathers, ds.buildIndexGather(path))
			}
			// TODO: If index columns can not cover the schema, use IndexLookUpGather.
		}
	}
	return gathers
}

// deriveTablePathStats will fulfill the information that the AccessPath need.
// And it will check whether the primary key is covered only by point query.
func (ds *DataSource) deriveTablePathStats(path *util.AccessPath, conds []expression.Expression) (bool, error) {
	var err error
	sc := ds.ctx.GetSessionVars().StmtCtx
	path.CountAfterAccess = float64(ds.statisticTable.Count)
	path.TableFilters = conds
	var pkCol *expression.Column
	columnLen := len(ds.schema.Columns)
	isUnsigned := false
	if ds.tableInfo.PKIsHandle {
		if pkColInfo := ds.tableInfo.GetPkColInfo(); pkColInfo != nil {
			isUnsigned = mysql.HasUnsignedFlag(pkColInfo.Flag)
			pkCol = expression.ColInfo2Col(ds.schema.Columns, pkColInfo)
		}
	} else if columnLen > 0 && ds.schema.Columns[columnLen-1].ID == model.ExtraHandleID {
		pkCol = ds.schema.Columns[columnLen-1]
	}
	if pkCol == nil {
		path.Ranges = ranger.FullIntRange(isUnsigned)
		return false, nil
	}

	path.Ranges = ranger.FullIntRange(isUnsigned)
	if len(conds) == 0 {
		return false, nil
	}
	path.AccessConds, path.TableFilters = ranger.DetachCondsForColumn(ds.ctx, conds, pkCol)
	path.Ranges, err = ranger.BuildTableRange(path.AccessConds, sc, pkCol.RetType)
	if err != nil {
		return false, err
	}
	path.CountAfterAccess, err = ds.statisticTable.GetRowCountByIntColumnRanges(sc, pkCol.ID, path.Ranges)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/selectionFactor, float64(ds.statisticTable.Count))
	}
	// Check whether the primary key is covered by point query.
	noIntervalRange := true
	for _, ran := range path.Ranges {
		if !ran.IsPoint(sc) {
			noIntervalRange = false
			break
		}
	}
	return noIntervalRange, err
}

func (ds *DataSource) fillIndexPath(path *util.AccessPath, conds []expression.Expression) error {
	sc := ds.ctx.GetSessionVars().StmtCtx
	path.Ranges = ranger.FullRange()
	path.CountAfterAccess = float64(ds.statisticTable.Count)
	path.IdxCols, path.IdxColLens = expression.IndexInfo2PrefixCols(ds.Columns, ds.schema.Columns, path.Index)
	path.FullIdxCols, path.FullIdxColLens = expression.IndexInfo2Cols(ds.Columns, ds.schema.Columns, path.Index)
	if !path.Index.Unique && !path.Index.Primary && len(path.Index.Columns) == len(path.IdxCols) {
		handleCol := ds.getPKIsHandleCol()
		if handleCol != nil && !mysql.HasUnsignedFlag(handleCol.RetType.Flag) {
			path.IdxCols = append(path.IdxCols, handleCol)
			path.IdxColLens = append(path.IdxColLens, types.UnspecifiedLength)
		}
	}
	if len(path.IdxCols) != 0 {
		res, err := ranger.DetachCondAndBuildRangeForIndex(ds.ctx, conds, path.IdxCols, path.IdxColLens)
		if err != nil {
			return err
		}
		path.Ranges = res.Ranges
		path.AccessConds = res.AccessConds
		path.TableFilters = res.RemainedConds
		path.EqCondCount = res.EqCondCount
		path.EqOrInCondCount = res.EqOrInCount
		path.IsDNFCond = res.IsDNFCond
		path.CountAfterAccess, err = ds.tableStats.HistColl.GetRowCountByIndexRanges(sc, path.Index.ID, path.Ranges)
		if err != nil {
			return err
		}
	} else {
		path.TableFilters = conds
	}
	return nil
}

// deriveIndexPathStats will fulfill the information that the AccessPath need.
// And it will check whether this index is full matched by point query. We will use this check to
// determine whether we remove other paths or not.
// conds is the conditions used to generate the DetachRangeResult for path.
func (ds *DataSource) deriveIndexPathStats(path *util.AccessPath) bool {
	sc := ds.ctx.GetSessionVars().StmtCtx
	if path.EqOrInCondCount == len(path.AccessConds) {
		accesses, remained := path.SplitAccessCondFromFilters(path.EqOrInCondCount)
		path.AccessConds = append(path.AccessConds, accesses...)
		path.TableFilters = remained
		if len(accesses) > 0 && ds.statisticTable.Pseudo {
			path.CountAfterAccess = ds.statisticTable.PseudoAvgCountPerValue()
		} else {
			selectivity := path.CountAfterAccess / float64(ds.statisticTable.Count)
			for i := range accesses {
				col := path.IdxCols[path.EqOrInCondCount+i]
				ndv := ds.getColumnNDV(col.ID)
				ndv *= selectivity
				if ndv < 1 {
					ndv = 1.0
				}
				path.CountAfterAccess = path.CountAfterAccess / ndv
			}
		}
	}
	path.IndexFilters, path.TableFilters = splitIndexFilterConditions(path.TableFilters, path.FullIdxCols, path.FullIdxColLens, ds.tableInfo)
	// If the `CountAfterAccess` is less than `stats.RowCount`, there must be some inconsistent stats info.
	// We prefer the `stats.RowCount` because it could use more stats info to calculate the selectivity.
	if path.CountAfterAccess < ds.stats.RowCount {
		path.CountAfterAccess = math.Min(ds.stats.RowCount/selectionFactor, float64(ds.statisticTable.Count))
	}
	if path.IndexFilters != nil {
		selectivity, err := ds.tableStats.HistColl.Selectivity(ds.ctx, path.IndexFilters, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = selectionFactor
		}
		path.CountAfterIndex = math.Max(path.CountAfterAccess*selectivity, ds.stats.RowCount)
	}
	// Check whether there's only point query.
	noIntervalRanges := true
	haveNullVal := false
	for _, ran := range path.Ranges {
		// Not point or the not full matched.
		if !ran.IsPoint(sc) || len(ran.HighVal) != len(path.Index.Columns) {
			noIntervalRanges = false
			break
		}
		// Check whether there's null value.
		for i := 0; i < len(path.Index.Columns); i++ {
			if ran.HighVal[i].IsNull() {
				haveNullVal = true
				break
			}
		}
		if haveNullVal {
			break
		}
	}
	return noIntervalRanges && !haveNullVal
}

func getPKIsHandleColFromSchema(cols []*model.ColumnInfo, schema *expression.Schema, pkIsHandle bool) *expression.Column {
	if !pkIsHandle {
		// If the PKIsHandle is false, return the ExtraHandleColumn.
		for i, col := range cols {
			if col.ID == model.ExtraHandleID {
				return schema.Columns[i]
			}
		}
		return nil
	}
	for i, col := range cols {
		if mysql.HasPriKeyFlag(col.Flag) {
			return schema.Columns[i]
		}
	}
	return nil
}

func (ds *DataSource) getPKIsHandleCol() *expression.Column {
	return getPKIsHandleColFromSchema(ds.Columns, ds.schema, ds.tableInfo.PKIsHandle)
}

func (p *LogicalIndexScan) getPKIsHandleCol(schema *expression.Schema) *expression.Column {
	// We cannot use p.Source.getPKIsHandleCol() here,
	// Because we may re-prune p.Columns and p.schema during the transformation.
	// That will make p.Columns different from p.Source.Columns.
	return getPKIsHandleColFromSchema(p.Columns, schema, p.Source.tableInfo.PKIsHandle)
}

func (ds *DataSource) getHandleCol() *expression.Column {
	if ds.handleCol != nil {
		return ds.handleCol
	}

	if !ds.tableInfo.PKIsHandle {
		ds.handleCol = ds.newExtraHandleSchemaCol()
		return ds.handleCol
	}

	for i, col := range ds.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			ds.handleCol = ds.schema.Columns[i]
			break
		}
	}

	return ds.handleCol
}

// TableInfo returns the *TableInfo of data source.
func (ds *DataSource) TableInfo() *model.TableInfo {
	return ds.tableInfo
}

// LogicalSort stands for the order by plan.
type LogicalSort struct {
	baseLogicalPlan

	ByItems []*ByItems
}

// LogicalTopN represents a top-n plan.
type LogicalTopN struct {
	baseLogicalPlan

	ByItems []*ByItems
	Offset  uint64
	Count   uint64
}

// isLimit checks if TopN is a limit plan.
func (lt *LogicalTopN) isLimit() bool {
	return len(lt.ByItems) == 0
}

// LogicalLimit represents offset and limit plan.
type LogicalLimit struct {
	baseLogicalPlan

	Offset uint64
	Count  uint64
}

// ShowContents stores the contents for the `SHOW` statement.
type ShowContents struct {
	Tp          ast.ShowStmtType // Databases/Tables/Columns/....
	DBName      string
	Table       *ast.TableName  // Used for showing columns.
	Column      *ast.ColumnName // Used for `desc table column`.
	IndexName   model.CIStr
	Flag        int // Some flag parsed from sql, such as FULL.
	Full        bool
	IfNotExists bool // Used for `show create database if not exists`.

	GlobalScope bool // Used by show variables.
}

// LogicalShow represents a show plan.
type LogicalShow struct {
	logicalSchemaProducer
	ShowContents
}

// LogicalShowDDLJobs is for showing DDL job list.
type LogicalShowDDLJobs struct {
	logicalSchemaProducer

	JobNumber int64
}
