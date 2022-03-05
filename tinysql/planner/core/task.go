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
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
)

// task is a new version of `PhysicalPlanInfo`. It stores cost information for a task.
// A task may be CopTask, RootTask, MPPTask or a ParallelTask.
type task interface {
	count() float64
	addCost(cost float64)
	cost() float64
	copy() task
	plan() PhysicalPlan
	invalid() bool
}

// copTask is a task that runs in a distributed kv store.
// TODO: In future, we should split copTask to indexTask and tableTask.
type copTask struct {
	indexPlan PhysicalPlan
	tablePlan PhysicalPlan
	cst       float64
	// indexPlanFinished means we have finished index plan.
	indexPlanFinished bool
	// keepOrder indicates if the plan scans data by order.
	keepOrder bool
	// In double read case, it may output one more column for handle(row id).
	// We need to prune it, so we add a project do this.
	doubleReadNeedProj bool

	extraHandleCol *expression.Column
	// tblColHists stores the original stats of DataSource, it is used to get
	// average row width when computing network cost.
	tblColHists *statistics.HistColl
	// tblCols stores the original columns of DataSource before being pruned, it
	// is used to compute average row width when computing scan cost.
	tblCols []*expression.Column
	// rootTaskConds stores select conditions containing virtual columns.
	// These conditions can't push to TiKV, so we have to add a selection for rootTask
	rootTaskConds []expression.Expression
}

func (t *copTask) invalid() bool {
	return t.tablePlan == nil && t.indexPlan == nil
}

func (t *rootTask) invalid() bool {
	return t.p == nil
}

func (t *copTask) count() float64 {
	if t.indexPlanFinished {
		return t.tablePlan.statsInfo().RowCount
	}
	return t.indexPlan.statsInfo().RowCount
}

func (t *copTask) addCost(cst float64) {
	t.cst += cst
}

func (t *copTask) cost() float64 {
	return t.cst
}

func (t *copTask) copy() task {
	nt := *t
	return &nt
}

func (t *copTask) plan() PhysicalPlan {
	if t.indexPlanFinished {
		return t.tablePlan
	}
	return t.indexPlan
}

func attachPlan2Task(p PhysicalPlan, t task) task {
	switch v := t.(type) {
	case *copTask:
		if v.indexPlanFinished {
			p.SetChildren(v.tablePlan)
			v.tablePlan = p
		} else {
			p.SetChildren(v.indexPlan)
			v.indexPlan = p
		}
	case *rootTask:
		p.SetChildren(v.p)
		v.p = p
	}
	return t
}

// finishIndexPlan means we no longer add plan to index plan, and compute the network cost for it.
func (t *copTask) finishIndexPlan() {
	if t.indexPlanFinished {
		return
	}
	cnt := t.count()
	t.indexPlanFinished = true
	sessVars := t.indexPlan.SCtx().GetSessionVars()
	// Network cost of transferring rows of index scan to TiDB.
	t.cst += cnt * sessVars.NetworkFactor * t.tblColHists.GetAvgRowSize(t.indexPlan.Schema().Columns, true)
	if t.tablePlan == nil {
		return
	}
	// Calculate the IO cost of table scan here because we cannot know its stats until we finish index plan.
	t.tablePlan.(*PhysicalTableScan).stats = t.indexPlan.statsInfo()
	var p PhysicalPlan
	for p = t.indexPlan; len(p.Children()) > 0; p = p.Children()[0] {
	}
	rowSize := t.tblColHists.GetIndexAvgRowSize(t.tblCols, p.(*PhysicalIndexScan).Index.Unique)
	t.cst += cnt * rowSize * sessVars.ScanFactor
}

func (p *basePhysicalPlan) attach2Task(tasks ...task) task {
	t := finishCopTask(p.ctx, tasks[0].copy())
	return attachPlan2Task(p.self, t)
}

// GetCost computes cost of hash join operator itself.
func (p *PhysicalHashJoin) GetCost(lCnt, rCnt float64) float64 {
	buildCnt, probeCnt := lCnt, rCnt
	// Taking the right as the inner for right join or using the outer to build a hash table.
	if p.InnerChildIdx == 1 {
		buildCnt, probeCnt = rCnt, lCnt
	}
	sessVars := p.ctx.GetSessionVars()
	// Cost of building hash table.
	cpuCost := buildCnt * sessVars.CPUFactor
	memoryCost := buildCnt * sessVars.MemoryFactor
	// Number of matched row pairs regarding the equal join conditions.
	helper := &fullJoinRowCountHelper{
		cartesian:     false,
		leftProfile:   p.children[0].statsInfo(),
		rightProfile:  p.children[1].statsInfo(),
		leftJoinKeys:  p.LeftJoinKeys,
		rightJoinKeys: p.RightJoinKeys,
		leftSchema:    p.children[0].Schema(),
		rightSchema:   p.children[1].Schema(),
	}
	numPairs := helper.estimate()
	// Cost of querying hash table is cheap actually, so we just compute the cost of
	// evaluating `OtherConditions` and joining row pairs.
	probeCost := numPairs * sessVars.CPUFactor
	// Cost of evaluating outer filter.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		// Input outer count for the above compution should be adjusted by selectionFactor.
		probeCost *= selectionFactor
		probeCost += probeCnt * sessVars.CPUFactor
	}
	probeCost /= float64(p.Concurrency)
	// Cost of additional concurrent goroutines.
	cpuCost += probeCost + float64(p.Concurrency+1)*sessVars.ConcurrencyFactor

	return cpuCost + memoryCost
}

func (p *PhysicalHashJoin) attach2Task(tasks ...task) task {
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = BuildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.GetCost(lTask.count(), rTask.count()),
	}
}

// GetCost computes cost of merge join operator itself.
func (p *PhysicalMergeJoin) GetCost(lCnt, rCnt float64) float64 {
	outerCnt := lCnt
	innerKeys := p.RightJoinKeys
	innerSchema := p.children[1].Schema()
	innerStats := p.children[1].statsInfo()
	if p.JoinType == RightOuterJoin {
		outerCnt = rCnt
		innerKeys = p.LeftJoinKeys
		innerSchema = p.children[0].Schema()
		innerStats = p.children[0].statsInfo()
	}
	helper := &fullJoinRowCountHelper{
		cartesian:     false,
		leftProfile:   p.children[0].statsInfo(),
		rightProfile:  p.children[1].statsInfo(),
		leftJoinKeys:  p.LeftJoinKeys,
		rightJoinKeys: p.RightJoinKeys,
		leftSchema:    p.children[0].Schema(),
		rightSchema:   p.children[1].Schema(),
	}
	numPairs := helper.estimate()
	sessVars := p.ctx.GetSessionVars()
	probeCost := numPairs * sessVars.CPUFactor
	// Cost of evaluating outer filters.
	var cpuCost float64
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		probeCost *= selectionFactor
		cpuCost += outerCnt * sessVars.CPUFactor
	}
	cpuCost += probeCost
	// For merge join, only one group of rows with same join key(not null) are cached,
	// we compute averge memory cost using estimated group size.
	NDV := getCardinality(innerKeys, innerSchema, innerStats)
	memoryCost := (innerStats.RowCount / NDV) * sessVars.MemoryFactor
	return cpuCost + memoryCost
}

func (p *PhysicalMergeJoin) attach2Task(tasks ...task) task {
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = BuildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.GetCost(lTask.count(), rTask.count()),
	}
}

// splitCopAvg2CountAndSum splits the cop avg function to count and sum.
// Now it's only used for TableReader.
func splitCopAvg2CountAndSum(p PhysicalPlan) {
	var baseAgg *basePhysicalAgg
	if agg, ok := p.(*PhysicalHashAgg); ok {
		baseAgg = &agg.basePhysicalAgg
	}
	if baseAgg == nil {
		return
	}

	schemaCursor := len(baseAgg.Schema().Columns) - len(baseAgg.GroupByItems)
	for i := len(baseAgg.AggFuncs) - 1; i >= 0; i-- {
		f := baseAgg.AggFuncs[i]
		schemaCursor--
		if f.Name == ast.AggFuncAvg {
			schemaCursor--
			sumAgg := *f
			sumAgg.Name = ast.AggFuncSum
			sumAgg.RetTp = baseAgg.Schema().Columns[schemaCursor+1].RetType
			cntAgg := *f
			cntAgg.Name = ast.AggFuncCount
			cntAgg.RetTp = baseAgg.Schema().Columns[schemaCursor].RetType
			cntAgg.RetTp.Flag = f.RetTp.Flag
			baseAgg.AggFuncs = append(baseAgg.AggFuncs[:i], append([]*aggregation.AggFuncDesc{&cntAgg, &sumAgg}, baseAgg.AggFuncs[i+1:]...)...)
		}
	}
}

// finishCopTask means we close the coprocessor task and create a root task.
func finishCopTask(ctx sessionctx.Context, task task) task {
	t, ok := task.(*copTask)
	if !ok {
		return task
	}
	sessVars := ctx.GetSessionVars()
	// copTasks are run in parallel, to make the estimated cost closer to execution time, we amortize
	// the cost to cop iterator workers. According to `CopClient::Send`, the concurrency
	// is Min(DistSQLScanConcurrency, numRegionsInvolvedInScan), since we cannot infer
	// the number of regions involved, we simply use DistSQLScanConcurrency.
	copIterWorkers := float64(t.plan().SCtx().GetSessionVars().DistSQLScanConcurrency)
	t.finishIndexPlan()
	// Network cost of transferring rows of table scan to TiDB.
	if t.tablePlan != nil {
		t.cst += t.count() * sessVars.NetworkFactor * t.tblColHists.GetAvgRowSize(t.tablePlan.Schema().Columns, false)
	}
	t.cst /= copIterWorkers
	newTask := &rootTask{
		cst: t.cst,
	}
	if t.indexPlan != nil && t.tablePlan != nil {
		p := PhysicalIndexLookUpReader{
			tablePlan:      t.tablePlan,
			indexPlan:      t.indexPlan,
			ExtraHandleCol: t.extraHandleCol,
		}.Init(ctx)
		p.stats = t.tablePlan.statsInfo()
		// Add cost of building table reader executors. Handles are extracted in batch style,
		// each handle is a range, the CPU cost of building copTasks should be:
		// (indexRows / batchSize) * batchSize * CPUFactor
		// Since we don't know the number of copTasks built, ignore these network cost now.
		indexRows := t.indexPlan.statsInfo().RowCount
		newTask.cst += indexRows * sessVars.CPUFactor
		// Add cost of worker goroutines in index lookup.
		numTblWorkers := float64(sessVars.IndexLookupConcurrency)
		newTask.cst += (numTblWorkers + 1) * sessVars.ConcurrencyFactor
		// When building table reader executor for each batch, we would sort the handles. CPU
		// cost of sort is:
		// CPUFactor * batchSize * Log2(batchSize) * (indexRows / batchSize)
		indexLookupSize := float64(sessVars.IndexLookupSize)
		batchSize := math.Min(indexLookupSize, indexRows)
		if batchSize > 2 {
			sortCPUCost := (indexRows * math.Log2(batchSize) * sessVars.CPUFactor) / numTblWorkers
			newTask.cst += sortCPUCost
		}
		// Also, we need to sort the retrieved rows if index lookup reader is expected to return
		// ordered results. Note that row count of these two sorts can be different, if there are
		// operators above table scan.
		tableRows := t.tablePlan.statsInfo().RowCount
		selectivity := tableRows / indexRows
		batchSize = math.Min(indexLookupSize*selectivity, tableRows)
		if t.keepOrder && batchSize > 2 {
			sortCPUCost := (tableRows * math.Log2(batchSize) * sessVars.CPUFactor) / numTblWorkers
			newTask.cst += sortCPUCost
		}
		if t.doubleReadNeedProj {
			schema := p.IndexPlans[0].(*PhysicalIndexScan).dataSourceSchema
			proj := PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.stats, nil)
			proj.SetSchema(schema)
			proj.SetChildren(p)
			newTask.p = proj
		} else {
			newTask.p = p
		}
	} else if t.indexPlan != nil {
		p := PhysicalIndexReader{indexPlan: t.indexPlan}.Init(ctx)
		p.stats = t.indexPlan.statsInfo()
		newTask.p = p
	} else {
		tp := t.tablePlan
		splitCopAvg2CountAndSum(tp)
		for len(tp.Children()) > 0 {
			tp = tp.Children()[0]
		}
		p := PhysicalTableReader{
			tablePlan: t.tablePlan,
		}.Init(ctx)
		p.stats = t.tablePlan.statsInfo()
		newTask.p = p
	}

	if len(t.rootTaskConds) > 0 {
		sel := PhysicalSelection{Conditions: t.rootTaskConds}.Init(ctx, newTask.p.statsInfo())
		sel.SetChildren(newTask.p)
		newTask.p = sel
	}

	return newTask
}

// rootTask is the final sink node of a plan graph. It should be a single goroutine on tidb.
type rootTask struct {
	p   PhysicalPlan
	cst float64
}

func (t *rootTask) copy() task {
	return &rootTask{
		p:   t.p,
		cst: t.cst,
	}
}

func (t *rootTask) count() float64 {
	return t.p.statsInfo().RowCount
}

func (t *rootTask) addCost(cst float64) {
	t.cst += cst
}

func (t *rootTask) cost() float64 {
	return t.cst
}

func (t *rootTask) plan() PhysicalPlan {
	return t.p
}

func (p *PhysicalLimit) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	if cop, ok := t.(*copTask); ok {
		// For double read which requires order being kept, the limit cannot be pushed down to the table side,
		// because handles would be reordered before being sent to table scan.
		if !cop.keepOrder || !cop.indexPlanFinished || cop.indexPlan == nil {
			// When limit is pushed down, we should remove its offset.
			newCount := p.Offset + p.Count
			childProfile := cop.plan().statsInfo()
			// Strictly speaking, for the row count of stats, we should multiply newCount with "regionNum",
			// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
			stats := deriveLimitStats(childProfile, float64(newCount))
			pushedDownLimit := PhysicalLimit{Count: newCount}.Init(p.ctx, stats)
			cop = attachPlan2Task(pushedDownLimit, cop).(*copTask)
		}
		t = finishCopTask(p.ctx, cop)
	}
	return attachPlan2Task(p, t)
}

// GetCost computes cost of TopN operator itself.
func (p *PhysicalTopN) GetCost(count float64, isRoot bool) float64 {
	heapSize := float64(p.Offset + p.Count)
	if heapSize < 2.0 {
		heapSize = 2.0
	}
	sessVars := p.ctx.GetSessionVars()
	// Ignore the cost of `doCompaction` in current implementation of `TopNExec`, since it is the
	// special side-effect of our Chunk format in TiDB layer, which may not exist in coprocessor's
	// implementation, or may be removed in the future if we change data format.
	// Note that we are using worst complexity to compute CPU cost, because it is simpler compared with
	// considering probabilities of average complexity, i.e, we may not need adjust heap for each input
	// row.
	var cpuCost float64
	if isRoot {
		cpuCost = count * math.Log2(heapSize) * sessVars.CPUFactor
	} else {
		cpuCost = count * math.Log2(heapSize) * sessVars.CopCPUFactor
	}
	memoryCost := heapSize * sessVars.MemoryFactor
	return cpuCost + memoryCost
}

// canPushDown checks if this topN can be pushed down. If each of the expression can be converted to pb, it can be pushed.
func (p *PhysicalTopN) canPushDown() bool {
	exprs := make([]expression.Expression, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		exprs = append(exprs, item.Expr)
	}
	_, _, remained := expression.ExpressionsToPB(p.ctx.GetSessionVars().StmtCtx, exprs, p.ctx.GetClient())
	return len(remained) == 0
}

func (p *PhysicalTopN) allColsFromSchema(schema *expression.Schema) bool {
	cols := make([]*expression.Column, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		cols = append(cols, expression.ExtractColumns(item.Expr)...)
	}
	return len(schema.ColumnsIndices(cols)) > 0
}

// GetCost computes the cost of in memory sort.
func (p *PhysicalSort) GetCost(count float64) float64 {
	if count < 2.0 {
		count = 2.0
	}
	sessVars := p.ctx.GetSessionVars()
	return count*math.Log2(count)*sessVars.CPUFactor + count*sessVars.MemoryFactor
}

func (p *PhysicalSort) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	t = attachPlan2Task(p, t)
	t.addCost(p.GetCost(t.count()))
	return t
}

func (p *NominalSort) attach2Task(tasks ...task) task {
	return tasks[0]
}

func (p *PhysicalTopN) getPushedDownTopN(childPlan PhysicalPlan) *PhysicalTopN {
	newByItems := make([]*ByItems, 0, len(p.ByItems))
	for _, expr := range p.ByItems {
		newByItems = append(newByItems, expr.Clone())
	}
	newCount := p.Offset + p.Count
	childProfile := childPlan.statsInfo()
	// Strictly speaking, for the row count of pushed down TopN, we should multiply newCount with "regionNum",
	// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
	stats := deriveLimitStats(childProfile, float64(newCount))
	topN := PhysicalTopN{
		ByItems: newByItems,
		Count:   newCount,
	}.Init(p.ctx, stats)
	topN.SetChildren(childPlan)
	return topN
}

func (p *PhysicalTopN) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	inputCount := t.count()
	if copTask, ok := t.(*copTask); ok && p.canPushDown() {
		// If all columns in topN are from index plan, we push it to index plan, otherwise we finish the index plan and
		// push it to table plan.
		var pushedDownTopN *PhysicalTopN
		if !copTask.indexPlanFinished && p.allColsFromSchema(copTask.indexPlan.Schema()) {
			pushedDownTopN = p.getPushedDownTopN(copTask.indexPlan)
			copTask.indexPlan = pushedDownTopN
		} else {
			copTask.finishIndexPlan()
			pushedDownTopN = p.getPushedDownTopN(copTask.tablePlan)
			copTask.tablePlan = pushedDownTopN
		}
		copTask.addCost(pushedDownTopN.GetCost(inputCount, false))
	}
	rootTask := finishCopTask(p.ctx, t)
	rootTask.addCost(p.GetCost(rootTask.count(), true))
	rootTask = attachPlan2Task(p, rootTask)
	return rootTask
}

// GetCost computes the cost of projection operator itself.
func (p *PhysicalProjection) GetCost(count float64) float64 {
	sessVars := p.ctx.GetSessionVars()
	cpuCost := count * sessVars.CPUFactor
	concurrency := float64(sessVars.ProjectionConcurrency)
	if concurrency <= 0 {
		return cpuCost
	}
	cpuCost /= concurrency
	concurrencyCost := (1 + concurrency) * sessVars.ConcurrencyFactor
	return cpuCost + concurrencyCost
}

func (p *PhysicalProjection) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	if copTask, ok := t.(*copTask); ok {
		// TODO: support projection push down.
		t = finishCopTask(p.ctx, copTask)
	}
	t = attachPlan2Task(p, t)
	t.addCost(p.GetCost(t.count()))
	return t
}

func (sel *PhysicalSelection) attach2Task(tasks ...task) task {
	sessVars := sel.ctx.GetSessionVars()
	t := finishCopTask(sel.ctx, tasks[0].copy())
	t.addCost(t.count() * sessVars.CPUFactor)
	t = attachPlan2Task(sel, t)
	return t
}

// CheckAggCanPushCop checks whether the aggFuncs with groupByItems can
// be pushed down to coprocessor.
func CheckAggCanPushCop(sctx sessionctx.Context, aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression) bool {
	sc := sctx.GetSessionVars().StmtCtx
	client := sctx.GetClient()
	for _, aggFunc := range aggFuncs {
		pb := aggregation.AggFuncToPBExpr(sc, client, aggFunc)
		if pb == nil {
			return false
		}
	}
	_, _, remained := expression.ExpressionsToPB(sc, groupByItems, client)
	if len(remained) > 0 {
		return false
	}
	return true
}

// BuildFinalModeAggregation splits either LogicalAggregation or PhysicalAggregation to finalAgg and partial1Agg,
// returns the body of finalAgg and the schema of partialAgg.
func BuildFinalModeAggregation(
	sctx sessionctx.Context,
	aggFuncs []*aggregation.AggFuncDesc,
	groupByItems []expression.Expression,
	finalSchema *expression.Schema) (finalAggFuncs []*aggregation.AggFuncDesc, finalGbyItems []expression.Expression, partialSchema *expression.Schema) {
	// TODO: Refactor the way of constructing aggregation functions.
	partialSchema = expression.NewSchema()
	partialCursor := 0
	finalAggFuncs = make([]*aggregation.AggFuncDesc, len(aggFuncs))
	for i, aggFunc := range aggFuncs {
		finalAggFunc := &aggregation.AggFuncDesc{}
		finalAggFunc.Name = aggFunc.Name
		args := make([]expression.Expression, 0, len(aggFunc.Args))
		if aggregation.NeedCount(finalAggFunc.Name) {
			ft := types.NewFieldType(mysql.TypeLonglong)
			ft.Flen, ft.Charset, ft.Collate = 21, charset.CharsetBin, charset.CollationBin
			partialSchema.Append(&expression.Column{
				UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  ft,
			})
			args = append(args, partialSchema.Columns[partialCursor])
			partialCursor++
		}
		if aggregation.NeedValue(finalAggFunc.Name) {
			partialSchema.Append(&expression.Column{
				UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  finalSchema.Columns[i].GetType(),
			})
			args = append(args, partialSchema.Columns[partialCursor])
			partialCursor++
		}
		finalAggFunc.Args = args
		finalAggFunc.Mode = aggregation.FinalMode
		finalAggFunc.RetTp = aggFunc.RetTp
		finalAggFuncs[i] = finalAggFunc
	}
	// add group by columns
	finalGbyItems = make([]expression.Expression, 0, len(groupByItems))
	for _, gbyExpr := range groupByItems {
		var gbyCol *expression.Column
		if col, ok := gbyExpr.(*expression.Column); ok {
			gbyCol = col
		} else {
			gbyCol = &expression.Column{
				UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  gbyExpr.GetType(),
			}
		}
		partialSchema.Append(gbyCol)
		finalGbyItems = append(finalGbyItems, gbyCol)
	}
	return
}

func (p *basePhysicalAgg) newPartialAggregate() (partial, final PhysicalPlan) {
	// Check if this aggregation can push down.
	if !CheckAggCanPushCop(p.ctx, p.AggFuncs, p.GroupByItems) {
		return nil, p.self
	}
	finalAggFuncs, finalGbyItems, partialSchema := BuildFinalModeAggregation(p.ctx, p.AggFuncs, p.GroupByItems, p.schema)
	// Remove unnecessary FirstRow.
	p.AggFuncs = RemoveUnnecessaryFirstRow(p.ctx, finalAggFuncs, finalGbyItems, p.AggFuncs, p.GroupByItems, partialSchema)
	finalSchema := p.schema
	p.schema = partialSchema
	partialAgg := p.self
	// Create physical "final" aggregation.
	finalAgg := basePhysicalAgg{
		AggFuncs:     finalAggFuncs,
		GroupByItems: finalGbyItems,
	}.initForHash(p.ctx, p.stats)
	finalAgg.schema = finalSchema
	return partialAgg, finalAgg
}

// RemoveUnnecessaryFirstRow removes unnecessary FirstRow of the aggregation. This function can be
// used for both LogicalAggregation and PhysicalAggregation.
// When the select column is same with the group by key, the column can be removed and gets value from the group by key.
// e.g
// select a, count(b) from t group by a;
// The schema is [firstrow(a), count(b), a]. The column firstrow(a) is unnecessary.
// Can optimize the schema to [count(b), a] , and change the index to get value.
func RemoveUnnecessaryFirstRow(
	sctx sessionctx.Context,
	finalAggFuncs []*aggregation.AggFuncDesc,
	finalGbyItems []expression.Expression,
	partialAggFuncs []*aggregation.AggFuncDesc,
	partialGbyItems []expression.Expression,
	partialSchema *expression.Schema) []*aggregation.AggFuncDesc {
	partialCursor := 0
	newAggFuncs := make([]*aggregation.AggFuncDesc, 0, len(partialAggFuncs))
	for i, aggFunc := range partialAggFuncs {
		if aggFunc.Name == ast.AggFuncFirstRow {
			canOptimize := false
			for j, gbyExpr := range partialGbyItems {
				if gbyExpr.Equal(sctx, aggFunc.Args[0]) {
					canOptimize = true
					finalAggFuncs[i].Args[0] = finalGbyItems[j]
					break
				}
			}
			if canOptimize {
				partialSchema.Columns = append(partialSchema.Columns[:partialCursor], partialSchema.Columns[partialCursor+1:]...)
				continue
			}
		}
		if aggregation.NeedCount(aggFunc.Name) {
			partialCursor++
		}
		if aggregation.NeedValue(aggFunc.Name) {
			partialCursor++
		}
		newAggFuncs = append(newAggFuncs, aggFunc)
	}
	return newAggFuncs
}

// cpuCostDivisor computes the concurrency to which we would amortize CPU cost
// for hash aggregation.
func (p *PhysicalHashAgg) cpuCostDivisor() (float64, float64) {
	sessionVars := p.ctx.GetSessionVars()
	finalCon, partialCon := sessionVars.HashAggFinalConcurrency, sessionVars.HashAggPartialConcurrency
	// According to `ValidateSetSystemVar`, `finalCon` and `partialCon` cannot be less than or equal to 0.
	if finalCon == 1 && partialCon == 1 {
		return 0, 0
	}
	// It is tricky to decide which concurrency we should use to amortize CPU cost. Since cost of hash
	// aggregation is tend to be under-estimated as explained in `attach2Task`, we choose the smaller
	// concurrecy to make some compensation.
	return math.Min(float64(finalCon), float64(partialCon)), float64(finalCon + partialCon)
}

func (p *PhysicalHashAgg) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	inputRows := t.count()
	if cop, ok := t.(*copTask); ok {
		partialAgg, finalAgg := p.newPartialAggregate()
		if partialAgg != nil {
			if cop.tablePlan != nil {
				cop.finishIndexPlan()
				partialAgg.SetChildren(cop.tablePlan)
				cop.tablePlan = partialAgg
			} else {
				partialAgg.SetChildren(cop.indexPlan)
				cop.indexPlan = partialAgg
			}
			cop.addCost(p.GetCost(inputRows, false))
		}
		// In `newPartialAggregate`, we are using stats of final aggregation as stats
		// of `partialAgg`, so the network cost of transferring result rows of `partialAgg`
		// to TiDB is normally under-estimated for hash aggregation, since the group-by
		// column may be independent of the column used for region distribution, so a closer
		// estimation of network cost for hash aggregation may multiply the number of
		// regions involved in the `partialAgg`, which is unknown however.
		t = finishCopTask(p.ctx, cop)
		inputRows = t.count()
		attachPlan2Task(finalAgg, t)
	} else {
		attachPlan2Task(p, t)
	}
	// We may have 3-phase hash aggregation actually, strictly speaking, we'd better
	// calculate cost of each phase and sum the results up, but in fact we don't have
	// region level table stats, and the concurrency of the `partialAgg`,
	// i.e, max(number_of_regions, DistSQLScanConcurrency) is unknown either, so it is hard
	// to compute costs separately. We ignore region level parallelism for both hash
	// aggregation and stream aggregation when calculating cost, though this would lead to inaccuracy,
	// hopefully this inaccuracy would be imposed on both aggregation implementations,
	// so they are still comparable horizontally.
	// Also, we use the stats of `partialAgg` as the input of cost computing for TiDB layer
	// hash aggregation, it would cause under-estimation as the reason mentioned in comment above.
	// To make it simple, we also treat 2-phase parallel hash aggregation in TiDB layer as
	// 1-phase when computing cost.
	t.addCost(p.GetCost(inputRows, true))
	return t
}

// GetCost computes the cost of hash aggregation considering CPU/memory.
func (p *PhysicalHashAgg) GetCost(inputRows float64, isRoot bool) float64 {
	cardinality := p.statsInfo().RowCount
	aggFuncFactor := p.getAggFuncCostFactor()
	var cpuCost float64
	sessVars := p.ctx.GetSessionVars()
	if isRoot {
		cpuCost = inputRows * sessVars.CPUFactor * aggFuncFactor
		divisor, con := p.cpuCostDivisor()
		if divisor > 0 {
			cpuCost /= divisor
			// Cost of additional goroutines.
			cpuCost += (con + 1) * sessVars.ConcurrencyFactor
		}
	} else {
		cpuCost = inputRows * sessVars.CopCPUFactor * aggFuncFactor
	}
	memoryCost := cardinality * sessVars.MemoryFactor * float64(len(p.AggFuncs))
	return cpuCost + memoryCost
}
