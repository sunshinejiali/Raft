package raftstore

import (
	"bytes"
	"fmt"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

type MsgApplyProposal struct {
	Id       uint64
	RegionId uint64
	Props    []*proposal
}

type MsgApplyCommitted struct {
	regionId uint64
	term     uint64
	entries  []eraftpb.Entry
}

type proposal struct {
	isConfChange bool
	index        uint64
	term         uint64
	cb           *message.Callback
}

type MsgApplyRefresh struct {
	id     uint64
	term   uint64
	region *metapb.Region
}

type MsgApplyRes struct {
	regionID     uint64
	execResults  []execResult
	sizeDiffHint uint64
}

type execResult = interface{}

type execResultChangePeer struct {
	confChange *eraftpb.ConfChange
	peer       *metapb.Peer
	region     *metapb.Region
}

type execResultCompactLog struct {
	truncatedIndex uint64
	firstIndex     uint64
}

type execResultSplitRegion struct {
	regions []*metapb.Region
	derived *metapb.Region
}

/// Calls the callback of `cmd` when the Region is removed.
func notifyRegionRemoved(regionID, peerID uint64, cmd pendingCmd) {
	log.Debug(fmt.Sprintf("region %d is removed, peerID %d, index %d, term %d", regionID, peerID, cmd.index, cmd.term))
	cmd.cb.Done(ErrRespRegionNotFound(regionID))
}

/// Calls the callback of `cmd` when it can not be processed further.
func notifyStaleCommand(regionID, peerID, term uint64, cmd pendingCmd) {
	log.Info(fmt.Sprintf("command is stale, skip. regionID %d, peerID %d, index %d, term %d",
		regionID, peerID, cmd.index, cmd.term))
	cmd.cb.Done(ErrRespStaleCommand(term))
}

/// The applier of a Region which is responsible for handling committed
/// raft log entries of a Region.
///
/// `Apply` is a term of Raft, which means executing the actual commands.
/// In Raft, once some log entries are committed, for every peer of the Raft
/// group will apply the logs one by one. For write commands, it does write or
/// delete to local engine; for admin commands, it does some meta change of the
/// Raft group.
///
/// The raft worker receives all the apply tasks of different Regions
/// located at this store, and it will get the corresponding applier to
/// handle the apply worker.Task to make the code logic more clear.
type applier struct {
	id     uint64
	term   uint64
	region *metapb.Region
	tag    string

	/// Set to true when removing itself because of `ConfChangeType::RemoveNode`, and then
	/// any following committed logs in same Ready should be applied failed.
	pendingRemove bool

	/// The commands waiting to be committed and applied
	pendingCmds pendingCmdQueue

	/// We writes apply_state to KV DB, in one write batch together with kv data.
	///
	/// If we write it to Raft DB, apply_state and kv data (Put, Delete) are in
	/// separate WAL file. When power failure, for current raft log, apply_index may synced
	/// to file, but KV data may not synced to file, so we will lose data.
	applyState rspb.RaftApplyState

	sizeDiffHint uint64
}

func newApplierFromPeer(peer *peer) *applier {
	return &applier{
		tag:    fmt.Sprintf("[region %d] %d", peer.Region().GetId(), peer.PeerId()),
		id:     peer.PeerId(),
		term:   peer.Term(),
		region: peer.Region(),
	}
}

func (a *applier) destroy() {
	log.Info(fmt.Sprintf("%s remove applier", a.tag))
	for _, cmd := range a.pendingCmds.normals {
		notifyRegionRemoved(a.region.Id, a.id, cmd)
	}
	a.pendingCmds.normals = nil
	if cmd := a.pendingCmds.takeConfChange(); cmd != nil {
		notifyRegionRemoved(a.region.Id, a.id, *cmd)
	}
}

func (a *applier) handleTask(aCtx *applyContext, msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeApplyProposal:
		a.handleProposal(msg.Data.(*MsgApplyProposal))
	case message.MsgTypeApplyCommitted:
		a.handleApply(aCtx, msg.Data.(*MsgApplyCommitted))
	case message.MsgTypeApplyRefresh:
		a.handleRefresh(msg.Data.(*MsgApplyRefresh))
	}
}

// when a snapshot, need to refresh the apply state
/// Handles peer registration. When a peer is created, it will register an applier.
func (a *applier) handleRefresh(reg *MsgApplyRefresh) {
	log.Info(fmt.Sprintf("%s refresh the applier, term %d", a.tag, reg.term))
	y.Assert(a.id == reg.id)
	a.term = reg.term
	for _, cmd := range a.pendingCmds.normals {
		notifyStaleCommand(a.region.Id, a.id, a.term, cmd)
	}
	a.pendingCmds.normals = a.pendingCmds.normals[:0]
	if cmd := a.pendingCmds.takeConfChange(); cmd != nil {
		notifyStaleCommand(a.region.Id, a.id, a.term, *cmd)
	}
	*a = applier{
		tag:    fmt.Sprintf("[region %d] %d", reg.region.Id, reg.id),
		id:     reg.id,
		term:   reg.term,
		region: reg.region,
	}
}

/// Handles apply tasks, and uses the applier to handle the committed entries.
func (a *applier) handleApply(aCtx *applyContext, apply *MsgApplyCommitted) {
	if len(apply.entries) == 0 || a.pendingRemove {
		return
	}
	a.term = apply.term
	a.handleRaftCommittedEntries(aCtx, apply.entries)
	apply.entries = apply.entries[:0]
}

/// Handles proposals, and appends the commands to the applier.
func (a *applier) handleProposal(regionProposal *MsgApplyProposal) {
	regionID, peerID := a.region.Id, a.id
	y.Assert(a.id == regionProposal.Id)
	if a.pendingRemove {
		for _, p := range regionProposal.Props {
			cmd := pendingCmd{index: p.index, term: p.term, cb: p.cb}
			notifyStaleCommand(regionID, peerID, a.term, cmd)
		}
		return
	}
	for _, p := range regionProposal.Props {
		cmd := pendingCmd{index: p.index, term: p.term, cb: p.cb}
		if p.isConfChange {
			if confCmd := a.pendingCmds.takeConfChange(); confCmd != nil {
				// if it loses leadership before conf change is replicated, there may be
				// a stale pending conf change before next conf change is applied. If it
				// becomes leader again with the stale pending conf change, will enter
				// this block, so we notify leadership may have been changed.
				notifyStaleCommand(regionID, peerID, a.term, *confCmd)
			}
			a.pendingCmds.setConfChange(&cmd)
		} else {
			a.pendingCmds.appendNormal(cmd)
		}
	}
}

type pendingCmd struct {
	index uint64
	term  uint64
	cb    *message.Callback
}

type pendingCmdQueue struct {
	normals    []pendingCmd
	confChange *pendingCmd
}

func (q *pendingCmdQueue) popNormal(term uint64) *pendingCmd {
	if len(q.normals) == 0 {
		return nil
	}
	cmd := &q.normals[0]
	if cmd.term > term {
		return nil
	}
	q.normals = q.normals[1:]
	return cmd
}

func (q *pendingCmdQueue) appendNormal(cmd pendingCmd) {
	q.normals = append(q.normals, cmd)
}

func (q *pendingCmdQueue) takeConfChange() *pendingCmd {
	// conf change will not be affected when changing between follower and leader,
	// so there is no need to check term.
	cmd := q.confChange
	q.confChange = nil
	return cmd
}

// TODO: seems we don't need to separate conf change from normal entries.
func (q *pendingCmdQueue) setConfChange(cmd *pendingCmd) {
	q.confChange = cmd
}

type applyResultType int

const (
	applyResultTypeNone       applyResultType = 0
	applyResultTypeExecResult applyResultType = 1
)

type applyResult struct {
	tp   applyResultType
	data interface{}
}

type applyExecContext struct {
	index      uint64
	term       uint64
	applyState rspb.RaftApplyState
}

type applyCallback struct {
	region *metapb.Region
	cbs    []*message.Callback
}

func (c *applyCallback) invokeAll() {
	for _, cb := range c.cbs {
		if cb != nil {
			cb.Done(nil)
		}
	}
}

func (c *applyCallback) push(cb *message.Callback, resp *raft_cmdpb.RaftCmdResponse, txn *badger.Txn) {
	if cb != nil {
		cb.Resp = resp
		cb.Txn = txn
	}
	c.cbs = append(c.cbs, cb)
}

type applyContext struct {
	tag              string
	notifier         chan<- message.Msg
	engines          *engine_util.Engines
	cbs              []applyCallback
	applyTaskResList []*MsgApplyRes
	execCtx          *applyExecContext
	wb               *engine_util.WriteBatch
	lastAppliedIndex uint64
	committedCount   int
}

func newApplyContext(tag string, engines *engine_util.Engines,
	notifier chan<- message.Msg, cfg *config.Config) *applyContext {
	return &applyContext{
		tag:      tag,
		engines:  engines,
		notifier: notifier,
		wb:       new(engine_util.WriteBatch),
	}
}

/// Prepares for applying entries for `applier`.
///
/// A general apply progress for an applier is:
/// `prepare_for` -> `commit` [-> `commit` ...] -> `finish_for`.
/// After all appliers are handled, `write_to_db` method should be called.
func (ac *applyContext) prepareFor(d *applier) {
	if ac.wb == nil {
		ac.wb = new(engine_util.WriteBatch)
	}
	ac.cbs = append(ac.cbs, applyCallback{region: d.region})
	applyState, _ := meta.GetApplyState(ac.engines.Kv, d.region.GetId())
	d.applyState = *applyState
	ac.lastAppliedIndex = d.applyState.AppliedIndex
}

/// Commits all changes have done for applier. `persistent` indicates whether
/// write the changes into rocksdb.
///
/// This call is valid only when it's between a `prepare_for` and `finish_for`.
func (ac *applyContext) commit(d *applier) {
	if ac.lastAppliedIndex < d.applyState.AppliedIndex {
		d.writeApplyState(ac.wb)
	}
	// last_applied_index doesn't need to be updated, set persistent to true will
	// force it call `prepare_for` automatically.
	ac.commitOpt(d, true)
}

func (ac *applyContext) commitOpt(d *applier, persistent bool) {
	if persistent {
		ac.writeToDB()
		ac.prepareFor(d)
	}
}

/// Writes all the changes into badger.
func (ac *applyContext) writeToDB() {
	if err := ac.wb.WriteToDB(ac.engines.Kv); err != nil {
		panic(err)
	}
	ac.wb.Reset()
	for _, cb := range ac.cbs {
		cb.invokeAll()
	}
	ac.cbs = ac.cbs[:0]
}

/// Finishes `Apply`s for the applier.
func (ac *applyContext) finishFor(d *applier, results []execResult) {
	if !d.pendingRemove {
		d.writeApplyState(ac.wb)
	}
	ac.commitOpt(d, false)
	res := &MsgApplyRes{
		regionID:    d.region.Id,
		execResults: results,
	}
	ac.applyTaskResList = append(ac.applyTaskResList, res)
}

func (ac *applyContext) flush() {
	// Write to engine
	ac.writeToDB()
	if len(ac.applyTaskResList) > 0 {
		for _, res := range ac.applyTaskResList {
			ac.notifier <- message.NewPeerMsg(message.MsgTypeApplyRes, res.regionID, res)
		}
		ac.applyTaskResList = ac.applyTaskResList[:0]
	}
	ac.committedCount = 0
}

/// Handles all the committed_entries, namely, applies the committed entries.
func (a *applier) handleRaftCommittedEntries(aCtx *applyContext, committedEntries []eraftpb.Entry) {
	if len(committedEntries) == 0 {
		return
	}
	aCtx.prepareFor(a)
	aCtx.committedCount += len(committedEntries)
	// If we send multiple ConfChange commands, only first one will be proposed correctly,
	// others will be saved as a normal entry with no data, so we must re-propose these
	// commands again.
	aCtx.committedCount += len(committedEntries)
	var results []execResult
	for i := range committedEntries {
		entry := &committedEntries[i]
		if a.pendingRemove {
			// This peer is about to be destroyed, skip everything.
			break
		}
		expectedIndex := a.applyState.AppliedIndex + 1
		if expectedIndex != entry.Index {
			panic(fmt.Sprintf("%s expect index %d, but got %d", a.tag, expectedIndex, entry.Index))
		}
		var res applyResult
		switch entry.EntryType {
		case eraftpb.EntryType_EntryNormal:
			res = a.handleRaftEntryNormal(aCtx, entry)
		case eraftpb.EntryType_EntryConfChange:
			res = a.handleRaftEntryConfChange(aCtx, entry)
		}
		switch res.tp {
		case applyResultTypeNone:
		case applyResultTypeExecResult:
			results = append(results, res.data)
		}
		aCtx.commit(a)
	}
	aCtx.finishFor(a, results)
}

func (a *applier) writeApplyState(wb *engine_util.WriteBatch) {
	wb.SetMeta(meta.ApplyStateKey(a.region.Id), &a.applyState)
}

func (a *applier) handleRaftEntryNormal(aCtx *applyContext, entry *eraftpb.Entry) applyResult {
	index := entry.Index
	term := entry.Term
	if len(entry.Data) > 0 {
		cmd := new(raft_cmdpb.RaftCmdRequest)
		err := cmd.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}
		return a.processRaftCmd(aCtx, index, term, cmd)
	}

	// when a peer become leader, it will send an empty entry.
	a.applyState.AppliedIndex = index
	y.Assert(term > 0)
	for {
		cmd := a.pendingCmds.popNormal(term - 1)
		if cmd == nil {
			break
		}
		// apparently, all the callbacks whose term is less than entry's term are stale.
		aCtx.cbs[len(aCtx.cbs)-1].push(cmd.cb, ErrRespStaleCommand(term), nil)
	}
	return applyResult{}
}

func (a *applier) handleRaftEntryConfChange(aCtx *applyContext, entry *eraftpb.Entry) applyResult {
	index := entry.Index
	term := entry.Term
	confChange := new(eraftpb.ConfChange)
	if err := confChange.Unmarshal(entry.Data); err != nil {
		panic(err)
	}
	cmd := new(raft_cmdpb.RaftCmdRequest)
	if err := cmd.Unmarshal(confChange.Context); err != nil {
		panic(err)
	}
	result := a.processRaftCmd(aCtx, index, term, cmd)
	switch result.tp {
	case applyResultTypeNone:
		// If failed, tell Raft that the `ConfChange` was aborted.
		return applyResult{tp: applyResultTypeExecResult, data: &execResultChangePeer{
			confChange: new(eraftpb.ConfChange),
		}}
	case applyResultTypeExecResult:
		cp := result.data.(*execResultChangePeer)
		cp.confChange = confChange
		return applyResult{tp: applyResultTypeExecResult, data: result.data}
	default:
		panic("unreachable")
	}
}

func (a *applier) findCallback(index, term uint64, isConfChange bool) *message.Callback {
	regionID := a.region.Id
	peerID := a.id
	if isConfChange {
		cmd := a.pendingCmds.takeConfChange()
		if cmd == nil {
			return nil
		}
		if cmd.index == index && cmd.term == term {
			return cmd.cb
		}
		notifyStaleCommand(regionID, peerID, term, *cmd)
		return nil
	}
	for {
		head := a.pendingCmds.popNormal(term)
		if head == nil {
			break
		}
		if head.index == index && head.term == term {
			return head.cb
		}
		// Because of the lack of original RaftCmdRequest, we skip calling
		// coprocessor here.
		notifyStaleCommand(regionID, peerID, term, *head)
	}
	return nil
}

func (a *applier) processRaftCmd(aCtx *applyContext, index, term uint64, cmd *raft_cmdpb.RaftCmdRequest) applyResult {
	if index == 0 {
		panic(fmt.Sprintf("%s process raft cmd need a none zero index", a.tag))
	}
	isConfChange := GetChangePeerCmd(cmd) != nil
	resp, txn, result := a.applyRaftCmd(aCtx, index, term, cmd)
	log.Debug(fmt.Sprintf("applied command. region_id %d, peer_id %d, index %d", a.region.Id, a.id, index))

	// TODO: if we have exec_result, maybe we should return this callback too. Outer
	// store will call it after handing exec result.
	BindRespTerm(resp, term)
	cmdCB := a.findCallback(index, term, isConfChange)
	aCtx.cbs[len(aCtx.cbs)-1].push(cmdCB, resp, txn)
	return result
}

/// Applies raft command.
///
/// An apply operation can fail in the following situations:
///   1. it encounters an error that will occur on all stores, it can continue
/// applying next entry safely, like epoch not match for example;
///   2. it encounters an error that may not occur on all stores, in this case
/// we should try to apply the entry again or panic. Considering that this
/// usually due to disk operation fail, which is rare, so just panic is ok.
func (a *applier) applyRaftCmd(aCtx *applyContext, index, term uint64,
	req *raft_cmdpb.RaftCmdRequest) (*raft_cmdpb.RaftCmdResponse, *badger.Txn, applyResult) {
	// if pending remove, apply should be aborted already.
	y.Assert(!a.pendingRemove)

	aCtx.execCtx = a.newCtx(aCtx.engines, index, term)
	aCtx.wb.SetSafePoint()
	resp, txn, applyResult, err := a.execRaftCmd(aCtx, req)
	if err != nil {
		// clear dirty values.
		aCtx.wb.RollbackToSafePoint()
		if _, ok := err.(*util.ErrEpochNotMatch); ok {
			log.Debug(fmt.Sprintf("epoch not match region_id %d, peer_id %d, err %v", a.region.Id, a.id, err))
		} else {
			log.Error(fmt.Sprintf("execute raft command region_id %d, peer_id %d, err %v", a.region.Id, a.id, err))
		}
		if txn != nil {
			txn.Discard()
			txn = nil
		}
		resp = ErrResp(err)
		applyResult.tp = applyResultTypeNone
	}
	a.applyState = aCtx.execCtx.applyState
	aCtx.execCtx = nil
	a.applyState.AppliedIndex = index

	if applyResult.tp == applyResultTypeExecResult {
		switch x := applyResult.data.(type) {
		case *execResultChangePeer:
			a.region = x.region
		case *execResultSplitRegion:
			a.region = x.derived
		default:
		}
	}
	return resp, txn, applyResult
}

func (a *applier) newCtx(engines *engine_util.Engines, index, term uint64) *applyExecContext {
	return &applyExecContext{
		index:      index,
		term:       term,
		applyState: a.applyState,
	}
}

// Only errors that will also occur on all other stores should be returned.
func (a *applier) execRaftCmd(aCtx *applyContext, req *raft_cmdpb.RaftCmdRequest) (
	resp *raft_cmdpb.RaftCmdResponse, txn *badger.Txn, result applyResult, err error) {
	// Include region for epoch not match after merge may cause key not in range.
	err = util.CheckRegionEpoch(req, a.region, false)
	if err != nil {
		return
	}
	if req.AdminRequest != nil {
		return a.execAdminCmd(aCtx, req)
	}
	return a.execNormalCmd(aCtx, req)
}

func (a *applier) execAdminCmd(aCtx *applyContext, req *raft_cmdpb.RaftCmdRequest) (
	resp *raft_cmdpb.RaftCmdResponse, txn *badger.Txn, result applyResult, err error) {
	adminReq := req.AdminRequest
	cmdType := adminReq.CmdType
	if cmdType != raft_cmdpb.AdminCmdType_CompactLog {
		log.Info(fmt.Sprintf("%s execute admin command. term %d, index %d, command %s",
			a.tag, aCtx.execCtx.term, aCtx.execCtx.index, adminReq))
	}
	var adminResp *raft_cmdpb.AdminResponse
	switch cmdType {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		adminResp, result, err = a.execChangePeer(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_Split:
		adminResp, result, err = a.execSplit(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_CompactLog:
		adminResp, result, err = a.execCompactLog(aCtx, adminReq)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		err = errors.New("transfer leader won't execute")
	case raft_cmdpb.AdminCmdType_InvalidAdmin:
		err = errors.New("unsupported command type")
	}
	if err != nil {
		return
	}
	adminResp.CmdType = cmdType
	resp = newCmdResp()
	resp.AdminResponse = adminResp
	return
}

func (a *applier) execNormalCmd(aCtx *applyContext, req *raft_cmdpb.RaftCmdRequest) (
	resp *raft_cmdpb.RaftCmdResponse, txn *badger.Txn, result applyResult, err error) {
	requests := req.GetRequests()
	resps := make([]*raft_cmdpb.Response, 0, len(requests))
	hasWrite, hasRead := false, false
	for _, req := range requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			var r *raft_cmdpb.Response
			r, err = a.handlePut(aCtx, req.GetPut())
			resps = append(resps, r)
			hasWrite = true
		case raft_cmdpb.CmdType_Delete:
			var r *raft_cmdpb.Response
			r, err = a.handleDelete(aCtx, req.GetDelete())
			resps = append(resps, r)
			hasWrite = true
		case raft_cmdpb.CmdType_Get:
			var r *raft_cmdpb.Response
			r, err = a.handleGet(aCtx, req.GetGet())
			resps = append(resps, r)
			hasRead = true
		case raft_cmdpb.CmdType_Snap:
			resps = append(resps, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    &raft_cmdpb.SnapResponse{Region: a.region},
			})
			txn = aCtx.engines.Kv.NewTransaction(false)
			hasRead = true
		default:
			log.Fatal(fmt.Sprintf("invalid cmd type=%v", req.CmdType))
		}
	}
	if hasWrite && hasRead {
		panic("mixed write and read in one request")
	}
	resp = newCmdResp()
	resp.Responses = resps
	return
}

func (a *applier) handlePut(aCtx *applyContext, req *raft_cmdpb.PutRequest) (*raft_cmdpb.Response, error) {
	key, value := req.GetKey(), req.GetValue()
	if err := util.CheckKeyInRegion(key, a.region); err != nil {
		return nil, err
	}

	if cf := req.GetCf(); len(cf) != 0 {
		aCtx.wb.SetCF(cf, key, value)
	} else {
		aCtx.wb.SetCF(engine_util.CfDefault, key, value)
	}
	return &raft_cmdpb.Response{
		CmdType: raft_cmdpb.CmdType_Put,
	}, nil
}

func (a *applier) handleDelete(aCtx *applyContext, req *raft_cmdpb.DeleteRequest) (*raft_cmdpb.Response, error) {
	key := req.GetKey()
	if err := util.CheckKeyInRegion(key, a.region); err != nil {
		return nil, err
	}

	if cf := req.GetCf(); len(cf) != 0 {
		aCtx.wb.DeleteCF(cf, key)
	} else {
		aCtx.wb.DeleteCF(engine_util.CfDefault, key)
	}
	return &raft_cmdpb.Response{
		CmdType: raft_cmdpb.CmdType_Delete,
	}, nil
}

func (a *applier) handleGet(aCtx *applyContext, req *raft_cmdpb.GetRequest) (*raft_cmdpb.Response, error) {
	key := req.GetKey()
	if err := util.CheckKeyInRegion(key, a.region); err != nil {
		return nil, err
	}
	var val []byte
	var err error
	if cf := req.GetCf(); len(cf) != 0 {
		val, err = engine_util.GetCF(aCtx.engines.Kv, cf, key)
	} else {
		val, err = engine_util.GetCF(aCtx.engines.Kv, engine_util.CfDefault, key)
	}
	if err == badger.ErrKeyNotFound {
		err = nil
		val = nil
	}

	return &raft_cmdpb.Response{
		CmdType: raft_cmdpb.CmdType_Get,
		Get:     &raft_cmdpb.GetResponse{Value: val},
	}, err
}

func (a *applier) execChangePeer(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	request := req.ChangePeer
	peer := request.Peer
	storeID := peer.StoreId
	changeType := request.ChangeType
	region := new(metapb.Region)
	err = util.CloneMsg(a.region, region)
	if err != nil {
		return
	}
	log.Info(fmt.Sprintf("%s exec ConfChange, peer_id %d, type %s, epoch %s",
		a.tag, peer.Id, changeType, region.RegionEpoch))

	// TODO: we should need more check, like peer validation, duplicated id, etc.
	region.RegionEpoch.ConfVer++

	switch changeType {
	case eraftpb.ConfChangeType_AddNode:
		if p := util.FindPeer(region, storeID); p != nil {
			errMsg := fmt.Sprintf("%s can't add duplicated peer, peer %s, region %s",
				a.tag, p, a.region)
			log.Error(errMsg)
			err = errors.New(errMsg)
			return
		}
		region.Peers = append(region.Peers, peer)
		log.Info(fmt.Sprintf("%s add peer successfully, peer %s, region %s", a.tag, peer, a.region))
	case eraftpb.ConfChangeType_RemoveNode:
		if p := util.RemovePeer(region, storeID); p != nil {
			if !util.PeerEqual(p, peer) {
				errMsg := fmt.Sprintf("%s ignore remove unmatched peer, expected_peer %s, got_peer %s",
					a.tag, peer, p)
				log.Error(errMsg)
				err = errors.New(errMsg)
				return
			}
			if a.id == peer.Id {
				// Remove ourself, we will destroy all region data later.
				// So we need not to apply following logs.
				a.pendingRemove = true
			}
		} else {
			errMsg := fmt.Sprintf("%s removing missing peers, peer %s, region %s",
				a.tag, peer, a.region)
			log.Error(errMsg)
			err = errors.New(errMsg)
			return
		}
		log.Info(fmt.Sprintf("%s remove peer successfully, peer %s, region %s", a.tag, peer, a.region))
	}
	state := rspb.PeerState_Normal
	if a.pendingRemove {
		state = rspb.PeerState_Tombstone
	}
	meta.WriteRegionState(aCtx.wb, region, state)
	resp = &raft_cmdpb.AdminResponse{
		ChangePeer: &raft_cmdpb.ChangePeerResponse{
			Region: region,
		},
	}
	result = applyResult{
		tp: applyResultTypeExecResult,
		data: &execResultChangePeer{
			confChange: new(eraftpb.ConfChange),
			region:     region,
			peer:       peer,
		},
	}
	return
}

func (a *applier) execSplit(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	splitReq := req.Split
	derived := new(metapb.Region)
	if err := util.CloneMsg(a.region, derived); err != nil {
		panic(err)
	}

	newRegionCnt := 1
	regions := make([]*metapb.Region, 0, newRegionCnt+1)
	keys := make([][]byte, 0, newRegionCnt+1)
	keys = append(keys, derived.StartKey)
	splitKey := splitReq.SplitKey
	if len(splitKey) == 0 {
		err = errors.New("missing split key")
		return
	}
	if bytes.Compare(splitKey, keys[len(keys)-1]) <= 0 {
		err = errors.Errorf("invalid split request:%s", splitReq)
		return
	}
	if len(splitReq.NewPeerIds) != len(derived.Peers) {
		err = errors.Errorf("invalid new peer id count, need %d but got %d",
			len(derived.Peers), len(splitReq.NewPeerIds))
		return
	}
	keys = append(keys, splitKey)

	err = util.CheckKeyInRegion(keys[len(keys)-1], a.region)
	if err != nil {
		return
	}

	if len(keys) < 2 {
		err = errors.New("losing the startKey or splitKey")
		return

	}

	log.Info(fmt.Sprintf("%s split region %s, keys %v", a.tag, a.region, keys))
	derived.RegionEpoch.Version += uint64(newRegionCnt)
	newRegion := &metapb.Region{
		Id:          splitReq.NewRegionId,
		RegionEpoch: derived.RegionEpoch,
		StartKey:    keys[0],
		EndKey:      keys[1],
	}
	newRegion.Peers = make([]*metapb.Peer, len(derived.Peers))
	for j := range newRegion.Peers {
		newRegion.Peers[j] = &metapb.Peer{
			Id:      splitReq.NewPeerIds[j],
			StoreId: derived.Peers[j].StoreId,
		}
	}
	meta.WriteRegionState(aCtx.wb, newRegion, rspb.PeerState_Normal)
	writeInitialApplyState(aCtx.wb, newRegion.Id)
	regions = append(regions, newRegion)
	derived.StartKey = keys[len(keys)-1]
	regions = append(regions, derived)
	meta.WriteRegionState(aCtx.wb, derived, rspb.PeerState_Normal)

	resp = &raft_cmdpb.AdminResponse{
		Split: &raft_cmdpb.SplitResponse{
			Regions: regions,
		},
	}
	result = applyResult{tp: applyResultTypeExecResult, data: &execResultSplitRegion{
		regions: regions,
		derived: derived,
	}}
	return
}

func (a *applier) execCompactLog(aCtx *applyContext, req *raft_cmdpb.AdminRequest) (
	resp *raft_cmdpb.AdminResponse, result applyResult, err error) {
	compactIndex := req.CompactLog.CompactIndex
	resp = new(raft_cmdpb.AdminResponse)
	applyState := &aCtx.execCtx.applyState
	firstIndex := applyState.TruncatedState.Index + 1
	if compactIndex <= firstIndex {
		log.Debug(fmt.Sprintf("%s compact index <= first index, no need to compact", a.tag))
		return
	}
	compactTerm := req.CompactLog.CompactTerm
	if compactTerm == 0 {
		log.Info(fmt.Sprintf("%s compact term missing, skip", a.tag))
		// old format compact log command, safe to ignore.
		err = errors.New("command format is outdated, please upgrade leader")
		return
	}

	if compactIndex <= applyState.TruncatedState.Index || compactIndex > applyState.AppliedIndex {
		return
	}
	log.Debug(fmt.Sprintf("%s compact log entries to prior to %d", a.tag, compactIndex))
	applyState.TruncatedState.Index = compactIndex
	applyState.TruncatedState.Term = compactTerm

	result = applyResult{tp: applyResultTypeExecResult, data: &execResultCompactLog{
		truncatedIndex: applyState.TruncatedState.Index,
		firstIndex:     firstIndex,
	}}
	return
}
