// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/log"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// lockedRand is a small wrapper around rand.Rand to provide
// synchronization among multiple raft groups. Only the methods needed
// by the code are exposed (e.g. Intn).
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1].
	randomizedElectionTimeout int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	PendingConfIndex uint64

	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftlog := newLog(c.Storage)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	peers := c.peers
	if len(cs.Nodes) > 0 {
		if len(peers) > 0 {
			panic("cannot specify both newRaft (peers) and ConfState.(Nodes)")
		}
		peers = cs.Nodes
	}
	r := &Raft{
		id:               c.ID,
		Lead:             None,
		RaftLog:          raftlog,
		Prs:              make(map[uint64]*Progress),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}
	for _, p := range peers {
		r.Prs[p] = &Progress{Next: 1}
	}

	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range nodes(r) {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%d", n))
	}

	log.Info(fmt.Sprintf("newRaft %d [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.RaftLog.lastTerm()))
	return r
}

func (r *Raft) GetSnap() *pb.Snapshot {
	return r.RaftLog.pending_snapshot
}

// softState return the softState of this peer
func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

// hardState return the hardState of this peer
func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) quorum() int { return len(r.Prs)/2 + 1 }

// send persists state to stable storage and then sends to its mailbox.
func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			// All campaign messages need to have the term set when sending.
			// - MessageType_MsgRequestVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MessageType_MsgRequestVoteResponse: m.Term is the new r.Term if the MessageType_MsgRequestVote was
			//   granted, non-zero for the same reason MessageType_MsgRequestVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}
		// do not attach term to MessageType_MsgPropose
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) getProgress(id uint64) *Progress {
	return r.Prs[id]
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	pr := r.getProgress(to)
	m := pb.Message{}
	m.To = to

	term, errt := r.RaftLog.Term(pr.Next - 1)
	ents, erre := r.RaftLog.Entries(pr.Next)

	log.Debug(fmt.Sprintf("The last entry's info: %d %v", term, ents))

	if errt != nil || erre != nil { // send snapshot if we failed to get term or entries
		m.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				log.Debug(fmt.Sprintf("%d failed to send snapshot to %d because snapshot is temporarily unavailable", r.id, to))
				return false
			}
			panic(err)
		}
		if IsEmptySnap(&snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = &snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		log.Debug(fmt.Sprintf("%d [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %d [%v]",
			r.id, r.RaftLog.firstIndex(), r.RaftLog.committed, sindex, sterm, to, pr))
		log.Debug(fmt.Sprintf("%d paused sending replication messages to %d [%v]", r.id, to, pr))
	} else {
		// Raft: Replication_Step2:::Send entries.
		// You need to set the info of MsgAppend_message, which include logTerm, index, msgType, entries and commit.
		panic("Raft: Replication_Step2:::Your code here.")




	}
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.

	// Raft: Leader_Election_Step7:::send heartbeat.
	// You need to fill in pb.MessageType_MsgHeartbeat and send it. And don't forget to carry the commit index.
	panic("Raft: Leader_Election_Step7:::Your code here.")



}

func (r *Raft) forEachProgress(f func(id uint64, pr *Progress)) {
	for id, pr := range r.Prs {
		f(id, pr)
	}
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.Prs.
func (r *Raft) bcastAppend() {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}

		r.sendAppend(id)
	})
}

// bcastHeartbeat sends RPC, without entries to all the peers.
func (r *Raft) bcastHeartbeat() {
	r.forEachProgress(func(id uint64, _ *Progress) {
		if id == r.id {
			return
		}
		r.sendHeartbeat(id)
	})
}

// maybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
func (r *Raft) maybeCommit() bool {
	matchIndex := make(uint64Slice, len(r.Prs))
	idx := 0
	for _, p := range r.Prs {
		matchIndex[idx] = p.Match
		idx++
	}
	sort.Sort(matchIndex)
	mci := matchIndex[len(matchIndex)-r.quorum()]
	return r.RaftLog.maybeCommit(mci, r.Term)
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()

	r.votes = make(map[uint64]bool)
	r.forEachProgress(func(id uint64, pr *Progress) {
		*pr = Progress{Next: r.RaftLog.LastIndex() + 1}
		if id == r.id {
			pr.Match = r.RaftLog.LastIndex()
		}
	})

	r.PendingConfIndex = 0
}

func (r *Raft) appendEntry(es ...pb.Entry) {
	li := r.RaftLog.LastIndex()
	// Raft: Replication_Step1:::Append entries.
	// You need to set the term and the index of the entries.
	panic("Raft: Replication_Step1:::Your code here.")



	// use latest "last" index after truncate/append
	li = r.RaftLog.append(es...)
	r.getProgress(r.id).maybeUpdate(li)
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	r.maybeCommit()
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *Raft) tickElection() {
	// Raft: Leader_Election_Step1:::Advance the electionElapsed.
	// You need to advance the time.
	panic("Raft: Leader_Election_Step1:::Your code here.")


	if r.promotable() && r.pastElectionTimeout() {
		// Raft: Leader_Election_Step2:::Launching a round of election.
		// You need to update the electionElapsed and set the msgHup message.
		panic("Raft: Leader_Election_Step2:::Your code here.")




	}
}

// tickHeartbeat is run by leaders to send a MessageType_MsgBeat after r.heartbeatTimeout.
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		// If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
		if r.State == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}

	if r.State != StateLeader {
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	log.Info(fmt.Sprintf("%d became follower at term %d", r.id, r.Term))
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Raft: Leader_Election_Step3:::becomeCandidate.
	// You need to set the term, vote and state.
	panic("Raft: Leader_Election_Step3:::Your code here.")



	log.Info(fmt.Sprintf("%d became candidate at term %d", r.id, r.Term))
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader

	// Conservatively set the PendingConfIndex to the last index in the
	// log. There may or may not be a pending config change, but it's
	// safe to delay any future proposals until we commit all our
	// pending log entries, and scanning the entire tail of the log
	// could be expensive.
	r.PendingConfIndex = r.RaftLog.LastIndex()

	emptyEnt := pb.Entry{Data: nil}
	r.appendEntry(emptyEnt)
	log.Info(fmt.Sprintf("%d became leader at term %d", r.id, r.Term))
}

func (r *Raft) campaign() {
	r.becomeCandidate()
	voteMsg := pb.MessageType_MsgRequestVote
	term := r.Term

	if r.quorum() == r.poll(r.id, pb.MessageType_MsgRequestVoteResponse, true) {
		// Raft: Leader_Election_Step4:::becomeLeader.
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
		panic("Raft: Leader_Election_Step4:::Your code here.")


		return
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		log.Info(fmt.Sprintf("%d [logterm: %d, index: %d] sent %s request to %d at term %d", r.id,
			r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), voteMsg, id, r.Term))

		r.send(pb.Message{Term: term, To: id, MsgType: voteMsg, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.lastTerm()})
	}
}

func (r *Raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	if v {
		log.Info(fmt.Sprintf("%d received %s from %d at term %d", r.id, t, id, r.Term))
	} else {
		log.Info(fmt.Sprintf("%d received %s rejection from %d at term %d", r.id, t, id, r.Term))
	}
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Handle the message term, which may result in our stepping down to a follower.
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		log.Info(fmt.Sprintf("%d [term: %d] received a %s message with higher term from %d [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term))
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		log.Info(fmt.Sprintf("%d [term: %d] ignored a %s message with lower term from %d [term: %d]", r.id, r.Term, m.MsgType, m.From, m.Term))
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if r.State != StateLeader {
			ents, err := r.RaftLog.slice(r.RaftLog.applied+1, r.RaftLog.committed+1)
			if err != nil {
				log.Fatal(fmt.Sprintf("unexpected error getting unapplied entries (%v)", err))
			}
			if n := numOfPendingConf(ents); n != 0 && r.RaftLog.committed > r.RaftLog.applied {
				log.Warn(fmt.Sprintf("%d cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n))
				return nil
			}

			log.Info(fmt.Sprintf("%d is starting a new election at term %d", r.id, r.Term))

			r.campaign()
		} else {
			log.Debug(fmt.Sprintf("%d ignoring MessageType_MsgHup because already leader", r.id))
		}

	case pb.MessageType_MsgRequestVote:
		canVote := false

		// Raft: Leader_Election_Step5:::Voting or not.
		// We can vote if this is a repeat of a vote we've already cast...
		// ...we haven't voted and we don't think there's a leader yet in this term...
		panic("Raft: Leader_Election_Step5:::Your code here.")


		// ...and we believe the candidate is up to date.
		if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			log.Info(fmt.Sprintf("%d [logterm: %d, index: %d, vote: %d] cast %s for %d [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term))
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse})
			// Only record real votes.
			r.electionElapsed = 0
			r.Vote = m.From
		} else {
			log.Info(fmt.Sprintf("%d [logterm: %d, index: %d, vote: %d] rejected %s from %d [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.lastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term))
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}

	default:
		switch r.State {
		case StateFollower:
			err := r.stepFollower(m)
			if err != nil {
				return err
			}
		case StateCandidate:
			err := r.stepCandidate(m)
			if err != nil {
				return err
			}
		case StateLeader:
			err := r.stepLeader(m)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type stepFunc func(r *Raft, m pb.Message) error

// stepLeader handle leader's message
func (r *Raft) stepLeader(m pb.Message) error {
	pr := r.getProgress(m.From)
	if pr == nil && m.MsgType != pb.MessageType_MsgBeat && m.MsgType != pb.MessageType_MsgPropose {
		log.Debug(fmt.Sprintf("%d no progress available for %d", r.id, m.From))
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			log.Fatal(fmt.Sprintf("%d stepped empty MessageType_MsgPropose", r.id))
		}
		if _, ok := r.Prs[r.id]; !ok {
			// If we are not currently a member of the range (i.e. this node
			// was removed from the configuration while serving as leader),
			// drop any new proposals.
			return ErrProposalDropped
		}
		if r.leadTransferee != None {
			log.Debug(fmt.Sprintf("%d [term %d] transfer leadership to %d is in progress; dropping proposal", r.id, r.Term, r.leadTransferee))
			return ErrProposalDropped
		}

		for i, e := range m.Entries {
			if e.EntryType == pb.EntryType_EntryConfChange {
				if r.PendingConfIndex > r.RaftLog.applied {
					log.Info(fmt.Sprintf("propose conf %s ignored since pending unapplied configuration [index %d, applied %d]",
						e.String(), r.PendingConfIndex, r.RaftLog.applied))
					m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
				} else {
					r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(i) + 1
				}
			}
		}

		es := make([]pb.Entry, 0, len(m.Entries))
		for _, e := range m.Entries {
			es = append(es, *e)
		}

		r.appendEntry(es...)
		r.bcastAppend()
		return nil
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			log.Debug(fmt.Sprintf("%d received MessageType_MsgAppend rejection(lastindex: %d) from %d for index %d",
				r.id, m.RejectHint, m.From, m.Index))
			if pr.maybeDecrTo(m.Index, m.RejectHint) {
				r.sendAppend(m.From)
			}
		} else {
			if pr.maybeUpdate(m.Index) {
				// Raft: Replication_Step3:::Broadcast commit index.
				// You need to determine if the current log entry needs to be committed. And broadcast the result if the
				// entry need to be committed.
				panic("Raft: Replication_Step3:::Your code here.")


				// Transfer leadership is in progress.
				if m.From == r.leadTransferee && pr.Match == r.RaftLog.LastIndex() {
					log.Info(fmt.Sprintf("%d sent MessageType_MsgTimeoutNow to %d after received MessageType_MsgAppendResponse", r.id, m.From))
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader:
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				log.Info(fmt.Sprintf("%d [term %d] transfer leadership to %d is in progress, ignores request to same node %d",
					r.id, r.Term, leadTransferee, leadTransferee))
				return nil
			}
			r.abortLeaderTransfer()
			log.Info(fmt.Sprintf("%d [term %d] abort previous transferring leadership to %d", r.id, r.Term, lastLeadTransferee))
		}
		if leadTransferee == r.id {
			log.Debug(fmt.Sprintf("%d is already leader. Ignored transferring leadership to self", r.id))
			return nil
		}
		// Transfer leadership to third party.
		log.Info(fmt.Sprintf("%d [term %d] starts to transfer leadership to %d", r.id, r.Term, leadTransferee))
		// Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(leadTransferee)
			log.Info(fmt.Sprintf("%d sends MessageType_MsgTimeoutNow to %d immediately as %d already has up-to-date log", r.id, leadTransferee, leadTransferee))
		} else {
			r.sendAppend(leadTransferee)
		}
	}
	return nil
}

// stepCandidate handle candidate's message
func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		log.Info(fmt.Sprintf("%d no leader at term %d; dropping proposal", r.id, r.Term))
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From) // always m.Term == r.Term
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVoteResponse:
		gr := r.poll(m.From, m.MsgType, !m.Reject)
		log.Info(fmt.Sprintf("%d [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.MsgType, len(r.votes)-gr))
		// Raft: Leader_Election_Step6:::Change state.
		// If the candidate receives majority of the active votes, it can become the leader and broadcast the result.
		// But if the candidate receives majority of the negative votes, it can become the follower.
		panic("Raft: Leader_Election_Step6:::Your code here.")




	case pb.MessageType_MsgTimeoutNow:
		log.Debug(fmt.Sprintf("%d [term %d state %v] ignored MessageType_MsgTimeoutNow from %d", r.id, r.Term, r.State, m.From))
	}
	return nil
}

// stepFollower handle follower's message
func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		log.Info(fmt.Sprintf("%d is no leader at term %d; dropping proposal", r.id, r.Term))
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead == None {
			log.Info(fmt.Sprintf("%d no leader at term %d; dropping leader transfer msg", r.id, r.Term))
			return nil
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgTimeoutNow:
		if r.promotable() {
			log.Info(fmt.Sprintf("%d [term %d] received MessageType_MsgTimeoutNow from %d and starts an election to get leadership.", r.id, r.Term, m.From))
			r.campaign()
		} else {
			log.Info(fmt.Sprintf("%d received MessageType_MsgTimeoutNow from %d but is not promotable", r.id, m.From))
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	ents := make([]pb.Entry, 0, len(m.Entries))
	for _, ent := range m.Entries {
		ents = append(ents, *ent)
	}
	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, ents...); ok {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: mlastIndex})
	} else {
		log.Debug(fmt.Sprintf("%d [logterm: %d, index: %d] rejected MessageType_MsgAppend [logterm: %d, index: %d] from %d",
			r.id, r.RaftLog.zeroTermOnRangeErr(r.RaftLog.Term(m.Index)), m.Index, m.LogTerm, m.Index, m.From))
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index, Reject: true, RejectHint: r.RaftLog.LastIndex()})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	sindex, sterm := m.Snapshot.Metadata.Index, m.Snapshot.Metadata.Term
	if r.restore(*m.Snapshot) {
		log.Info(fmt.Sprintf("%d [commit: %d] restored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm))
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex()})
	} else {
		log.Info(fmt.Sprintf("%d [commit: %d] ignored snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, sindex, sterm))
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
	}
}

// restore recovers the state machine from a snapshot. It restores the log and the
// configuration of state machine.
func (r *Raft) restore(s pb.Snapshot) bool {
	if s.Metadata.Index <= r.RaftLog.committed {
		return false
	}
	if r.RaftLog.matchTerm(s.Metadata.Index, s.Metadata.Term) {
		log.Info(fmt.Sprintf("%d [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
			r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term))
		r.RaftLog.commitTo(s.Metadata.Index)
		return false
	}

	log.Info(fmt.Sprintf("%d [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]",
		r.id, r.RaftLog.committed, r.RaftLog.LastIndex(), r.RaftLog.lastTerm(), s.Metadata.Index, s.Metadata.Term))

	r.RaftLog.restore(s)
	r.Prs = make(map[uint64]*Progress)
	r.restoreNode(s.Metadata.ConfState.Nodes)
	return true
}

func (r *Raft) restoreNode(nodes []uint64) {
	for _, n := range nodes {
		match, next := uint64(0), r.RaftLog.LastIndex()+1
		if n == r.id {
			match = next - 1
		}
		r.setProgress(n, match, next)
		log.Info(fmt.Sprintf("%d restored progress of %d [%+v]", r.id, n, r.getProgress(n)))
	}
}

// promotable indicates whether state machine can be promoted to Leader,
// which is true when its own id is in progress list.
func (r *Raft) promotable() bool {
	_, ok := r.Prs[r.id]
	return ok
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	if r.getProgress(id) == nil {
		r.setProgress(id, 0, r.RaftLog.LastIndex()+1)
	} else {
		return
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	delete(r.Prs, id)

	// do not try to commit or abort transferring if there is no nodes in the cluster.
	if len(r.Prs) == 0 {
		return
	}

	// The quorum size is now smaller, so see if any pending entries can
	// be committed.
	if r.maybeCommit() {
		r.bcastAppend()
	}
	// If the removed node is the leadTransferee, then abort the leadership transferring.
	if r.State == StateLeader && r.leadTransferee == id {
		r.abortLeaderTransfer()
	}
}

func (r *Raft) setProgress(id, match, next uint64) {
	r.Prs[id] = &Progress{Next: next, Match: match}
	return
}

func (r *Raft) loadState(state pb.HardState) {
	if state.Commit < r.RaftLog.committed || state.Commit > r.RaftLog.LastIndex() {
		log.Fatal(fmt.Sprintf("%d state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex()))
	}
	r.RaftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

// pastElectionTimeout returns true iff r.electionElapsed is greater
// than or equal to the randomized election timeout in
// [electiontimeout, 2 * electiontimeout - 1].
func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, MsgType: pb.MessageType_MsgTimeoutNow})
}

func (r *Raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func numOfPendingConf(ents []pb.Entry) int {
	n := 0
	for i := range ents {
		if ents[i].EntryType == pb.EntryType_EntryConfChange {
			n++
		}
	}
	return n
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

// maybeUpdate returns false if the given n index comes from an outdated message.
// Otherwise it updates the progress and returns true.
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
	}
	if pr.Next < n+1 {
		pr.Next = n + 1
	}
	return updated
}

// maybeDecrTo returns false if the given to index comes from an out of order message.
// Otherwise it decreases the progress next index to min(rejected, last) and returns true.
func (pr *Progress) maybeDecrTo(rejected, last uint64) bool {
	// the rejection must be stale if the progress has matched and "rejected"
	// is smaller than "match".
	if rejected <= pr.Match {
		return false
	}
	if pr.Next = min(rejected, last+1); pr.Next < 1 {
		pr.Next = 1
	}
	return true
}
