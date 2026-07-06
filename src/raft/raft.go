package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// server roles.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// election/heartbeat timing.
const (
	heartbeatInterval = 100 * time.Millisecond
	electionTimeoutLo = 300 * time.Millisecond
	electionTimeoutHi = 600 * time.Millisecond
)

// LogEntry is a single command in the replicated log.
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// --- Persistent state on all servers (Figure 2) ---
	currentTerm int
	votedFor    int // -1 == none
	// log[] is stored with a leading dummy entry at slot 0 that holds the
	// snapshot's lastIncludedIndex/Term. So the real entry with absolute index
	// i lives at log[i - lastIncludedIndex].
	log               []LogEntry
	lastIncludedIndex int // index of the last entry in the latest snapshot
	lastIncludedTerm  int // term of that entry

	// --- Volatile state on all servers ---
	commitIndex int
	lastApplied int
	role        Role

	// --- Volatile state on leaders (reinitialized after election) ---
	nextIndex  []int
	matchIndex []int

	// election bookkeeping
	electionResetAt time.Time
	electionTimeout time.Duration // fixed once per reset, re-rolled on each reset

	// apply pipeline
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

// ---------- log index helpers (all require rf.mu held) ----------

// lastLogIndex returns the absolute index of the last log entry.
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

// term of the entry at absolute index idx. idx must be >= lastIncludedIndex.
func (rf *Raft) termAt(idx int) int {
	return rf.log[idx-rf.lastIncludedIndex].Term
}

// entryAt returns the LogEntry at absolute index idx.
func (rf *Raft) entryAt(idx int) LogEntry {
	return rf.log[idx-rf.lastIncludedIndex]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// HasCurrentTermLog reports whether the last log entry is from the current
// term. A leader that has already committed such an entry can safely advance
// commitIndex; shardkv uses this to decide whether to append a no-op. This is
// an extension used by the kvraft/shardkv services, not part of the core lab.
func (rf *Raft) HasCurrentTermLog() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.termAt(rf.lastLogIndex()) == rf.currentTerm
}

// encodeState serializes the persistent Raft state. Caller holds rf.mu.
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

// persist saves Raft's persistent state (and the current snapshot) to stable
// storage. Caller holds rf.mu.
func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeState(), rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		// Fresh start: one dummy entry at index 0.
		rf.log = []LogEntry{{Term: 0}}
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		// Corrupt state; fall back to empty. Should not happen in tests.
		rf.log = []LogEntry{{Term: 0}}
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	// Everything up to the snapshot is already applied.
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Ignore stale snapshots or ones we've already superseded.
	if index <= rf.lastIncludedIndex || index > rf.commitIndex {
		return
	}

	// Trim the log: keep entries after `index`, with a new dummy at slot 0.
	newLog := make([]LogEntry, 1)
	newLog[0].Term = rf.termAt(index)
	newLog = append(newLog, rf.log[index-rf.lastIncludedIndex+1:]...)

	rf.lastIncludedTerm = rf.termAt(index)
	rf.lastIncludedIndex = index
	rf.log = newLog

	// Persist state + snapshot atomically.
	rf.persister.Save(rf.encodeState(), snapshot)
}

// ---------- RequestVote RPC ----------

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	// Grant vote if we haven't voted for someone else this term and the
	// candidate's log is at least as up-to-date as ours.
	upToDate := rf.candidateUpToDate(args.LastLogIndex, args.LastLogTerm)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()
		rf.persist()
	}
}

// resetElectionTimer restarts the election countdown with a fresh random
// deadline. Caller holds rf.mu.
func (rf *Raft) resetElectionTimer() {
	rf.electionResetAt = time.Now()
	rf.electionTimeout = electionTimeoutLo +
		time.Duration(rand.Int63n(int64(electionTimeoutHi-electionTimeoutLo)))
}

// candidateUpToDate implements the "at least as up-to-date" comparison
// (§5.4.1). Caller holds rf.mu.
func (rf *Raft) candidateUpToDate(lastIndex, lastTerm int) bool {
	myIndex := rf.lastLogIndex()
	myTerm := rf.termAt(myIndex)
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= myIndex
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// ---------- AppendEntries RPC ----------

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// Fast-backup fields (accelerated log backtracking, §5.3 optimization).
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}
	// Valid leader for this term: reset election timer and ensure follower role.
	rf.role = Follower
	rf.resetElectionTimer()

	// If PrevLogIndex is inside the snapshot, we can't verify it directly.
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// The leader is behind our snapshot; ask it to send from the entry
		// right after our snapshot.
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		// Special-case: if the request still carries entries beyond our
		// snapshot, we can try to append them below by clamping.
		if args.PrevLogIndex+len(args.Entries) <= rf.lastIncludedIndex {
			return
		}
		// Trim the leader's entries so PrevLogIndex aligns with our snapshot.
		offset := rf.lastIncludedIndex - args.PrevLogIndex
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
		if offset <= len(args.Entries) {
			args.Entries = args.Entries[offset:]
		} else {
			args.Entries = nil
		}
	}

	// Consistency check on PrevLogIndex/PrevLogTerm.
	if args.PrevLogIndex > rf.lastLogIndex() {
		// We are missing entries: tell the leader where our log ends.
		reply.ConflictIndex = rf.lastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		// Term conflict at PrevLogIndex: report the conflicting term and the
		// first index of that term so the leader can skip a whole term.
		reply.ConflictTerm = rf.termAt(args.PrevLogIndex)
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex && rf.termAt(i-1) == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// Append any new entries, deleting conflicting suffixes.
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.lastLogIndex() {
			if rf.termAt(idx) == entry.Term {
				continue
			}
			// Conflict: truncate everything from idx onward.
			rf.log = rf.log[:idx-rf.lastIncludedIndex]
		}
		// Append the rest of the entries.
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}
	rf.persist()

	// Advance commit index.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.applyCond.Signal()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// ---------- InstallSnapshot RPC ----------

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}
	rf.role = Follower
	rf.resetElectionTimer()

	// Ignore stale snapshots.
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	// Trim our log to reflect the incoming snapshot.
	if args.LastIncludedIndex < rf.lastLogIndex() &&
		rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		// We have the covered entry; keep the tail after it.
		newLog := make([]LogEntry, 1)
		newLog[0].Term = args.LastIncludedTerm
		newLog = append(newLog, rf.log[args.LastIncludedIndex-rf.lastIncludedIndex+1:]...)
		rf.log = newLog
	} else {
		// Discard the whole log; start fresh from the snapshot.
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.persister.Save(rf.encodeState(), args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()

	// Deliver the snapshot to the service. Do this without holding rf.mu to
	// avoid deadlock with the apply goroutine.
	rf.applyCh <- msg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

// ---------- role transitions (caller holds rf.mu) ----------

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.role = Follower
	rf.persist()
}

// ---------- Start / Kill ----------

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := rf.lastLogIndex()
	rf.persist()
	// Kick replication immediately for snappier agreement.
	rf.broadcastAppendEntries()
	return index, rf.currentTerm, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// ---------- election ----------

func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		// Use the deadline fixed at the last reset (not a fresh roll each tick),
		// so the timeout distribution is uniform and elections aren't triggered
		// too eagerly.
		if rf.role != Leader && time.Since(rf.electionResetAt) >= rf.electionTimeout {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// startElection transitions to candidate and requests votes. Caller holds rf.mu.
func (rf *Raft) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	rf.persist()

	term := rf.currentTerm
	lastIndex := rf.lastLogIndex()
	lastTerm := rf.termAt(lastIndex)
	votes := int32(1)

	args := &RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// Ignore if we've moved on since sending this request.
			if rf.currentTerm != term || rf.role != Candidate {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if reply.VoteGranted {
				if atomic.AddInt32(&votes, 1) > int32(len(rf.peers)/2) {
					rf.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeLeader initializes leader state and starts sending heartbeats.
// Caller holds rf.mu.
func (rf *Raft) becomeLeader() {
	if rf.role != Candidate {
		return
	}
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	next := rf.lastLogIndex() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = next
		rf.matchIndex[i] = 0
	}
	rf.broadcastAppendEntries()
}

// heartbeatLoop sends periodic AppendEntries while this peer is leader.
func (rf *Raft) heartbeatLoop() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role == Leader {
			rf.broadcastAppendEntries()
		}
		rf.mu.Unlock()
		time.Sleep(heartbeatInterval)
	}
}

// broadcastAppendEntries sends AppendEntries/InstallSnapshot to all peers.
// Caller holds rf.mu.
func (rf *Raft) broadcastAppendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicateTo(peer, rf.currentTerm)
	}
}

// replicateTo sends the appropriate message to one follower.
func (rf *Raft) replicateTo(server, term int) {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	next := rf.nextIndex[server]

	// If the follower is behind our snapshot, ship the snapshot instead.
	if next <= rf.lastIncludedIndex {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()

		reply := &InstallSnapshotReply{}
		if !rf.sendInstallSnapshot(server, args, reply) {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != Leader || rf.currentTerm != term {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		return
	}

	prevIndex := next - 1
	prevTerm := rf.termAt(prevIndex)
	entries := make([]LogEntry, rf.lastLogIndex()-prevIndex)
	copy(entries, rf.log[prevIndex-rf.lastIncludedIndex+1:])

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(server, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader || rf.currentTerm != term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatch
			rf.nextIndex[server] = newMatch + 1
		}
		rf.advanceCommit()
		return
	}

	// Failure: use the conflict info to back up nextIndex quickly.
	if reply.ConflictTerm == -1 {
		// Follower told us exactly where to resume (missing entries / snapshot).
		if reply.ConflictIndex > 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	} else {
		// Find the last index in our log for ConflictTerm.
		lastOfTerm := -1
		for i := rf.lastLogIndex(); i > rf.lastIncludedIndex; i-- {
			if rf.termAt(i) == reply.ConflictTerm {
				lastOfTerm = i
				break
			}
		}
		if lastOfTerm >= 0 {
			rf.nextIndex[server] = lastOfTerm + 1
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	}
	if rf.nextIndex[server] < 1 {
		rf.nextIndex[server] = 1
	}
}

// advanceCommit moves commitIndex forward when a majority has replicated an
// entry from the current term (§5.3, §5.4.2). Caller holds rf.mu.
func (rf *Raft) advanceCommit() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
			continue
		}
		count := 1
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			break
		}
	}
}

// applier delivers committed entries to the service in index order.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.killed() == false {
		if rf.lastApplied < rf.commitIndex && rf.lastApplied >= rf.lastIncludedIndex {
			rf.lastApplied++
			// A concurrent snapshot may have moved lastIncludedIndex past us.
			if rf.lastApplied <= rf.lastIncludedIndex {
				rf.lastApplied = rf.lastIncludedIndex
				continue
			}
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.entryAt(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// the service or tester wants to create a Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.votedFor = -1
	rf.role = Follower
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start goroutines
	go rf.ticker()
	go rf.heartbeatLoop()
	go rf.applier()

	return rf
}
