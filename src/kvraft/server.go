package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	return
}

// applyTimeout bounds how long a client RPC waits for its command to be
// committed and applied before giving up (so the client can retry elsewhere).
const applyTimeout = 500 * time.Millisecond

// Op is the command replicated through Raft. A single type covers Get/Put/Append.
type Op struct {
	Type     string // "Get", "Put", or "Append"
	Key      string
	Value    string
	ClientId int64
	Seq      int64
}

// result is what an applied Op yields, delivered back to the waiting RPC.
// clientId+seq identify which command actually committed at the index, so a
// waiter can detect if a *different* command took its slot after a leader change.
type result struct {
	err      Err
	value    string
	clientId int64
	seq      int64
}

// lastReply remembers the outcome of the most recent request per client, for
// exactly-once semantics across retries and leader changes. Fields are exported
// because this struct is gob-encoded into the snapshot.
type lastReply struct {
	Seq   int64
	Value string // Append's returned value
	Err   Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	data       map[string]string   // the key/value store
	dedup      map[int64]lastReply // clientId -> last applied request
	waiters    map[int]chan result // log index -> waiting RPC channel
	lastApplied int                // highest log index applied to the state machine
}

// waitFor submits op to Raft and blocks until it is applied (or times out /
// leadership is lost). Returns the applied result.
func (kv *KVServer) waitFor(op Op) result {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return result{err: ErrWrongLeader}
	}

	kv.mu.Lock()
	ch := make(chan result, 1)
	kv.waiters[index] = ch
	kv.mu.Unlock()

	select {
	case res := <-ch:
		kv.mu.Lock()
		delete(kv.waiters, index)
		kv.mu.Unlock()
		// If a different command committed at our index (leadership changed
		// between Start and apply), tell the client to retry.
		if res.clientId != op.ClientId || res.seq != op.Seq {
			return result{err: ErrWrongLeader}
		}
		return res
	case <-time.After(applyTimeout):
		kv.mu.Lock()
		delete(kv.waiters, index)
		kv.mu.Unlock()
		return result{err: ErrTimeout}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	res := kv.waitFor(Op{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
	reply.Err = res.err
	reply.Value = res.value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	res := kv.waitFor(Op{
		Type:     "Put",
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
	reply.Err = res.err
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	res := kv.waitFor(Op{
		Type:     "Append",
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	})
	reply.Err = res.err
}

// applyLoop consumes committed entries and snapshots from Raft, applies them to
// the state machine, and wakes any RPC waiting on that log index.
func (kv *KVServer) applyLoop() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}
		if msg.CommandValid {
			kv.applyCommand(msg)
		} else if msg.SnapshotValid {
			kv.applySnapshot(msg)
		}
	}
}

func (kv *KVServer) applyCommand(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if msg.CommandIndex <= kv.lastApplied {
		return
	}
	kv.lastApplied = msg.CommandIndex

	op := msg.Command.(Op)
	var res result

	// De-duplicate mutating operations. Get is idempotent but we still cache
	// its result to answer retries consistently.
	last, seen := kv.dedup[op.ClientId]
	if seen && last.Seq >= op.Seq && op.Type != "Get" {
		res = result{err: last.Err, value: last.Value}
	} else {
		switch op.Type {
		case "Get":
			if v, ok := kv.data[op.Key]; ok {
				res = result{err: OK, value: v}
			} else {
				res = result{err: ErrNoKey}
			}
		case "Put":
			kv.data[op.Key] = op.Value
			res = result{err: OK}
		case "Append":
			kv.data[op.Key] += op.Value
			res = result{err: OK}
		}
		if op.Type != "Get" {
			kv.dedup[op.ClientId] = lastReply{Seq: op.Seq, Value: res.value, Err: res.err}
		}
	}

	// Stamp the identity of the command that actually committed here.
	res.clientId = op.ClientId
	res.seq = op.Seq

	// Wake the waiting RPC, if this server is the one that proposed it and is
	// still leader for this index.
	if ch, ok := kv.waiters[msg.CommandIndex]; ok {
		ch <- res
	}

	kv.maybeSnapshot(msg.CommandIndex)
}

func (kv *KVServer) applySnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if msg.SnapshotIndex <= kv.lastApplied {
		return
	}
	kv.readSnapshot(msg.Snapshot)
	kv.lastApplied = msg.SnapshotIndex
}

// maybeSnapshot asks Raft to compact its log once persisted state grows past
// maxraftstate. Caller holds kv.mu.
func (kv *KVServer) maybeSnapshot(index int) {
	if kv.maxraftstate < 0 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.dedup)
	kv.rf.Snapshot(index, w.Bytes())
}

// readSnapshot restores the state machine from a snapshot. Caller holds kv.mu.
func (kv *KVServer) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvData map[string]string
	var dedup map[int64]lastReply
	if d.Decode(&kvData) != nil || d.Decode(&dedup) != nil {
		return
	}
	kv.data = kvData
	kv.dedup = dedup
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// StartKVServer starts a fault-tolerant key/value server backed by Raft.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.data = make(map[string]string)
	kv.dedup = make(map[int64]lastReply)
	kv.waiters = make(map[int]chan result)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Restore any snapshot taken before a crash.
	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applyLoop()

	return kv
}
