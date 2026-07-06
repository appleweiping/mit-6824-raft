package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const (
	applyTimeout   = 500 * time.Millisecond
	pollInterval   = 80 * time.Millisecond
	migrateInterval = 60 * time.Millisecond
)

// per-shard migration state.
type ShardState int

const (
	Serving   ShardState = iota // owned and serving requests
	Pulling                     // we own it in the new config but must pull data
	BePulling                   // we owned it in the old config; another group will pull
	GCing                       // we have pulled it, must tell the old owner to delete
)

// Shard holds the data plus migration state for one shard.
type Shard struct {
	Data  map[string]string
	State ShardState
}

func newShard() *Shard {
	return &Shard{Data: make(map[string]string), State: Serving}
}

func (s *Shard) copyData() map[string]string {
	d := make(map[string]string, len(s.Data))
	for k, v := range s.Data {
		d[k] = v
	}
	return d
}

// Op is what we replicate through Raft. A single struct covers every kind of
// state change so the apply loop stays uniform.
type Op struct {
	Type string // "Get","Put","Append","Config","InstallShard","DeleteShard","Empty"

	// client op fields
	Key      string
	Value    string
	ClientId int64
	Seq      int64

	// config change
	Config shardctrler.Config

	// shard migration
	ConfigNum int
	Shards    []int
	ShardData map[int]map[string]string
	Dedup     map[int64]int64
}

type result struct {
	err      Err
	value    string
	clientId int64
	seq      int64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int
	dead         int32
	persister    *raft.Persister
	mck          *shardctrler.Clerk

	shards      map[int]*Shard      // shard index -> shard (only shards we hold/handle)
	dedup       map[int64]int64     // clientId -> last applied seq
	waiters     map[int]chan result // log index -> waiter
	lastApplied int

	prevConfig shardctrler.Config
	config     shardctrler.Config
}

// ---------------- client-facing RPCs ----------------

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if !kv.canServe(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	res := kv.submit(Op{Type: "Get", Key: args.Key, ClientId: args.ClientId, Seq: args.Seq})
	reply.Err = res.err
	reply.Value = res.value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if !kv.canServe(shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	res := kv.submit(Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, Seq: args.Seq})
	reply.Err = res.err
}

// canServe reports whether this group currently owns and is ready to serve the
// shard. Caller holds kv.mu.
func (kv *ShardKV) canServe(shard int) bool {
	if kv.config.Shards[shard] != kv.gid {
		return false
	}
	s, ok := kv.shards[shard]
	return ok && s.State == Serving
}

// submit replicates op and waits for it to be applied.
func (kv *ShardKV) submit(op Op) result {
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
		if op.Type == "Get" || op.Type == "Put" || op.Type == "Append" {
			if res.clientId != op.ClientId || res.seq != op.Seq {
				return result{err: ErrWrongLeader}
			}
		}
		return res
	case <-time.After(applyTimeout):
		kv.mu.Lock()
		delete(kv.waiters, index)
		kv.mu.Unlock()
		return result{err: ErrTimeout}
	}
}

// ---------------- apply loop ----------------

func (kv *ShardKV) applyLoop() {
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

func (kv *ShardKV) applyCommand(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if msg.CommandIndex <= kv.lastApplied {
		return
	}
	kv.lastApplied = msg.CommandIndex

	op := msg.Command.(Op)
	var res result
	res.clientId = op.ClientId
	res.seq = op.Seq

	switch op.Type {
	case "Get", "Put", "Append":
		res = kv.applyClientOp(op)
	case "Config":
		kv.applyConfig(op.Config)
	case "InstallShard":
		kv.applyInstallShard(op)
	case "DeleteShard":
		kv.applyDeleteShard(op)
	case "Empty":
		// no-op used to commit an entry in the current term
	}

	if ch, ok := kv.waiters[msg.CommandIndex]; ok {
		ch <- res
	}
	kv.maybeSnapshot(msg.CommandIndex)
}

// applyClientOp applies a Get/Put/Append if the shard is servable and the
// request isn't a duplicate. Caller holds kv.mu.
func (kv *ShardKV) applyClientOp(op Op) result {
	shard := key2shard(op.Key)
	if kv.config.Shards[shard] != kv.gid {
		return result{err: ErrWrongGroup, clientId: op.ClientId, seq: op.Seq}
	}
	s, ok := kv.shards[shard]
	if !ok || s.State != Serving {
		return result{err: ErrWrongGroup, clientId: op.ClientId, seq: op.Seq}
	}

	res := result{err: OK, clientId: op.ClientId, seq: op.Seq}
	if op.Type == "Get" {
		if v, ok := s.Data[op.Key]; ok {
			res.value = v
		} else {
			res.err = ErrNoKey
		}
		return res
	}

	// Mutating op: de-duplicate.
	if last, seen := kv.dedup[op.ClientId]; seen && op.Seq <= last {
		return res
	}
	if op.Type == "Put" {
		s.Data[op.Key] = op.Value
	} else { // Append
		s.Data[op.Key] += op.Value
	}
	kv.dedup[op.ClientId] = op.Seq
	return res
}

// applyConfig advances to the next config, marking shards that need migration.
// Caller holds kv.mu.
func (kv *ShardKV) applyConfig(newConfig shardctrler.Config) {
	if newConfig.Num != kv.config.Num+1 {
		return // only apply the immediate next config, one at a time
	}
	// Every shard must be in a settled (Serving) state before we advance.
	for _, s := range kv.shards {
		if s.State != Serving {
			return
		}
	}

	old := kv.config
	kv.prevConfig = old
	kv.config = newConfig

	for shard := 0; shard < NShards; shard++ {
		oldGid := old.Shards[shard]
		newGid := newConfig.Shards[shard]
		if newGid != kv.gid {
			// We no longer own this shard.
			if oldGid == kv.gid {
				if _, ok := kv.shards[shard]; ok {
					kv.shards[shard].State = BePulling
				}
			}
			continue
		}
		// We own this shard in the new config.
		if oldGid == kv.gid || oldGid == 0 {
			// We already had it, or it was previously unassigned (config 1):
			// nothing to pull, just serve.
			if _, ok := kv.shards[shard]; !ok {
				kv.shards[shard] = newShard()
			}
			kv.shards[shard].State = Serving
		} else {
			// Must pull from the previous owner.
			kv.shards[shard] = &Shard{Data: make(map[string]string), State: Pulling}
		}
	}
}

// applyInstallShard installs pulled shard data. Caller holds kv.mu.
func (kv *ShardKV) applyInstallShard(op Op) {
	if op.ConfigNum != kv.config.Num {
		return
	}
	for _, shard := range op.Shards {
		s, ok := kv.shards[shard]
		if !ok || s.State != Pulling {
			continue
		}
		data := op.ShardData[shard]
		ns := &Shard{Data: make(map[string]string, len(data)), State: GCing}
		for k, v := range data {
			ns.Data[k] = v
		}
		kv.shards[shard] = ns
	}
	// Merge dedup info from the source group so we don't re-apply their
	// already-applied client requests.
	for cid, seq := range op.Dedup {
		if cur, ok := kv.dedup[cid]; !ok || seq > cur {
			kv.dedup[cid] = seq
		}
	}
}

// applyDeleteShard finalizes a shard after we've confirmed the old owner
// deleted its copy: GCing -> Serving; and BePulling -> removed. Caller holds mu.
func (kv *ShardKV) applyDeleteShard(op Op) {
	if op.ConfigNum != kv.config.Num {
		return
	}
	for _, shard := range op.Shards {
		s, ok := kv.shards[shard]
		if !ok {
			continue
		}
		if s.State == GCing {
			s.State = Serving
		} else if s.State == BePulling {
			delete(kv.shards, shard)
		}
	}
}

// ---------------- snapshotting ----------------

func (kv *ShardKV) maybeSnapshot(index int) {
	if kv.maxraftstate < 0 || kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.dedup)
	e.Encode(kv.config)
	e.Encode(kv.prevConfig)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) applySnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if msg.SnapshotIndex <= kv.lastApplied {
		return
	}
	kv.readSnapshot(msg.Snapshot)
	kv.lastApplied = msg.SnapshotIndex
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var shards map[int]*Shard
	var dedup map[int64]int64
	var config, prevConfig shardctrler.Config
	if d.Decode(&shards) != nil || d.Decode(&dedup) != nil ||
		d.Decode(&config) != nil || d.Decode(&prevConfig) != nil {
		return
	}
	kv.shards = shards
	kv.dedup = dedup
	kv.config = config
	kv.prevConfig = prevConfig
}

// ---------------- background config puller ----------------

// configPoller periodically fetches the next config, but only when all shards
// are in a settled state (so config changes are processed one at a time).
func (kv *ShardKV) configPoller() {
	for !kv.killed() {
		time.Sleep(pollInterval)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		kv.mu.Lock()
		settled := true
		for _, s := range kv.shards {
			if s.State != Serving {
				settled = false
				break
			}
		}
		nextNum := kv.config.Num + 1
		kv.mu.Unlock()
		if !settled {
			continue
		}
		next := kv.mck.Query(nextNum)
		if next.Num == nextNum {
			kv.rf.Start(Op{Type: "Config", Config: next})
		}
	}
}

// shardPuller pulls shard data for shards in the Pulling state from their
// previous owners.
func (kv *ShardKV) shardPuller() {
	for !kv.killed() {
		time.Sleep(migrateInterval)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		kv.mu.Lock()
		// Group Pulling shards by their previous-owner GID.
		gidShards := make(map[int][]int)
		for shard, s := range kv.shards {
			if s.State == Pulling {
				gid := kv.prevConfig.Shards[shard]
				gidShards[gid] = append(gidShards[gid], shard)
			}
		}
		configNum := kv.config.Num
		prevGroups := kv.prevConfig.Groups
		kv.mu.Unlock()

		var wg sync.WaitGroup
		for gid, shards := range gidShards {
			servers := prevGroups[gid]
			wg.Add(1)
			go func(servers []string, shards []int) {
				defer wg.Done()
				args := PullShardArgs{ConfigNum: configNum, Shards: shards}
				for _, name := range servers {
					srv := kv.make_end(name)
					var reply PullShardReply
					if srv.Call("ShardKV.PullShard", &args, &reply) && reply.Err == OK {
						kv.rf.Start(Op{
							Type:      "InstallShard",
							ConfigNum: configNum,
							Shards:    shards,
							ShardData: reply.Data,
							Dedup:     reply.Dedup,
						})
						return
					}
				}
			}(servers, shards)
		}
		wg.Wait()
	}
}

// shardGC tells previous owners to delete shards we've fully pulled (GCing),
// then transitions them to Serving.
func (kv *ShardKV) shardGC() {
	for !kv.killed() {
		time.Sleep(migrateInterval)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		kv.mu.Lock()
		gidShards := make(map[int][]int)
		for shard, s := range kv.shards {
			if s.State == GCing {
				gid := kv.prevConfig.Shards[shard]
				gidShards[gid] = append(gidShards[gid], shard)
			}
		}
		configNum := kv.config.Num
		prevGroups := kv.prevConfig.Groups
		kv.mu.Unlock()

		var wg sync.WaitGroup
		for gid, shards := range gidShards {
			servers := prevGroups[gid]
			wg.Add(1)
			go func(servers []string, shards []int) {
				defer wg.Done()
				args := DeleteShardArgs{ConfigNum: configNum, Shards: shards}
				for _, name := range servers {
					srv := kv.make_end(name)
					var reply DeleteShardReply
					if srv.Call("ShardKV.DeleteShard", &args, &reply) && reply.Err == OK {
						kv.rf.Start(Op{Type: "DeleteShard", ConfigNum: configNum, Shards: shards})
						return
					}
				}
			}(servers, shards)
		}
		wg.Wait()
	}
}

// emptyLogChecker commits a no-op entry if the leader has no entry from its
// current term yet, so that commitIndex can advance (needed for liveness after
// leader change).
func (kv *ShardKV) emptyLogChecker() {
	for !kv.killed() {
		time.Sleep(pollInterval)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		if !kv.rf.HasCurrentTermLog() {
			kv.rf.Start(Op{Type: "Empty"})
		}
	}
}

// ---------------- migration RPC handlers ----------------

// PullShard serves a request from a new owner for the data of some shards.
func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum > kv.config.Num {
		// We haven't caught up to the requester's config; tell it to retry.
		reply.Err = ErrNotReady
		return
	}

	reply.Data = make(map[int]map[string]string)
	for _, shard := range args.Shards {
		if s, ok := kv.shards[shard]; ok {
			reply.Data[shard] = s.copyData()
		} else {
			reply.Data[shard] = make(map[string]string)
		}
	}
	reply.Dedup = make(map[int64]int64, len(kv.dedup))
	for cid, seq := range kv.dedup {
		reply.Dedup[cid] = seq
	}
	reply.ConfigNum = args.ConfigNum
	reply.Err = OK
}

// DeleteShard lets the previous owner drop shards after they've been pulled.
func (kv *ShardKV) DeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if args.ConfigNum < kv.config.Num {
		// Already advanced past this; the shard is gone, treat as success.
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrNotReady
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// Replicate the deletion so every replica in this (the previous-owner)
	// group drops the shard consistently. DeleteShard carries no client
	// identity, so a committed entry (anything but a leadership/timeout miss)
	// means the shard is gone — report OK so the caller stops retrying.
	res := kv.submit(Op{Type: "DeleteShard", ConfigNum: args.ConfigNum, Shards: args.Shards})
	if res.err == ErrWrongLeader || res.err == ErrTimeout {
		reply.Err = res.err
	} else {
		reply.Err = OK
	}
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	kv.mck = shardctrler.MakeClerk(ctrlers)
	kv.shards = make(map[int]*Shard)
	kv.dedup = make(map[int64]int64)
	kv.waiters = make(map[int]chan result)
	kv.config = shardctrler.Config{Groups: map[int][]string{}}
	kv.prevConfig = shardctrler.Config{Groups: map[int][]string{}}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applyLoop()
	go kv.configPoller()
	go kv.shardPuller()
	go kv.shardGC()
	go kv.emptyLogChecker()

	return kv
}
