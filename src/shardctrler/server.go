package shardctrler

import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "sync/atomic"
import "6.5840/labgob"
import "sort"
import "time"

const applyTimeout = 500 * time.Millisecond

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	configs []Config // indexed by config num

	dedup       map[int64]int64     // clientId -> last applied seq
	waiters     map[int]chan Op     // log index -> waiter channel
	lastApplied int
}

// Op is the command replicated through Raft.
type Op struct {
	Type     string // "Join", "Leave", "Move", "Query"
	Servers  map[int][]string
	GIDs     []int
	Shard    int
	GID      int
	Num      int
	ClientId int64
	Seq      int64
}

// submit replicates op through Raft and waits until it is applied. Returns
// false if this server is not the leader or the wait timed out.
func (sc *ShardCtrler) submit(op Op) bool {
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return false
	}
	sc.mu.Lock()
	ch := make(chan Op, 1)
	sc.waiters[index] = ch
	sc.mu.Unlock()

	select {
	case applied := <-ch:
		sc.mu.Lock()
		delete(sc.waiters, index)
		sc.mu.Unlock()
		return applied.ClientId == op.ClientId && applied.Seq == op.Seq
	case <-time.After(applyTimeout):
		sc.mu.Lock()
		delete(sc.waiters, index)
		sc.mu.Unlock()
		return false
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	ok := sc.submit(Op{Type: "Join", Servers: args.Servers, ClientId: args.ClientId, Seq: args.Seq})
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	ok := sc.submit(Op{Type: "Leave", GIDs: args.GIDs, ClientId: args.ClientId, Seq: args.Seq})
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	ok := sc.submit(Op{Type: "Move", Shard: args.Shard, GID: args.GID, ClientId: args.ClientId, Seq: args.Seq})
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	if !sc.submit(Op{Type: "Query", Num: args.Num, ClientId: args.ClientId, Seq: args.Seq}) {
		reply.WrongLeader = true
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	reply.Err = OK
	reply.Config = sc.configAt(args.Num)
}

// configAt returns config number num, or the latest if num is -1 or out of
// range. Caller holds sc.mu.
func (sc *ShardCtrler) configAt(num int) Config {
	if num < 0 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[num]
}

// applyLoop consumes committed entries and applies them in order.
func (sc *ShardCtrler) applyLoop() {
	for msg := range sc.applyCh {
		if sc.killed() {
			return
		}
		if !msg.CommandValid {
			continue
		}
		sc.mu.Lock()
		if msg.CommandIndex <= sc.lastApplied {
			sc.mu.Unlock()
			continue
		}
		sc.lastApplied = msg.CommandIndex
		op := msg.Command.(Op)

		// Apply mutating ops once per (client, seq). Query is read-only.
		if op.Type != "Query" {
			if last, ok := sc.dedup[op.ClientId]; !ok || op.Seq > last {
				sc.applyMutation(op)
				sc.dedup[op.ClientId] = op.Seq
			}
		}

		if ch, ok := sc.waiters[msg.CommandIndex]; ok {
			ch <- op
		}
		sc.mu.Unlock()
	}
}

// applyMutation produces a new Config from the current one. Caller holds sc.mu.
func (sc *ShardCtrler) applyMutation(op Op) {
	last := sc.configs[len(sc.configs)-1]
	nc := Config{
		Num:    last.Num + 1,
		Shards: last.Shards,
		Groups: copyGroups(last.Groups),
	}

	switch op.Type {
	case "Join":
		for gid, servers := range op.Servers {
			nc.Groups[gid] = append([]string(nil), servers...)
		}
		rebalance(&nc)
	case "Leave":
		for _, gid := range op.GIDs {
			delete(nc.Groups, gid)
		}
		// Unassign shards owned by departed groups.
		leaving := make(map[int]bool)
		for _, gid := range op.GIDs {
			leaving[gid] = true
		}
		for s := range nc.Shards {
			if leaving[nc.Shards[s]] {
				nc.Shards[s] = 0
			}
		}
		rebalance(&nc)
	case "Move":
		nc.Shards[op.Shard] = op.GID
	}

	sc.configs = append(sc.configs, nc)
}

func copyGroups(g map[int][]string) map[int][]string {
	out := make(map[int][]string, len(g))
	for gid, servers := range g {
		out[gid] = append([]string(nil), servers...)
	}
	return out
}

// rebalance deterministically assigns shards to groups so the load differs by
// at most one shard between any two groups, moving as few shards as possible.
// Because every replica runs this on the same input, the result is identical
// across the group.
func rebalance(c *Config) {
	gids := make([]int, 0, len(c.Groups))
	for gid := range c.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	n := len(gids)
	if n == 0 {
		// No groups: all shards go to the invalid group 0.
		for s := range c.Shards {
			c.Shards[s] = 0
		}
		return
	}

	// Target load per group: the first (NShards % n) groups get one extra.
	base := NShards / n
	extra := NShards % n
	target := make(map[int]int, n)
	for i, gid := range gids {
		if i < extra {
			target[gid] = base + 1
		} else {
			target[gid] = base
		}
	}

	// Count current assignments (treating group 0 / unknown as unassigned).
	count := make(map[int]int, n)
	valid := make(map[int]bool, n)
	for _, gid := range gids {
		valid[gid] = true
	}
	var unassigned []int // shard indices that need a home
	for s := 0; s < NShards; s++ {
		g := c.Shards[s]
		if g != 0 && valid[g] {
			count[g]++
		} else {
			unassigned = append(unassigned, s)
		}
	}

	// Take shards away from over-loaded groups. Iterate groups in sorted order
	// and, within a group, remove the highest-numbered shards first so the
	// outcome is deterministic.
	for _, gid := range gids {
		for count[gid] > target[gid] {
			// find one shard currently owned by gid to release
			for s := NShards - 1; s >= 0; s-- {
				if c.Shards[s] == gid {
					c.Shards[s] = 0
					unassigned = append(unassigned, s)
					count[gid]--
					break
				}
			}
		}
	}
	sort.Ints(unassigned)

	// Hand unassigned shards to under-loaded groups, in sorted GID order.
	ui := 0
	for _, gid := range gids {
		for count[gid] < target[gid] && ui < len(unassigned) {
			c.Shards[unassigned[ui]] = gid
			ui++
			count[gid]++
		}
	}
}

func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.dedup = make(map[int64]int64)
	sc.waiters = make(map[int]chan Op)

	go sc.applyLoop()

	return sc
}
