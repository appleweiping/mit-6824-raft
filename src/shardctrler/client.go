package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync/atomic"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// clientId + seq de-duplicate retries; leaderHint caches the last leader.
	clientId   int64
	seq        int64
	leaderHint int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) nextSeq() int64 {
	return atomic.AddInt64(&ck.seq, 1)
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{Num: num, ClientId: ck.clientId, Seq: ck.nextSeq()}
	leader := int(atomic.LoadInt32(&ck.leaderHint))
	for {
		var reply QueryReply
		ok := ck.servers[leader].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			atomic.StoreInt32(&ck.leaderHint, int32(leader))
			return reply.Config
		}
		leader = (leader + 1) % len(ck.servers)
		if leader == int(atomic.LoadInt32(&ck.leaderHint)) {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Servers: servers, ClientId: ck.clientId, Seq: ck.nextSeq()}
	leader := int(atomic.LoadInt32(&ck.leaderHint))
	for {
		var reply JoinReply
		ok := ck.servers[leader].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			atomic.StoreInt32(&ck.leaderHint, int32(leader))
			return
		}
		leader = (leader + 1) % len(ck.servers)
		if leader == int(atomic.LoadInt32(&ck.leaderHint)) {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{GIDs: gids, ClientId: ck.clientId, Seq: ck.nextSeq()}
	leader := int(atomic.LoadInt32(&ck.leaderHint))
	for {
		var reply LeaveReply
		ok := ck.servers[leader].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			atomic.StoreInt32(&ck.leaderHint, int32(leader))
			return
		}
		leader = (leader + 1) % len(ck.servers)
		if leader == int(atomic.LoadInt32(&ck.leaderHint)) {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{Shard: shard, GID: gid, ClientId: ck.clientId, Seq: ck.nextSeq()}
	leader := int(atomic.LoadInt32(&ck.leaderHint))
	for {
		var reply MoveReply
		ok := ck.servers[leader].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			atomic.StoreInt32(&ck.leaderHint, int32(leader))
			return
		}
		leader = (leader + 1) % len(ck.servers)
		if leader == int(atomic.LoadInt32(&ck.leaderHint)) {
			time.Sleep(100 * time.Millisecond)
		}
	}
}
