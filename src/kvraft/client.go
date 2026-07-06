package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// clientId identifies this Clerk; seq gives each request a unique, strictly
	// increasing sequence number. leaderHint caches the last known leader to
	// avoid re-scanning all servers on every call.
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

// Get fetches the current value for a key ("" if absent). It retries across
// servers until a leader answers.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, ClientId: ck.clientId, Seq: ck.nextSeq()}
	leader := int(atomic.LoadInt32(&ck.leaderHint))
	for {
		reply := GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			atomic.StoreInt32(&ck.leaderHint, int32(leader))
			return reply.Value
		}
		// Wrong leader, timeout, or dropped RPC: advance to the next server.
		leader = (leader + 1) % len(ck.servers)
	}
}

// PutAppend sends a Put or Append, retrying across servers until a leader
// commits it. The stable (clientId, seq) makes retries idempotent.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		Seq:      ck.nextSeq(),
	}
	leader := int(atomic.LoadInt32(&ck.leaderHint))
	for {
		reply := PutAppendReply{}
		ok := ck.servers[leader].Call("KVServer."+op, &args, &reply)
		if ok && reply.Err == OK {
			atomic.StoreInt32(&ck.leaderHint, int32(leader))
			return
		}
		leader = (leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
