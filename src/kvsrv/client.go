package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "sync/atomic"

type Clerk struct {
	server *labrpc.ClientEnd
	// clientId uniquely identifies this Clerk; seq is a per-client monotonic
	// counter so the server can de-duplicate retried Put/Append RPCs.
	clientId int64
	seq      int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clientId = nrand()
	return ck
}

// nextSeq returns a fresh, strictly increasing sequence number for this Clerk.
func (ck *Clerk) nextSeq() int64 {
	return atomic.AddInt64(&ck.seq, 1)
}

// Get fetches the current value for a key, returning "" if the key is absent.
// It retries forever until it gets a reply, because the network may drop RPCs.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	for {
		reply := GetReply{}
		if ck.server.Call("KVServer.Get", &args, &reply) {
			return reply.Value
		}
		// RPC failed (dropped request or reply); retry.
	}
}

// PutAppend sends a Put or Append and retries until it receives a reply. Every
// retry carries the same (clientId, seq) so the server applies the mutation at
// most once. For Append the server returns the value the key held *before* the
// append.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		ClientId: ck.clientId,
		Seq:      ck.nextSeq(),
	}
	for {
		reply := PutAppendReply{}
		if ck.server.Call("KVServer."+op, &args, &reply) {
			return reply.Value
		}
		// RPC failed; retry with the same Seq so a duplicate is detected.
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
