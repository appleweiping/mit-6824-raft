package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// lastOp records the most recent mutating request handled for a client, so a
// retransmitted Put/Append is applied at most once. We keep only the latest
// sequence number (and, for Append, the value returned) per client; older
// entries are overwritten, keeping memory bounded.
type lastOp struct {
	seq   int64
	reply string // for Append: the value the key held before the append
}

type KVServer struct {
	mu sync.Mutex

	data map[string]string
	// dedup maps clientId -> its most recent applied request.
	dedup map[int64]lastOp
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if last, ok := kv.dedup[args.ClientId]; ok && last.seq >= args.Seq {
		// Duplicate: Put returns nothing, so no cached value is needed.
		return
	}
	kv.data[args.Key] = args.Value
	// Record this request. Put has no meaningful reply value, so store "".
	kv.dedup[args.ClientId] = lastOp{seq: args.Seq}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if last, ok := kv.dedup[args.ClientId]; ok && last.seq >= args.Seq {
		// Duplicate request: return the previously computed old value without
		// appending again.
		reply.Value = last.reply
		return
	}
	old := kv.data[args.Key]
	kv.data[args.Key] = old + args.Value
	reply.Value = old
	// Overwrite the previous cached reply for this client, freeing its memory.
	kv.dedup[args.ClientId] = lastOp{seq: args.Seq, reply: old}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.dedup = make(map[int64]lastOp)
	return kv
}
