package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// ClientId + Seq identify a request uniquely so the server can detect and
	// suppress duplicate Put/Append caused by client retries over the
	// unreliable network.
	ClientId int64
	Seq      int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// Get is idempotent, so it needs no de-duplication fields.
}

type GetReply struct {
	Value string
}
