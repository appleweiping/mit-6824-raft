package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// ClientId + Seq give each client request a unique identity so the servers
	// can apply it exactly once even across leader changes and retries.
	ClientId int64
	Seq      int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientId int64
	Seq      int64
}

type GetReply struct {
	Err   Err
	Value string
}
