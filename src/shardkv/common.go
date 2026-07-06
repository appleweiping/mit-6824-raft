package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//

import "6.5840/shardctrler"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrNotReady    = "ErrNotReady" // config not yet caught up; client should retry
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
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

// PullShardArgs requests the data for a set of shards at a given config number.
type PullShardArgs struct {
	ConfigNum int
	Shards    []int
}

type PullShardReply struct {
	Err       Err
	ConfigNum int
	Data      map[int]map[string]string // shard -> (key -> value)
	Dedup     map[int64]int64           // clientId -> last applied seq, for the moved shards
}

// DeleteShardArgs tells the previous owner it may garbage-collect shards it has
// already handed off (challenge 1).
type DeleteShardArgs struct {
	ConfigNum int
	Shards    []int
}

type DeleteShardReply struct {
	Err Err
}

func maxSeq(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// keys iterate shardctrler.NShards
const NShards = shardctrler.NShards
