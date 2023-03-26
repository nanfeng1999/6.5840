package kvraft

import (
	"log"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const ExecuteTimeout = 500 * time.Millisecond

const (
	PUT    = "PUT"
	GET    = "GET"
	APPEND = "APPEND"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	TimeOut        = "Timeout"
)

type Err string

// PUT or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "PUT" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int64 // 客户端请求Id
	ClientId  int64 // 访问的客户端Id
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int64 // 客户端请求Id
	ClientId  int64 // 访问的客户端Id
}

type GetReply struct {
	Err   Err
	Value string
}

type CommonArgs struct {
	RequestId int64
	ClientId  int64
}

type CommonReply struct {
	Err   Err
	Value string
}

type ReplyContext struct {
	LastRequestId int64
	Reply         CommonReply
}
