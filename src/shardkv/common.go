package shardkv

import (
	"6.5840/shardctrler"
	"crypto/rand"
	"log"
	"math/big"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const ExecuteTimeout = 500 * time.Millisecond

const (
	Put    = "Put"
	Get    = "Get"
	Append = "Append"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	TimeOut        = "Timeout"
	OutDateConfig  = "OutDateConfig"
)

const (
	RequestConfigTime = 100 * time.Millisecond
	MonitorInsertTime = 150 * time.Millisecond
	MonitorGCTime     = 50 * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RequestId int64 // 客户端请求Id
	ClientId  int64 // 访问的客户端Id
	Gid       int   // 请求的复制组Id
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	RequestId int64 // 客户端请求Id
	ClientId  int64 // 访问的客户端Id
	Gid       int   // 请求的复制组Id
}

type GetReply struct {
	Err   Err
	Value string
}

type GetShardArgs struct {
	Gid       int   // 复制组ID
	ShardIDs  []int // 需要获取的分片数组ID
	ConfigNum int   // 当前配置号
}

type GetShardReply struct {
	Err            Err
	ConfigNum      int                    // 当前配置号
	Shards         map[int]Shard          // 获取的分片数据
	LastRequestMap map[int64]ReplyContext // 请求Map 发送分片的时候也发送这个过去 更新对面复制组的map 用于去重
}

type DeleteShardArgs struct {
	ShardIDs  []int // 需要删除的分片数组ID
	ConfigNum int   // 当前配置号
}

type DeleteShardReply struct {
	Err Err
}

type OperationArgs struct {
	Key       string
	Value     string
	OpType    string // "Put" or "Append" or "Get"
	RequestId int64  // 客户端请求Id
	ClientId  int64  // 访问的客户端Id
	Gid       int    // 请求的复制组Id
}

type CommonReply struct {
	Err   Err
	Value string
}

type ReplyContext struct {
	LastRequestId int64
	Reply         CommonReply
}

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

func copyMap(oldMap map[int64]ReplyContext) map[int64]ReplyContext {
	newMap := make(map[int64]ReplyContext)
	for clientId, cxt := range oldMap {
		newMap[clientId] = cxt
	}

	return newMap
}
