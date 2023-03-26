package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.5840/labrpc"
	"time"
)
import "6.5840/shardctrler"

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIds map[int]int // 记录每个分组的leader ID号
	clientId  int64       // 本客户端的Id号 用于去重和标识客户端
	requestId int64       // 本次请求的Id号 用于去重和标识请求
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leaderIds = make(map[int]int)

	ck.config = ck.sm.Query(-1) // 请求最新的配置信息
	DPrintf("client {%d} init config = %v\n", ck.clientId, ck.config)
	return ck
}

func (ck *Clerk) Operation(args *OperationArgs) string {
	args.ClientId, args.RequestId = ck.clientId, ck.requestId

	sid := key2shard(args.Key)
	DPrintf("client {%d} send operation = %v\n", ck.clientId, args)
	for {
		gid := ck.config.Shards[sid] // 分片属于哪个复制组
		oldLeaderId := ck.leaderIds[gid]

		// 配置中本组对应的服务器是否存在 如果最初初始化的时候是不存在的
		if servers, ok := ck.config.Groups[gid]; ok {
			for {
				var reply CommonReply
				leaderId := ck.leaderIds[gid] // 取出复制组对应的ID号
				if ok := ck.make_end(servers[leaderId]).Call("ShardKV.Operation", args, &reply); !ok || reply.Err == TimeOut || reply.Err == ErrWrongLeader || reply.Err == OutDateConfig {
					ck.leaderIds[gid] = (ck.leaderIds[gid] + 1) % len(servers)
					// 有可能执行到这里的时候最新配置更新了 分片不再属于该复制组 这个时候如果不更新配置会陷入死循环
					// 所以当执行了一遍 发现全部都不可以成功返回的时候 需要重新更新一下配置
					if ck.leaderIds[gid] == oldLeaderId {
						break
					}
					continue
				} else if reply.Err == ErrWrongGroup {
					// 如果表示组错误 说明正在发生分片迁移 退出本循环 重新更新配置
					DPrintf("client {%d} get group err\n", ck.clientId)
					break
				} else {
					DPrintf("client {%d} get right reply = %v\n", ck.clientId, reply)
					ck.requestId += 1
					return reply.Value
				}
			}

		}

		ck.config = ck.sm.Query(-1)
		DPrintf("client {%d} update config = %v\n", ck.clientId, ck.config)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.Operation(&OperationArgs{Key: key, OpType: Get})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Operation(&OperationArgs{Key: key, Value: value, OpType: Put})
}

func (ck *Clerk) Append(key string, value string) {
	ck.Operation(&OperationArgs{Key: key, Value: value, OpType: Append})
}
