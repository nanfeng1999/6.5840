package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64  // 客户端Id
	RequestId int64  // 请求Id
	OpType    string // 操作类型PutAppend/Get
	Key       string // 添加的Key
	Value     string // 添加的Value
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	// Your definitions here.
	mck            *shardctrler.Clerk        // 控制器对应的客户端
	currentConfig  shardctrler.Config        // 本分组保存的最新的配置
	lastConfig     shardctrler.Config        // 本分组保存的上一个的配置
	shards         map[int]*Shard            // 状态机 记录kv key是分片sid value是分片数据
	notifyChanMap  map[int]chan *CommonReply // 通知chan key = index val = chan
	lastRequestMap map[int64]ReplyContext    // 缓存每个客户端对应的最近请求和reply key = clientId
	persist        *raft.Persister           // 持久化
	lastApplied    int                       // 最后被应用到kv中的command Index
}

func (kv *ShardKV) Operation(args *OperationArgs, reply *CommonReply) {
	// Your code here.
	kv.mu.Lock()

	DPrintf("group = {%d} kv {%d} receive operation args = %v\n", kv.gid, kv.me, args)
	// 这个请求可能是延迟请求 此时本复制组已经不负责该key对应的分片了
	sid := key2shard(args.Key)
	if !kv.canServe(sid) {
		DPrintf("group = {%d} kv {%d} can not serve the key = %s sid = %d\n", kv.gid, kv.me, args.Key, sid)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// 判断是否是重复请求 幂等操作Get不需要判断
	if args.OpType != Get && kv.isOldRequest(args.ClientId, args.RequestId) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 放入日志队列进行同步并更新返回结果
	kv.executeCommand(NewOperationCommand(*args), reply)
}

func (kv *ShardKV) getSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.lastRequestMap) // 这个也需要持久化，因为服务端崩溃重启 客户端可能还没有感知到 如果这个不保存 那么会出现重复命令的可能
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	kvstate := w.Bytes()
	return kvstate
}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shards map[int]*Shard
	var lastRequestMap map[int64]ReplyContext
	var lastConfig shardctrler.Config
	var currentConfig shardctrler.Config

	if d.Decode(&shards) != nil ||
		d.Decode(&lastRequestMap) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&currentConfig) != nil {
		panic("decode persist state fail")
	}

	kv.shards = shards
	kv.lastRequestMap = lastRequestMap
	kv.lastConfig = lastConfig
	kv.currentConfig = currentConfig
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.currentConfig = shardctrler.NewConfig()
	kv.lastConfig = shardctrler.NewConfig()
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.shards = make(map[int]*Shard)
	// 初始化的时候需要都赋予一个最初的分片状态
	// 不然后续的conServe函数永远不会被执行
	kv.notifyChanMap = make(map[int]chan *CommonReply)
	kv.lastRequestMap = make(map[int64]ReplyContext)
	kv.persist = persister

	labgob.Register(Command{})
	labgob.Register(OperationArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(GetShardReply{})
	labgob.Register(DeleteShardArgs{})
	labgob.Register(CommonReply{})
	labgob.Register(ReplyContext{})

	kv.restoreSnapshot(persister.ReadSnapshot())

	// 如果接收到空快照的话 本地shard保持原状不变
	for i := 0; i < shardctrler.NShards; i++ {
		if _, ok := kv.shards[i]; !ok {
			kv.shards[i] = NewShard(Serving)
		}
	}

	go kv.applier()
	go kv.requestLastConfig()
	go kv.monitorInsert()
	go kv.monitorGC()

	return kv
}

func (kv *ShardKV) applier() {
	for kv.killed() == false {
		select {
		case applyMsg := <-kv.applyCh:
			kv.mu.Lock()
			if applyMsg.CommandValid {
				if applyMsg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				reply := kv.apply(applyMsg.Command)
				currentTerm, isLeader := kv.rf.GetState()
				// 假设总共有五个节点 leader接收到新消息 返回index1 之后发生网络延迟 然后其他节点选举出了新的leader
				// 新leader接收到第二个新消息 返回index2 这个时候index1 等于 index2 并且成功把消息同步给其他所有的节点
				// 然后这个时候新leader又发生网络延迟 之前的节点重新当选leader并开始应用日志 如果不加入任期的判断限制的话
				// 这里的通知就会出现错误
				if isLeader && applyMsg.CommandTerm == currentTerm {
					kv.notify(applyMsg.CommandIndex, reply)
				}

				// 如果日志队列超出了最大的容量 需要做一份快照
				if kv.maxraftstate != -1 && kv.persist.RaftStateSize() > kv.maxraftstate {
					kv.rf.Snapshot(applyMsg.CommandIndex, kv.getSnapshot())
				}

				kv.lastApplied = applyMsg.CommandIndex
			} else if applyMsg.SnapshotValid {
				if applyMsg.SnapshotIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				kv.restoreSnapshot(applyMsg.Snapshot)
				kv.lastApplied = applyMsg.SnapshotIndex
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) apply(cmd interface{}) *CommonReply {
	reply := &CommonReply{}
	command := cmd.(Command)

	DPrintf("group {%d} kv {%d} apply command = %v\n", kv.gid, kv.me, command)
	switch command.Type {
	case Operation:
		op := command.Data.(OperationArgs)
		reply = kv.applyOperation(&op)
	case AddConfig:
		cfg := command.Data.(shardctrler.Config)
		reply = kv.applyConfig(cfg)
	case InsertShard:
		response := command.Data.(GetShardReply)
		reply = kv.applyInsertShard(&response)
	case DeleteShard:
		args := command.Data.(DeleteShardArgs)
		reply = kv.applyDeleteShard(&args)
	}

	DPrintf("group {%d} kv {%d} return reply = %v\n", kv.gid, kv.me, reply)
	return reply
}

func (kv *ShardKV) applyOperation(op *OperationArgs) *CommonReply {
	var reply = &CommonReply{}

	sid := key2shard(op.Key) // 计算出key归属于哪个分片
	// 应用操作符之前需要判断对应分片能否为当前key服务
	// 有可能准备应用的时候有新的配置到来 修改了分片归属的复制组和分片的状态
	if !kv.canServe(sid) {
		reply.Err = ErrWrongGroup
		return reply
	}

	// 如果是Get请求 或者 不是重复请求 那么需要应用
	if op.OpType == Get || !kv.isOldRequest(op.ClientId, op.RequestId) {
		// 根据操作类型判断执行具体的操作
		switch op.OpType {
		case Get:
			reply.Value = kv.shards[sid].get(op.Key)
		case Put:
			kv.shards[sid].put(op.Key, op.Value)
		case Append:
			kv.shards[sid].appendVal(op.Key, op.Value)
		}

		// 不是Get请求需要更新最后一次请求的记录
		if op.OpType != Get {
			kv.updateLastRequest(op, reply)
		}
	}

	reply.Err = OK
	return reply
}

func (kv *ShardKV) applyConfig(newCfg shardctrler.Config) *CommonReply {
	// 需要判断配置号是否正确 有可能应用了旧配置
	if newCfg.Num == kv.currentConfig.Num+1 {
		kv.updateShardState(newCfg)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = newCfg
		return &CommonReply{Err: OK}
	} else {
		// 配置的更改是没有lastRequestMap这种去重结构的 那么存在延迟的旧配置到达的情况
		// 这种情况下需要丢弃配置
		DPrintf("group {%d} kv {%d} receive old config,config num = %d\n", kv.gid, kv.me, newCfg.Num)
		return &CommonReply{Err: OutDateConfig}
	}
}

func (kv *ShardKV) applyInsertShard(response *GetShardReply) *CommonReply {
	if response.ConfigNum == kv.currentConfig.Num {
		shards := response.Shards
		//DPrintf("group {%d} kv {%d} update shards = %v\n", kv.gid, kv.me, printShards(kv.shards))

		for sid, shard := range shards {
			// 这里也需要深拷贝 这个BUG找了半天
			oldShard := kv.shards[sid]
			if oldShard.State == Pulling {
				for k, v := range shard.Data {
					oldShard.Data[k] = v
				}
				oldShard.State = GCing
			}

		}

		// 更新请求Map
		// 想象这么一种情况 如果客户端发送一条Append指令 因为网络原因返回了false 但是实际上服务器已经执行了
		// 这个时候配置修改 需要迁移分片 分片迁移完毕之后 客户端重发Append指令到新的复制组
		// 如果新的复制组中没有过滤重复请求会导致Append执行两次
		for clientId, cxt := range response.LastRequestMap {
			if oldCtx, ok := kv.lastRequestMap[clientId]; ok {
				if oldCtx.LastRequestId < cxt.LastRequestId {
					kv.lastRequestMap[clientId] = cxt
				}
			} else {
				kv.lastRequestMap[clientId] = cxt
			}
		}

		DPrintf("group {%d} kv {%d} update shards = %v\n", kv.gid, kv.me, printShards(kv.shards))
		return &CommonReply{Err: OK}
	} else {
		DPrintf("group {%d} kv {%d} receive old insert shard request,config num = %d\n", kv.gid, kv.me, response.ConfigNum)
		return &CommonReply{Err: OutDateConfig}
	}
}

func (kv *ShardKV) applyDeleteShard(args *DeleteShardArgs) *CommonReply {
	if args.ConfigNum == kv.currentConfig.Num {
		for _, sid := range args.ShardIDs {
			if kv.hasShard(sid) && kv.getShard(sid).State == BePulling {
				kv.shards[sid] = NewShard(Serving)
			}
		}
		return &CommonReply{Err: OK}
	} else {
		DPrintf("group {%d} kv {%d} receive old delete shard request,old config num = %d new config num = %d\n", kv.gid, kv.me, args.ConfigNum, kv.currentConfig.Num)
		return &CommonReply{Err: OK}
	}
}

func (kv *ShardKV) updateShardState(newConfig shardctrler.Config) {
	for i := 0; i < shardctrler.NShards; i++ {
		// 新的配置中i号分片分给本组 原有配置中不分 那么该分片是需要新增的分片
		if newConfig.Shards[i] == kv.gid && kv.currentConfig.Shards[i] != kv.gid {
			// 不能从0号复制组拉取分片 0号复制组是无效复制组 最开始初始化的时候 会发生0号复制组分片分给其他复制组的情况
			// [0,0,0,0] --> [1,1,1,1] kv.gid = 1
			if kv.currentConfig.Shards[i] != 0 {
				kv.shards[i].State = Pulling // 从其他复制组拉分片
			}
		}

		// 新的配置中i号分片不分给本组 原有配置中分 那么该分片是需要删除的分片
		if newConfig.Shards[i] != kv.gid && kv.currentConfig.Shards[i] == kv.gid {
			// [1,1,1,1] --> [0,0,0,0] 存在将复制组的信息全部删除的可能
			if newConfig.Shards[i] != 0 {
				kv.shards[i].State = BePulling // 被拉取分片
			}
		}

	}
}

// 判断当前复制组是否能够服务该key
func (kv *ShardKV) canServe(sid int) bool {
	if shard, ok := kv.shards[sid]; ok {
		DPrintf("group {%d} kv {%d} sid = %d shard = %v currentConfig = %v\n", kv.gid, kv.me, sid, kv.shards[sid], kv.currentConfig)
		// key对应的分片 归本分组管理 同时该分片的状态是可服务状态 或 垃圾回收状态
		if kv.currentConfig.Shards[sid] == kv.gid && (shard.State == Serving || shard.State == GCing) {
			return true
		}
	}

	return false
}

func (kv *ShardKV) notify(index int, reply *CommonReply) {
	if notifyCh, ok := kv.notifyChanMap[index]; ok {
		notifyCh <- reply
	}
}

func (kv *ShardKV) isOldRequest(clientId int64, requestId int64) bool {
	if cxt, ok := kv.lastRequestMap[clientId]; ok {
		if requestId <= cxt.LastRequestId {
			return true
		}
	}

	return false
}

func (kv *ShardKV) updateLastRequest(op *OperationArgs, reply *CommonReply) {
	ctx := ReplyContext{
		LastRequestId: op.RequestId,
		Reply:         *reply,
	}

	lastCtx, ok := kv.lastRequestMap[op.ClientId]
	if (ok && lastCtx.LastRequestId < op.RequestId) || !ok {
		kv.lastRequestMap[op.ClientId] = ctx
	}
}

func (kv *ShardKV) executeCommand(cmd Command, reply *CommonReply) {
	// 判断是否是leader节点
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 构造通知 chan
	kv.mu.Lock()
	kv.notifyChanMap[index] = make(chan *CommonReply, 1)
	notifyChan := kv.notifyChanMap[index]
	kv.mu.Unlock()

	// 通知到来 日志应用成功 或 超时直接返回
	select {
	case ret := <-notifyChan:
		reply.Err, reply.Value = ret.Err, ret.Value
		DPrintf("group {%d} kv {%d} reply = %v\n", kv.gid, kv.me, reply)
		currentTerm, isLeader := kv.rf.GetState()
		// 有可能通知的时候是leader节点 但是这边接受通知的时候就不是了 这里不加判断也行 因为此时已经应用日志成功了
		if !isLeader || currentTerm != term {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = TimeOut
	}

	// 删除通知chan 释放内存
	kv.mu.Lock()
	delete(kv.notifyChanMap, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) requestLastConfig() {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(RequestConfigTime)
			continue
		}

		kv.mu.Lock()
		isLastConfigDone := true

		// 只有等待上一个配置全部执行完毕才能够进入下一个配置
		for _, shard := range kv.shards {
			if shard.State != Serving {
				isLastConfigDone = false
				break
			}
		}
		currentConfigNum := kv.currentConfig.Num
		kv.mu.Unlock()

		if isLastConfigDone {
			if cfg := kv.mck.Query(currentConfigNum + 1); cfg.Num == currentConfigNum+1 {
				// 如果是下一个配置 那么把这个配置同步给其他节点
				reply := &CommonReply{}
				DPrintf("group {%d} kv {%d} get new config = %v\n", kv.gid, kv.me, cfg)
				kv.executeCommand(NewConfigCommand(cfg), reply)
			}
		}

		time.Sleep(RequestConfigTime)
	}
}

func (kv *ShardKV) monitorInsert() {
	// leader节点每间隔一段时间 判断是否需要向其他分组请求分片
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(MonitorInsertTime)
			continue
		}

		kv.mu.Lock()
		// 如果以分片粒度拉取分片效率低下 应该以复制组的粒度拉取分片
		// 获取需要拉取分片的集合 key = gid ; value = sid数组
		groupShardIDs := kv.getShardIDsByState(Pulling)
		wg := &sync.WaitGroup{}
		wg.Add(len(groupShardIDs))

		if len(groupShardIDs) != 0 {
			DPrintf("group {%d} kv {%d} start get shards groupShardIDs = %v\n", kv.gid, kv.me, groupShardIDs)
		}

		for gid, shardIDs := range groupShardIDs {
			num, servers := kv.currentConfig.Num, kv.lastConfig.Groups[gid]
			// 所有进行通信的地方都需要考虑包延迟的情况 所以这里需要加入配置号
			go func(gid int, configNum int, servers []string, shardIDs []int) {
				defer wg.Done()
				// 遍历寻找正确的leader节点
				for _, server := range servers {
					args, getShardReply := &GetShardArgs{}, &GetShardReply{}
					args.Gid, args.ConfigNum, args.ShardIDs = gid, configNum, shardIDs
					srv := kv.make_end(server)
					// 如果请求的复制组没有选举出leader节点，那么本次请求全部会失败，但是因为分片状态没有改变 所以会再来一次直到成功为止
					if srv.Call("ShardKV.GetShards", args, getShardReply); getShardReply.Err == OK {
						getShardReply.ConfigNum = configNum
						kv.executeCommand(NewInsertShardsCommand(*getShardReply), &CommonReply{})
					}
				}
			}(gid, num, servers, shardIDs)
		}
		kv.mu.Unlock()
		wg.Wait()
		DPrintf("group {%d} kv {%d} get shards end\n", kv.gid, kv.me)

		time.Sleep(MonitorInsertTime)
	}
}

func (kv *ShardKV) GetShards(args *GetShardArgs, reply *GetShardReply) {
	// 只有leader可以回复数据
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 判断配置是否相同 防止过期的信息传递过来导致错误发生
	if args.ConfigNum == kv.currentConfig.Num {
		shards := make(map[int]Shard)
		for _, sid := range args.ShardIDs {
			// 如果分片存在并且正等待被拉取
			// 这里不需要判断是否分片被拉取
			if kv.hasShard(sid) && kv.getShard(sid).State == BePulling {
				// 这里不能够修改BePulling的状态 因为可能发生丢包 其他复制组需要向本复制组再拉取一遍数据
				shards[sid] = kv.getShard(sid).deepCopy()
			}
		}
		reply.Shards, reply.LastRequestMap, reply.Err = shards, copyMap(kv.lastRequestMap), OK
	} else {
		reply.Err = ErrWrongGroup
	}

}

func (kv *ShardKV) monitorGC() {
	// leader节点每间隔一段时间 判断是否需要删除其他分组的分片（本分组向其他分组请求分片，分片接收成功之后需要请求对方释放分片）
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(MonitorGCTime)
			continue
		}

		kv.mu.Lock()
		// 如果以分片粒度拉取分片效率低下 应该以复制组的粒度拉取分片
		// 获取需要拉取分片的集合 key = gid ; value = sid数组
		groupShardIDs := kv.getShardIDsByState(GCing)
		wg := &sync.WaitGroup{}
		wg.Add(len(groupShardIDs))

		// 需要注意默认配置的情况 此时所有分片都分配给0号复制组（无效复制组）
		for gid, shardIDs := range groupShardIDs {
			num, servers := kv.currentConfig.Num, kv.lastConfig.Groups[gid]
			// 所有进行通信的地方都需要考虑包延迟的情况 所以这里需要加入配置号
			go func(gid int, configNum int, servers []string, shardIDs []int) {
				defer wg.Done()
				// 遍历寻找正确的leader节点
				for _, server := range servers {
					args, deleteShardReply := &DeleteShardArgs{}, &DeleteShardReply{}
					args.ConfigNum, args.ShardIDs = configNum, shardIDs

					srv := kv.make_end(server)
					// 如果这里返回false 但是对面收到了 然后分片状态变成了serving 然后更新了配置
					// 这块地方之后重发 但是对面发现是过期的配置 直接丢弃 这样子会陷入死循环
					// 所以对面碰到过期配置也应该返回OK 而不是OutDateConfig
					if srv.Call("ShardKV.DeleteShards", args, deleteShardReply) {
						if deleteShardReply.Err == OK {
							kv.mu.Lock()
							for _, sid := range shardIDs {
								if kv.hasShard(sid) {
									kv.getShard(sid).State = Serving
								}
							}
							kv.mu.Unlock()
						}
					}
				}
			}(gid, num, servers, shardIDs)
		}
		kv.mu.Unlock()
		wg.Wait()
		time.Sleep(MonitorGCTime)
	}
}

func (kv *ShardKV) DeleteShards(args *DeleteShardArgs, reply *DeleteShardReply) {
	commonReply := &CommonReply{}
	kv.executeCommand(NewDeleteShardsCommand(*args), commonReply)
	reply.Err = commonReply.Err
}

func (kv *ShardKV) getShardIDsByState(state ShardState) map[int][]int {
	shard2group := make(map[int][]int)

	// 当前复制组向其他复制组拉取分片 或 当前复制组拉取完毕之后请求其他复制组删除分片
	// 拉取分片需要知道分片原属于哪个复制组 所以使用上一个配置
	// 请求其他复制组删除分片 同理
	for sid, shard := range kv.shards {
		if shard.State == state {
			gid := kv.lastConfig.Shards[sid]
			if _, ok := shard2group[gid]; !ok {
				shard2group[gid] = make([]int, 0)
			}
			shard2group[gid] = append(shard2group[gid], sid)
		}
	}

	DPrintf("group {%d} kv {%d} get state = %s shard2group = %v\n", kv.gid, kv.me, state.String(), shard2group)
	return shard2group
}

func (kv *ShardKV) hasShard(sid int) bool {
	if _, ok := kv.shards[sid]; ok {
		return true
	}

	return false
}

func (kv *ShardKV) getShard(sid int) *Shard {
	return kv.shards[sid]
}
