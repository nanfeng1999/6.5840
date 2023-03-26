package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64  // 客户端Id
	RequestId int64  // 请求Id
	OpType    string // 操作类型PutAppend/GET
	Key       string // 添加的Key
	Value     string // 添加的Value
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// 3A
	stateMachine   *MemoryKV                 // 状态机 记录kv
	notifyChanMap  map[int]chan *CommonReply // 通知chan key = index val = chan
	lastRequestMap map[int64]ReplyContext    // 缓存每个客户端对应的最近请求和reply key = clientId
	// 3B
	persist     *raft.Persister // 持久化
	lastApplied int             // 最后被应用到kv中的command Index
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 构造操作日志
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    GET,
		Key:       args.Key,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("server = {%d} term = %d index = %d get args = %v", kv.me, term, index, args)
	kv.mu.Lock()
	kv.notifyChanMap[index] = make(chan *CommonReply, 1)
	notifyChan := kv.notifyChanMap[index]
	kv.mu.Unlock()

	DPrintf("server = {%d} wait notify or timeout", kv.me)
	select {
	case ret := <-notifyChan:
		DPrintf("server = %d get ret = %v", kv.me, ret)
		reply.Value, reply.Err = ret.Value, ret.Err
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || currentTerm != term {
			DPrintf("server = {%d} notify false node, isLeader = %t currentTerm = %d term = %d", kv.me, isLeader, currentTerm, term)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("server = {%d} timeout", kv.me)
		reply.Err = TimeOut
	}

	kv.mu.Lock()
	delete(kv.notifyChanMap, index)
	kv.mu.Unlock()

	// 不应该在这里保存快照 因为接收到命令的时候 你还不知道是否能够最终同步成功 所以应该在applier中保存快照
	//if kv.persist.RaftStateSize() > kv.maxraftstate {
	//	kv.rf.Snapshot(index, kv.encodeState())
	//}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	if kv.isOldRequest(args.ClientId, args.RequestId) {
		DPrintf("server = {%d} get old request", kv.me)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// 构造操作日志
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("server = {%d} term = %d index = %d get putAppend args = %v", kv.me, term, index, args)
	kv.mu.Lock()
	kv.notifyChanMap[index] = make(chan *CommonReply, 1)
	notifyChan := kv.notifyChanMap[index]
	kv.mu.Unlock()

	DPrintf("server = {%d} wait notify or timeout", kv.me)
	select {
	case ret := <-notifyChan:
		DPrintf("server = {%d} get ret = %v", kv.me, ret)
		reply.Err = ret.Err
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || currentTerm != term {
			DPrintf("server = {%d} notify false node, isLeader = %t currentTerm = %d term = %d", kv.me, isLeader, currentTerm, term)
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		DPrintf("server = {%d} timeout", kv.me)
		reply.Err = TimeOut
	}

	kv.mu.Lock()
	delete(kv.notifyChanMap, index)
	kv.mu.Unlock()
}

func (kv *KVServer) getSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastRequestMap) // 这个也需要持久化，因为服务端崩溃重启 客户端可能还没有感知到 如果这个不保存 那么会出现重复命令的可能
	kvstate := w.Bytes()
	return kvstate
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var stateMachine MemoryKV
	var lastRequestMap map[int64]ReplyContext

	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastRequestMap) != nil {
		panic("decode persist state fail")
	}

	kv.stateMachine = &stateMachine
	kv.lastRequestMap = lastRequestMap
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.stateMachine = NewMemoryKV()
	kv.notifyChanMap = make(map[int]chan *CommonReply)
	kv.lastRequestMap = make(map[int64]ReplyContext)
	kv.persist = persister

	kv.restoreSnapshot(persister.ReadSnapshot())

	go kv.applier()
	DPrintf("server = %d start", kv.me)
	return kv
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case applyMsg := <-kv.applyCh:
			DPrintf("server {%d} receive applyMsg = %v", kv.me, applyMsg)
			kv.mu.Lock()
			if applyMsg.CommandValid {
				if applyMsg.CommandIndex <= kv.lastApplied {
					DPrintf("server {%d} drop command index = %d lastApplied = %d", kv.me, applyMsg.CommandIndex, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				// 假设本节点因为网络原因落后于其他的节点 其他节点到日志条目数为n1时做了一个快照 同时这个节点是leader节点
				// 本节点请求日志的时候发现leader节点没有需要的日志（变成快照后删除了） 因此leader节点发送快照给本节点

				// 如果有快照的时候会读取快照 这个时候如果leader中没有保存快照直接把日志发送过来的话 直接返回即可不需要应用
				reply := kv.apply(applyMsg.Command)
				// 这个时候他可能不是leader 或者是发生了分区的旧leader 这两种情况下都不需要通知回复客户端
				currentTerm, isLeader := kv.rf.GetState()
				if isLeader && applyMsg.CommandTerm == currentTerm {
					kv.notify(applyMsg.CommandIndex, reply)
				}

				if kv.maxraftstate != -1 && kv.persist.RaftStateSize() > kv.maxraftstate {
					DPrintf("server {%d} get snapshot index = %d maxraftstate = %d raftStateSize = %d\n", kv.me, applyMsg.CommandIndex, kv.maxraftstate, kv.persist.RaftStateSize())
					kv.rf.Snapshot(applyMsg.CommandIndex, kv.getSnapshot())
				}

				kv.lastApplied = applyMsg.CommandIndex
			} else if applyMsg.SnapshotValid {
				if applyMsg.SnapshotIndex <= kv.lastApplied {
					DPrintf("server {%d} drop snapshot index = %d lastApplied = %d", kv.me, applyMsg.SnapshotIndex, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}

				DPrintf("server {%d} restore snapshot index = %d", kv.me, applyMsg.SnapshotIndex)
				kv.restoreSnapshot(applyMsg.Snapshot)
				kv.lastApplied = applyMsg.SnapshotIndex

			} else {
				panic("1111")
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) apply(cmd interface{}) *CommonReply {
	reply := &CommonReply{}
	op := cmd.(Op)
	DPrintf("server {%d} apply command = %v\n", kv.me, op)
	// 有可能出现这边刚执行到这里 然后另一边重试 进来了重复命令 这边还没来得及更新 那边判断重复指令不重复
	// 因此需要在应用日志之前再过滤一遍日志 如果发现有重复日志的话 那么就直接返回OK
	if op.OpType != GET && kv.isOldRequest(op.ClientId, op.RequestId) {
		reply.Err = OK
	} else {
		reply = kv.applyLogToStateMachine(&op)
		if op.OpType != GET {
			kv.updateLastRequest(&op, reply)
		}
	}

	reply.Err = OK
	return reply
}

func (kv *KVServer) applyLogToStateMachine(op *Op) *CommonReply {
	var reply = &CommonReply{}

	switch op.OpType {
	case GET:
		reply.Value = kv.stateMachine.get(op.Key)
	case PUT:
		kv.stateMachine.put(op.Key, op.Value)
	case APPEND:
		kv.stateMachine.appendVal(op.Key, op.Value)
	}

	reply.Err = OK

	return reply
}

func (kv *KVServer) notify(index int, reply *CommonReply) {
	DPrintf("server {%d} notify index = %d\n", kv.me, index)
	if notifyCh, ok := kv.notifyChanMap[index]; ok {
		notifyCh <- reply
	}
	DPrintf("server {%d} notify index = %d\n", kv.me, index)
}

func (kv *KVServer) isOldRequest(clientId int64, requestId int64) bool {
	if cxt, ok := kv.lastRequestMap[clientId]; ok {
		if requestId <= cxt.LastRequestId {
			return true
		}
	}

	return false
}

func (kv *KVServer) updateLastRequest(op *Op, reply *CommonReply) {

	ctx := ReplyContext{
		LastRequestId: op.RequestId,
		Reply:         *reply,
	}

	lastCtx, ok := kv.lastRequestMap[op.ClientId]
	if (ok && lastCtx.LastRequestId < op.RequestId) || !ok {
		kv.lastRequestMap[op.ClientId] = ctx
	}
}
