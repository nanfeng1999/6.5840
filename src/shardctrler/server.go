package shardctrler

import (
	"6.5840/raft"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs        []Config                  // indexed by config num
	notifyChanMap  map[int]chan *CommonReply // 通知chan key = index val = chan
	lastRequestMap map[int64]ReplyContext    // 缓存每个客户端对应的最近请求和reply key = clientId
}

type Op struct {
	// Your data here.
	ClientId  int64            // 客户端Id
	RequestId int64            // 请求Id
	OpType    string           // 操作类型
	Servers   map[int][]string // Join
	GIDs      []int            // Leave
	Shard     int              // Move
	GID       int              // Move
	Num       int              // Query
}

func (sc *ShardCtrler) getLastConfig() Config {
	return sc.configs[len(sc.configs)-1]
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.isOldRequest(args.ClientId, args.RequestId) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    Join,
		Servers:   args.Servers,
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	sc.notifyChanMap[index] = make(chan *CommonReply, 1)
	notifyChan := sc.notifyChanMap[index]
	sc.mu.Unlock()

	select {
	case ret := <-notifyChan:
		reply.WrongLeader, reply.Err = ret.WrongLeader, ret.Err
		currentTerm, isLeader := sc.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.WrongLeader = true
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = TimeOut
	}

	sc.mu.Lock()
	delete(sc.notifyChanMap, index)
	sc.mu.Unlock()

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.isOldRequest(args.ClientId, args.RequestId) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    Leave,
		GIDs:      args.GIDs,
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	sc.notifyChanMap[index] = make(chan *CommonReply, 1)
	notifyChan := sc.notifyChanMap[index]
	sc.mu.Unlock()

	select {
	case ret := <-notifyChan:
		reply.WrongLeader, reply.Err = ret.WrongLeader, ret.Err
		currentTerm, isLeader := sc.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.WrongLeader = true
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = TimeOut
	}

	sc.mu.Lock()
	delete(sc.notifyChanMap, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.isOldRequest(args.ClientId, args.RequestId) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    Move,
		Shard:     args.Shard,
		GID:       args.GID,
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	sc.notifyChanMap[index] = make(chan *CommonReply, 1)
	notifyChan := sc.notifyChanMap[index]
	sc.mu.Unlock()

	select {
	case ret := <-notifyChan:
		reply.WrongLeader, reply.Err = ret.WrongLeader, ret.Err
		currentTerm, isLeader := sc.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.WrongLeader = true
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = TimeOut
	}

	sc.mu.Lock()
	delete(sc.notifyChanMap, index)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.isOldRequest(args.ClientId, args.RequestId) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    Query,
		Num:       args.Num,
	}

	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	sc.notifyChanMap[index] = make(chan *CommonReply, 1)
	notifyChan := sc.notifyChanMap[index]
	sc.mu.Unlock()

	select {
	case ret := <-notifyChan:
		reply.WrongLeader, reply.Err, reply.Config = ret.WrongLeader, ret.Err, ret.Config
		currentTerm, isLeader := sc.rf.GetState()
		if !isLeader || currentTerm != term {
			reply.WrongLeader = true
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = TimeOut
	}

	sc.mu.Lock()
	delete(sc.notifyChanMap, index)
	sc.mu.Unlock()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			sc.mu.Lock()

			reply := sc.apply(applyMsg.Command)
			DPrintf("server {%d} reply = %v\n", sc.me, reply)
			currentTerm, isLeader := sc.rf.GetState()
			if isLeader && applyMsg.CommandTerm == currentTerm {
				DPrintf("server {%d} is leader currentTerm = %d\n", sc.me, currentTerm)
				sc.notify(applyMsg.CommandIndex, reply)
			}

			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) apply(cmd interface{}) *CommonReply {
	reply := &CommonReply{}
	op := cmd.(Op)

	if op.OpType != Query && sc.isOldRequest(op.ClientId, op.RequestId) {
		reply.Err = OK
	} else {
		reply = sc.applyLogToStateMachine(&op)
		if op.OpType != Query {
			sc.updateLastRequest(&op, reply)
		}
	}

	return reply
}

func (sc *ShardCtrler) applyLogToStateMachine(op *Op) *CommonReply {
	var reply = &CommonReply{}

	switch op.OpType {
	case Join:
		sc.addNewConfig(op.Servers)
	case Leave:
		sc.leaveGroup(op.GIDs)
	case Move:
		sc.moveShard(op.Shard, op.GID)
	case Query:
		reply.Config = sc.queryConfig(op.Num)
		DPrintf("server = {%d} get reply config = %v configs = %v\n", sc.me, reply.Config, sc.configs)
	}

	reply.Err = OK

	return reply
}

func (sc *ShardCtrler) addNewConfig(servers map[int][]string) {
	if len(servers) == 0 {
		return
	}
	lastCfg := sc.getLastConfig()
	groups := copyMap(lastCfg.Groups)
	newGroups := addMap(groups, servers)
	DPrintf("server = {%d} start rebalance shards\n", sc.me)
	newShards := rebalanceShards(lastCfg.Shards, newGroups)
	DPrintf("server = {%d} rebalance end\n", sc.me)

	newCfg := Config{
		Num:    lastCfg.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}
	DPrintf("server = {%d} config = %v\n", sc.me, newCfg)
	sc.configs = append(sc.configs, newCfg)
}

func (sc *ShardCtrler) leaveGroup(gids []int) {
	if len(gids) == 0 {
		return
	}

	lastCfg := sc.getLastConfig()
	newGroups := copyMap(lastCfg.Groups)
	shards := lastCfg.Shards

	for _, gid := range gids {
		for i, shard := range shards {
			if shard == gid {
				shards[i] = 0
			}
		}
		delete(newGroups, gid)
	}

	newShards := rebalanceShards(shards, newGroups)

	newCfg := Config{
		Num:    lastCfg.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}

	sc.configs = append(sc.configs, newCfg)

}

func (sc *ShardCtrler) moveShard(shard int, gid int) {
	if !(shard >= 0 && shard < NShards) {
		return
	}

	lastCfg := sc.getLastConfig()
	if _, ok := lastCfg.Groups[gid]; !ok {
		return
	}

	newGroups := copyMap(lastCfg.Groups)
	newShards := lastCfg.Shards
	newShards[shard] = gid

	newCfg := Config{
		Num:    lastCfg.Num + 1,
		Shards: newShards,
		Groups: newGroups,
	}

	sc.configs = append(sc.configs, newCfg)

}

func (sc *ShardCtrler) queryConfig(num int) Config {
	if num <= -1 || num >= len(sc.configs) {
		return sc.getLastConfig()
	}

	return sc.configs[num]
}

func (sc *ShardCtrler) notify(index int, reply *CommonReply) {
	if notifyCh, ok := sc.notifyChanMap[index]; ok {
		notifyCh <- reply
	}
}

func (sc *ShardCtrler) isOldRequest(clientId int64, requestId int64) bool {
	if cxt, ok := sc.lastRequestMap[clientId]; ok {
		if requestId <= cxt.LastRequestId {
			return true
		}
	}

	return false
}

func (sc *ShardCtrler) updateLastRequest(op *Op, reply *CommonReply) {

	ctx := ReplyContext{
		LastRequestId: op.RequestId,
		Reply:         *reply,
	}

	lastCtx, ok := sc.lastRequestMap[op.ClientId]
	if (ok && lastCtx.LastRequestId < op.RequestId) || !ok {
		sc.lastRequestMap[op.ClientId] = ctx
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs[0].Shards = [NShards]int{}
	sc.notifyChanMap = make(map[int]chan *CommonReply)
	sc.lastRequestMap = make(map[int64]ReplyContext)
	go sc.applier()
	return sc
}
