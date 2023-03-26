package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"sort"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	ELECTION_TIMEOUT = 200
	HEARTBEAT        = 100 * time.Millisecond
)

type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int // 给lab3使用

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	currentTerm int     // raft节点当前的周期
	votedFor    int     // 在当前获得选票的候选⼈的Id
	logs        []Entry // 复制日志队列
	leaderId    int     // 当前领导人的Id
	state       State   // 本节点的角色

	electionTimer  *time.Ticker // 选举计时器
	heartbeatTimer *time.Ticker // 心跳包计时器
	// 2B
	commitIndex  int   // 已知的最大被提交的日志条目的索引值
	lastApplied  int   // 最后被应用到状态机的日志条目索引值
	nextIndex    []int // 对于每⼀个服务器，需要发送给他的下⼀个日志条目的索引值（初始化为领导⼈最后索引值加⼀）
	matchIndex   []int // 对于每⼀个服务器，已经复制给他的日志的最高索引值
	applyCh      chan ApplyMsg
	applyMsgCond *sync.Cond // 提交日志到上层应用的条件变量 只有在成功提交新的日志的时候才会触发
	isSnapshot   uint32     // 是否正在保存快照到applyCh 这是为了维持日志与快照的顺序一致
	isAppendLog  uint32     // 是否正在加入新日志到applyCh
}

type Entry struct {
	Command interface{} // 日志中包含的命令
	Term    int         // 日志在哪个周期中产生的
	Index   int         // 日志的索引
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.Save(rf.encodeState(), rf.persister.ReadSnapshot())
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	raftstate := w.Bytes()
	return raftstate
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []Entry
	var voteFor, currentTerm int
	if d.Decode(&logs) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&currentTerm) != nil {
		panic("decode persist state fail")
	}

	rf.logs = logs
	rf.votedFor = voteFor
	rf.currentTerm = currentTerm
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("node {%d} snapshot index = %d", rf.me, index)
	lastIncludedIndex := rf.getFirstLog().Index
	if lastIncludedIndex >= index {
		// 已经做过快照了 直接返回即可
		DPrintf("node {%d} outdate snapshot lastIncludedIndex = %d index = %d\n", rf.me, lastIncludedIndex, index)
		return
	}

	// 裁剪日志
	var tmp []Entry
	rf.logs = append(tmp, rf.logs[index-lastIncludedIndex:]...)
	// lastIncludedIndex和lastIncludedTerm都保存在这里
	rf.logs[0].Command = nil

	// 保存新的日志队列和快照
	rf.persister.Save(rf.encodeState(), snapshot)
}

type InstallSnapshotArgs struct {
	Term              int    // 任期
	LeaderId          int    // 领导人Id
	LastIncludedIndex int    // 快照中最后一个条目包含的索引
	LastIncludedTerm  int    // 快照中最后一个条目包含的任期
	Snapshot          []byte // 快照
}

type InstallSnapshotReply struct {
	Term int // 任期
}

// InstallSnapshot 滞后或者因为发生RPC延迟 follower接收到快照
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	DPrintf("node {%d} term = %d receive args = %v\n", rf.me, rf.currentTerm, *args)
	reply.Term = rf.currentTerm
	// 任期过期直接返回
	if args.Term < rf.currentTerm {
		DPrintf("node {%d} term = %d receive out date data, term is wrong\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	// 任期正确 或 任期更大
	rf.currentTerm = args.Term
	// 需要转换为跟随者 因为接收到快照也是leader存活的一种标志
	rf.changeState(FOLLOWER)

	firstIndex := rf.getFirstLog().Index
	if rf.commitIndex >= args.LastIncludedIndex {
		DPrintf("node {%d} term = %d receive out date commitIndex = %d lastIncludedIndex = %d\n", rf.me, rf.currentTerm, rf.commitIndex, args.LastIncludedIndex)
		// 说明这个快照是因为RPC延迟的过期快照 直接丢弃即可
		rf.mu.Unlock()
		return
	}

	// 如果前面有快照正在保存 那么直接丢弃此快照 不然可能发生以下情况
	// 第一个快照生成并解锁 但是还没有应用成功 第二个快照生成解锁 并应用
	// 第一个快照 应用 因为第一个快照内容更少 相当于日志队列发生了回退
	// 所以这里第一个快照如果在应用 第二个直接丢弃 不再应用
	if atomic.LoadUint32(&rf.isSnapshot) == 1 {
		reply.Term = -1
		rf.mu.Unlock()
		return
	}

	// 快照中的最后一个条目的索引大于或等于当前日志队列中的最后一条索引
	if rf.getLastLog().Index <= args.LastIncludedIndex {
		rf.logs = make([]Entry, 1)
	} else {
		var tmp []Entry
		rf.logs = append(tmp, rf.logs[args.LastIncludedIndex-firstIndex:]...)
	}

	rf.logs[0].Term = args.LastIncludedTerm
	rf.logs[0].Index = args.LastIncludedIndex
	rf.logs[0].Command = nil
	rf.persister.Save(rf.encodeState(), args.Snapshot)

	DPrintf("node {%d} term = %d logs = %v\n", rf.me, rf.currentTerm, rf.logs)

	rf.lastApplied, rf.commitIndex = args.LastIncludedIndex, args.LastIncludedIndex

	rf.isSnapshot = 1
	rf.mu.Unlock()

	for atomic.LoadUint32(&rf.isAppendLog) == 1 {
		time.Sleep(10 * time.Millisecond)
	}

	DPrintf("start append snapshot\n")
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	atomic.StoreUint32(&rf.isSnapshot, 0)

	DPrintf("end append snapshot\n")
}

// CondInstallSnapshot 为了保证原子性
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex >= lastIncludedIndex {
		// 说明这个快照是因为RPC延迟的过期快照 直接丢弃即可
		return false
	}

	firstIndex := rf.getFirstLog().Index

	// 快照中的最后一个条目的索引大于或等于当前日志队列中的最后一条索引
	if rf.getLastLog().Index <= lastIncludedIndex {
		rf.logs = make([]Entry, 1)
	} else {
		var tmp []Entry
		rf.logs = append(tmp, rf.logs[lastIncludedIndex-firstIndex:]...)
	}

	rf.logs[0].Term = lastIncludedTerm
	rf.logs[0].Index = lastIncludedIndex
	rf.logs[0].Command = nil
	rf.persister.Save(rf.encodeState(), snapshot)
	DPrintf("node {%d} term = %d lastApplied = %d commitIndex = %d LastIncludedIndex = %d\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, lastIncludedIndex)
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	return true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选⼈的任期号
	CandidateId  int // 请求选票的候选⼈的 Id
	LastLogIndex int // 候选⼈的最后⽇志条⽬的索引值
	LastLogTerm  int // 候选⼈最后⽇志条⽬的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号，以便于候选⼈去更新自己的任期号
	VoteGranted bool // 候选⼈赢得了此张选票时为真
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 接收者实现：
	// 1. 如果 term < currentTerm 返回 false （5.2 节）
	// 2. 如果 votedFor 为空或者为 candidateId，并且候选⼈的日志至少和自己⼀样新，那么就投票给他（5.2 节， 5.4 节）
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm // 有更新的当前任期号
		reply.VoteGranted = false
		return
	}

	// todo 这个bug找了好久
	if args.Term > rf.currentTerm {
		if rf.state != FOLLOWER {
			// 有一种可能是其他节点不停地超时选举 然后导致该节点一直重置选举计时器
			// 正确的情况下是只有给其他节点成功投票才重置选举计时器 或者当前节点是过期leader的时候
			rf.changeState(FOLLOWER)
		}
		rf.currentTerm, rf.votedFor = args.Term, -1
	}
	// 执行到这里说明 请求参数中的任期大于等于当前节点的任期

	// 2A
	// 如果候选人的任期大于当前任期 那么必定请求投票成功
	// 如果候选人的任期等于当前任期 并且 （当前节点没有给其他节点投过票 或者 已经给当前候选人投过票（RPC可能发生重传））
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 2B 加上日志限制 日志需要是最新的
		if rf.isLogMatch(args.LastLogIndex, args.LastLogTerm) {
			DPrintf("node {%d} term {%d} vote for %d\n", rf.me, rf.currentTerm, args.CandidateId)
			rf.changeState(FOLLOWER)       // 当前节点状态变成follower
			rf.votedFor = args.CandidateId // 给候选者投票
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}

	} else {
		// 已经给其他候选人投过票了
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
}

//论文原话：通过⽐较两份日志中最后⼀条日志条目的索引值和任期号定义谁的日志⽐较新。如果两份
//日志最后的条⽬的任期号不同，那么任期号⼤的⽇志更加新。如果两份⽇志最后的条⽬任期号
//相同，那么⽇志⽐较⻓的那个就更加新
func (rf *Raft) isLogMatch(lastLogIndex int, lastLogTerm int) bool {
	lastLog := rf.getLastLog()
	if lastLogTerm > lastLog.Term || (lastLogTerm == lastLog.Term && lastLogIndex >= lastLog.Index) {
		return true
	}

	return false
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs 附加日志和心跳包的RPC请求参数
type AppendEntriesArgs struct {
	Term         int     // 领导⼈的任期号
	LeaderId     int     // 领导⼈的Id，以便于跟随者重定向请求
	PreLogIndex  int     // 新的日志条目紧随之前的索引值
	PreLogTerm   int     // prevLogIndex 条目的任期号
	Logs         []Entry // 准备存储的日志条目
	LeaderCommit int     // 领导⼈已经提交的日志的索引值
}

// AppendEntriesReply 附加日志和心跳包的RPC回复参数
type AppendEntriesReply struct {
	Term    int  // 当前的任期号，⽤于领导⼈去更新⾃⼰
	Success bool // 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的⽇志时为真
	XTerm   int  // 在冲突中的任期
	XIndex  int  // 在冲突中的索引
}

// 发送追加日志和心跳包的RPC请求
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	DPrintf("node {%d} term {%d} receive new log length %d preLogIndex = %d\n", rf.me, rf.currentTerm, len(args.Logs), args.PreLogIndex)
	// 任期过期 什么也不做
	if args.Term < rf.currentTerm {
		DPrintf("node {%d} term {%d} reject term = %d log\n", rf.me, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 2A 任期相同 或 本节点任期更小
	// 节点维持follower状态不变 并且更新任期
	rf.changeState(FOLLOWER)
	rf.currentTerm = args.Term

	// 2B 判断日志是否匹配
	// 说明是心跳包 直接返回即可
	// 下面这句话不能够加入 因为leader第一次发送新日志到本节点 本节点接收成功 leader更新nextIndex
	// 第二次leader发送日志为空 但是leaderCommit改变 如果这里直接返回 本节点第一次加入的新日志将一直无法提交
	//if len(args.Logs) == 0 {
	//	reply.Success = true
	//	return
	//}
	// 延迟的RPC请求 直接丢弃
	if args.PreLogIndex < rf.getFirstLog().Index {
		DPrintf("node {%d} term {%d} preLogIndex = %d firstIndex = %d\n", rf.me, rf.currentTerm, args.PreLogIndex, rf.getFirstLog().Index)
		reply.Success, reply.Term = false, 0
		return
	}

	DPrintf("node {%d} term {%d} start judge whether append log\n", rf.me, rf.currentTerm)
	// 如果上一条日志索引对应的日志 在本节点的日志队列中不存在 或者无法对应上 那么返回false 表示日志队列不匹配
	if args.PreLogIndex > rf.getLastLog().Index || rf.logs[args.PreLogIndex-rf.getFirstLog().Index].Term != args.PreLogTerm {
		DPrintf("node {%d} term {%d} reject append log\n", rf.me, rf.currentTerm)

		// 按照任期回退索引
		// 发生冲突的时候一共有三种可能
		// 1. follower中没有preLogIndex对应的日志 那么nextIndex回退到刚好匹配的那个索引
		// 2. follower中存在preLogIndex对应的日志 但是任期冲突 那么记录一下follower当前任期的第一条日志索引
		// leader接收到冲突任期之后判断一下 如果本地存在冲突任期的日志
		// 类似这种情况 112233 112222 那么leader应该回退到该任期对应的最后一条日志索引+1
		// 3. leader接收到冲突任期之后判断一下 如果本地不存在冲突任期的日志
		// 类似这种情况 112233 112244
		reply.Success, reply.Term = false, rf.currentTerm
		lastIndex := rf.getLastLog().Index
		firstIndex := rf.getFirstLog().Index
		if args.PreLogIndex > lastIndex {
			reply.XIndex, reply.XTerm = lastIndex+1, -1
		} else {
			reply.XTerm = rf.logs[args.PreLogIndex-firstIndex].Term

			index := args.PreLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == reply.XTerm {
				index--
			}
			reply.XIndex = index
		}
		DPrintf("node {%d} term {%d} reply = %v\n", rf.me, rf.currentTerm, *reply)
		return
	}

	DPrintf("node {%d} term {%d} start append log\n", rf.me, rf.currentTerm)
	// 论文原话：如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这⼀条
	// 和之后所有的 （5.3节）
	firstIndex := rf.cropLogs(args)
	// 附加日志
	rf.logs = append(rf.logs, args.Logs[firstIndex:]...)
	DPrintf("node {%d} term {%d} append log success\n", rf.me, rf.currentTerm)
	// 求出新的commitIndex
	newCommitIndex := min(args.LeaderCommit, rf.getLastLog().Index)
	// 通知有新日志提交 如果是重传了已经加入日志队列的数据 那么commitIndex是不会变的 因此加入一个判断
	if newCommitIndex > rf.commitIndex {
		DPrintf("node {%d} term {%d} update commitIndex %d to %d\n", rf.me, rf.currentTerm, rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
		rf.applyMsgCond.Signal()
	}

	reply.Success, reply.Term = true, rf.currentTerm
	DPrintf("node {%d} term {%d} reply = %v\n", rf.me, rf.currentTerm, *reply)
}

// 裁剪日志 并返回最后一个匹配的索引
func (rf *Raft) cropLogs(args *AppendEntriesArgs) int {
	// 得到第一条日志的索引
	firstIndex := rf.getFirstLog().Index

	// 遍历新加入的日志
	for i, entry := range args.Logs {
		// 一旦新加入日志条目的索引超出了日志队列的范围 那么原队列不需要裁剪 把接收到的日志中不存在的日志添加到本地日志队列
		// 如果某个新加入的日志对应的任期不等于本地日志对应的任期 那么本地日志从这个位置以及以后的日志全部丢弃
		if entry.Index >= firstIndex+len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			var tmp []Entry
			rf.logs = append(tmp, rf.logs[:entry.Index-firstIndex]...)
			return i
		}
	}

	// 遍历到这说明所有日志都匹配 可能是接收到了重传的日志队列 那么本地日志队列不需要加入新的日志
	return len(args.Logs)
}

func (rf *Raft) appendLog(command interface{}) Entry {
	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   rf.getLastLog().Index + 1,
	}

	rf.logs = append(rf.logs, entry)
	rf.persist()
	return entry
}

// 得到日志队列的第一条日志 这个函数是为了 后续的快照做准备 如果使用快照的话 日志的索引就不等于日志在日志队列中的索引了
func (rf *Raft) getFirstLog() Entry {
	return rf.logs[0]
}

// 得到日志队列的最后一条日志
func (rf *Raft) getLastLog() Entry {
	return rf.logs[len(rf.logs)-1]
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, -1, false
	}

	DPrintf("leader node {%d} term {%d} receive command %v\n", rf.me, rf.currentTerm, command)
	newLog := rf.appendLog(command) // 日志队列中加入一条新log
	index = newLog.Index
	term = rf.currentTerm         // 当前的任期
	isLeader = rf.state == LEADER // 是否是leader

	rf.sendEntries()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// 广播时间 << 选举超时时间 << 平均故障时间
		// 测试器要求领导者发送心跳rpc每秒不超过10次 所以广播时间最小100ms/次

		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			DPrintf("node {%d} term {%d} start election\n", rf.me, rf.currentTerm)
			// FOLLOWER --> CANDIDATE
			rf.changeState(CANDIDATE)
			// 重置选举超时计时器
			rf.resetElectionTimeout()
			rf.requestVotes()
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			// 只有领导人可以发送心跳包
			// 锁放在上面 不然有数据竞争
			rf.mu.Lock()
			if rf.state == LEADER {
				DPrintf("node {%d} term {%d} send heartbeat\n", rf.me, rf.currentTerm)
				rf.sendEntries()
			}
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) applyMsg() {
	for rf.killed() == false {
		// 锁分成两段 提升性能 因为把log加入队列中也需要一定的时间 同时channel是有并发保护机制的
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyMsgCond.Wait()
		}
		applyEntries := make([]Entry, rf.commitIndex-rf.lastApplied)
		firstIndex := rf.getFirstLog().Index
		commitIndex := rf.commitIndex
		copy(applyEntries, rf.logs[rf.lastApplied-firstIndex+1:rf.commitIndex-firstIndex+1])
		DPrintf("node {%d} term {%d} lastApplied = %d commitIndex = %d firstIndex = %d append entries %v\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, firstIndex, applyEntries)

		if rf.isSnapshot == 1 {
			rf.isAppendLog = 0
			rf.mu.Unlock()
			continue
		}

		rf.isAppendLog = 1
		rf.mu.Unlock()

		//DPrintf("node {%d} term {%d} start commit log %v\n", rf.me, rf.currentTerm, applyEntries)
		for _, entry := range applyEntries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}

		atomic.StoreUint32(&rf.isAppendLog, 0)

		//DPrintf("node {%d} term {%d} commit log success\n", rf.me, rf.currentTerm)
		rf.mu.Lock()
		// 这里不能使用rf.commitIndex 因为有可能apply执行到这里 这个时候本节点又更新了一次rf.commitIndex
		// 那么第一次的更新就丢失了 会造成部分日志没有成功apply
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()

	}
}

func (rf *Raft) changeState(state State) {
	if state == FOLLOWER {
		DPrintf("node {%d} term {%d} change ==> follower\n", rf.me, rf.currentTerm)
		rf.resetElectionTimeout()
		rf.heartbeatTimer.Stop()
	} else if state == LEADER {
		DPrintf("node {%d} term {%d} change ==> leader\n", rf.me, rf.currentTerm)
		lastLog := rf.getLastLog()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(HEARTBEAT)
	}

	rf.state = state
}

// 候选者向其他节点请求投票
func (rf *Raft) requestVotes() {
	totalVotes := len(rf.peers)

	rf.currentTerm += 1 // 任期加一
	rf.votedFor = rf.me // 自己给自己投票
	numVotes := 1       // 自己给自己投票
	rf.persist()

	// 不能放在闭包里面 因为 当前面的请求已经收到回复 后面的请求才刚才开始发送的时候 参数currentTerm会改变
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,        // 候选⼈的任期号
		CandidateId:  rf.me,                 // 请求选票的候选⼈的 Id
		LastLogIndex: rf.getLastLog().Index, // 最后一条日志索引
		LastLogTerm:  rf.getLastLog().Term,  // 最后一条日志任期
	}

	for i := range rf.peers {
		if rf.me != i {
			// 需要传入参数 否则会出现规定时间内无法产生leader的情况
			go func(peer int) {
				// 发送RPC请求
				reply := &RequestVoteReply{}
				// 下面的操作需要成功接收再执行
				if rf.sendRequestVote(peer, args, reply) {
					// 加锁 因为可能同时返回导致并发冲突
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// 如果不加入这个判断 那么当节点较多的时候会执行多次转换为leader或follower节点的操作
					// 如果其他节点任期更大 当前节点会转换为follower 同时任期更新到最新 后面的投票请求不应该再发送 所以要加个args.Term == rf.currentTerm的判断
					if rf.state == CANDIDATE && rf.currentTerm == args.Term {
						// 请求投票成功 那么票数+1
						if reply.VoteGranted {
							numVotes += 1

							// 一旦票数达到 立即转换成leader 并给其他节点发送心跳包 否
							if numVotes > totalVotes/2 {
								// 本节点当选Leader 选举停止 开始发送心跳包
								rf.changeState(LEADER)
								// 选举成功需要马上把心跳包发送给其他节点 则可能出现leader已经存在 其他节点还在选举的情况
								rf.sendEntries()
							}
						}

						// 已经出现了任期更大的节点 本节点强制转换为FOLLOWER
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							// 要强制转换成跟随者
							rf.changeState(FOLLOWER)
							rf.persist()
						}
					}

				}
			}(i)
		}
	}

}

func (rf *Raft) sendEntries() {
	for i := range rf.peers {
		if rf.me != i {
			go func(peer int) {
				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					return
				}

				firstIndex := rf.getFirstLog().Index
				if rf.nextIndex[peer] <= firstIndex {
					DPrintf("leader node {%d} term {%d} send snapshot to %d, nextIndex = %d firstIndex = %d\n", rf.me, rf.currentTerm, peer, rf.nextIndex[peer], firstIndex)
					// 走发送快照的流程
					args := rf.newInstallSnapshotArgs()
					rf.mu.Unlock()

					reply := &InstallSnapshotReply{}
					if rf.sendInstallSnapshot(peer, args, reply) {
						rf.mu.Lock()
						rf.handleInstallSnapshotResponse(peer, args, reply)
						rf.mu.Unlock()
					}
				} else {
					// 走正常发送日志的流程
					// 发送RPC请求
					args := rf.newAppendEntriesArgs(peer)
					rf.mu.Unlock()

					reply := &AppendEntriesReply{}
					if rf.sendAppendEntries(peer, args, reply) {
						rf.mu.Lock()
						rf.handleAppendEntriesResponse(peer, args, reply)
						rf.mu.Unlock()
					}
				}

			}(i)
		}
	}
}

func (rf *Raft) newAppendEntriesArgs(peer int) *AppendEntriesArgs {
	firstIndex := rf.getFirstLog().Index
	DPrintf("leader node {%d} term {%d} send entries to %d, preLogIndex = %d firstIndex = %d\n", rf.me, rf.currentTerm, peer, rf.nextIndex[peer]-1, firstIndex)
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,                                     // leader的任期
		LeaderId:     rf.me,                                              // leader的id号
		PreLogIndex:  rf.nextIndex[peer] - 1,                             // 发送日志条目上一条日志的索引
		PreLogTerm:   rf.logs[rf.nextIndex[peer]-1-firstIndex].Term,      // 发送日志条目上一条日志的任期 匹配才接收同步日志
		Logs:         cloneLogs(rf.logs[rf.nextIndex[peer]-firstIndex:]), // 下一批要同步过去的日志
		LeaderCommit: rf.commitIndex,                                     // leader已经提交的日志
	}
	return args
}

func (rf *Raft) handleAppendEntriesResponse(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A
	// 收到回复 对方的任期比当前leader的任期大 说明当前leader已经过期 变成follower
	// 当前节点变成了follower 然后 后面的请求发送过来的时候当前节点的任期已经修改了 所以会进入else中的语句 然后修改nextIndex导致发生错误
	// 不排除在此期间重新当选为leader然后导致错误的可能 所以任期也要判断一下
	if rf.state == LEADER && rf.currentTerm == args.Term {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.changeState(FOLLOWER)
			rf.persist()
		} else {
			// 2B 对方任期等于当前任期 或小于当前任期 那么判断日志接收的情况
			if reply.Success {
				// 匹配的日志索引
				// 如果之前的reply回复 那么matchIndex会倒退 nextIndex倒退 如果对方节点已经做好快照
				// 并且快照索引大于nextIndex 同时本节点的firstIndex小于nextIndex 那么就是接着发送日志过去
				// 那么preLogIndex就会小于对方节点的firstIndex 对方会直接把term = 0
				// 这种情况下这函数无法更新nextIndex
				rf.matchIndex[peer] = max(rf.matchIndex[peer], args.PreLogIndex+len(args.Logs))
				DPrintf("preLogIndex = %d log length = %d", args.PreLogIndex, len(args.Logs))
				// 日志成功被接收 下一个应该返给这个节点的日志索引+1
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				DPrintf("leader node {%d} term {%d} update %d nextIndex = %d\n", rf.me, rf.currentTerm, peer, rf.nextIndex[peer])
				// 论文原话：如果存在⼀个满⾜ N > commitIndex 的 N，并且⼤多数的 matchIndex[i] ≥ N
				// 成⽴，并且 log[N].term == currentTerm 成⽴，那么令 commitIndex 等于这个 N
				//（5.3 和 5.4 节）
				rf.updateLeaderCommitIndex()
			} else if reply.Term == rf.currentTerm {
				// 如果有延迟的回复 是之前发生了冲突 这里直接更新的话nextIndex会倒退
				// 如果这个时候peer节点正好记录了快照 那么会接收到preLogIndex < firstIndex的log entries
				// 这种情况下返回的任期为0 这块地方的代码都不会执行 那么nextIndex永远不会改变
				DPrintf("leader node {%d} term {%d} xIndex = %d peer = %d matchIndex = %d\n", rf.me, rf.currentTerm, reply.XIndex, peer, rf.matchIndex[peer])
				//if reply.XIndex <= rf.matchIndex[peer] {
				//	return
				//}
				// 按照任期回退
				firstIndex := rf.getFirstLog().Index
				rf.nextIndex[peer] = max(reply.XIndex, rf.matchIndex[peer]+1)

				if reply.XTerm != -1 {
					// 如果存在和冲突任期一致的日志 那么返回第一个等于的
					// 求最大值作为边界 减少遍历的次数
					boundary := max(firstIndex, reply.XIndex)
					for i := args.PreLogIndex; i >= boundary; i-- {
						if rf.logs[i-firstIndex].Term == reply.XTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
					// 如果不存在那么默认等于XIndex 在前面已经赋值过了
				}
				DPrintf("leader node {%d} term {%d} update %d nextIndex = %d\n", rf.me, rf.currentTerm, peer, rf.nextIndex[peer])
			}
		}

	}
}

func (rf *Raft) updateLeaderCommitIndex() {
	// 对matchIndex数组排序 然后求其中位数即可
	n := len(rf.matchIndex)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sortMatchIndex[rf.me] = rf.getLastLog().Index
	sort.Ints(sortMatchIndex)

	newCommitIndex := sortMatchIndex[n/2] // 因为本节点默认是满足条件的 所以不能够使用n/2 + 1
	if newCommitIndex > rf.commitIndex && newCommitIndex <= rf.getLastLog().Index && rf.logs[newCommitIndex-rf.getFirstLog().Index].Term == rf.currentTerm {
		DPrintf("leader node {%d} term {%d} update commitIndex %d to %d\n", rf.me, rf.currentTerm, rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
		rf.applyMsgCond.Signal() // 容易忘记通知
	}
}

func (rf *Raft) newInstallSnapshotArgs() *InstallSnapshotArgs {
	firstLog := rf.getFirstLog()
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstLog.Index,
		LastIncludedTerm:  firstLog.Term,
		Snapshot:          rf.persister.ReadSnapshot(),
	}

	return args
}

func (rf *Raft) handleInstallSnapshotResponse(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.state == LEADER && rf.currentTerm == args.Term {
		if rf.currentTerm < reply.Term {
			// 任期过期 直接返回
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.changeState(FOLLOWER)
			rf.persist()
			return
		} else if rf.currentTerm == reply.Term {
			DPrintf("node {%d} nextIndex from %d to %d\n", peer, rf.nextIndex[peer], args.LastIncludedIndex+1)
			rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex) // 这一行代码容易忘记
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		} else if reply.Term == -1 {
			// do nothing
		}
	}

}

// 重置本节点的超时截止时间
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimer.Reset(randomElectionTime())
}

func randomElectionTime() time.Duration {
	// 超时时间偏差 0 ~ 200 ms
	ms := rand.Int63() % 200
	// 超时计时器 200 ~ 400 ms
	eleTime := time.Duration(ELECTION_TIMEOUT+ms) * time.Millisecond
	return eleTime
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER

	// 初始化的时候开启选举计时器 关闭心跳包 只有当选了leader才开启心跳包
	rf.electionTimer = time.NewTicker(randomElectionTime())
	rf.heartbeatTimer = time.NewTicker(HEARTBEAT)
	rf.heartbeatTimer.Stop()
	// 2B
	rf.logs = make([]Entry, 1) // 设置第一个日志条目为空日志
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyMsgCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// 2D
	firstLog := rf.getFirstLog()
	rf.lastApplied = firstLog.Index
	rf.commitIndex = firstLog.Index
	// 2B
	lastLog := rf.getLastLog()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLog.Index + 1
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMsg()
	return rf
}
