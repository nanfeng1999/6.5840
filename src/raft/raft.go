package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	ELECTION_TIMEOUT = 300
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
	currentTerm int     // raft节点当前的周期
	votedFor    int     // 在当前获得选票的候选⼈的Id
	logs        []Entry // 复制日志队列
	leaderId    int     // 当前领导人的Id
	state       State   // 本节点的角色

	electionTimer  *time.Ticker // 选举计时器
	heartbeatTimer *time.Ticker // 心跳包计时器
}

type Entry struct {
	Command interface{} // 日志中包含的命令
	Term    int         // 日志在哪个周期中产生的
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm // 有更新的当前任期号
		reply.VoteGranted = false
		return
	}

	// 执行到这里说明 请求参数中的任期大于等于当前节点的任期

	// 如果候选人的任期大于当前任期 那么必定请求投票成功
	// todo: 后续需要加上日志限制 日志需要是最新的
	if args.Term > rf.currentTerm {
		rf.changeState(FOLLOWER)       // 当前节点状态变成follower
		rf.currentTerm = args.Term     // 更新当前节点的任期
		rf.votedFor = args.CandidateId // 给候选者投票
		reply.VoteGranted = true
	} else {
		// 如果候选人的任期等于当前任期
		// 如果当前节点没有给其他节点投过票 或者 已经给当前候选人投过票（RPC可能发生重传）
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			// 已经给其他候选人投过票了
			reply.VoteGranted = false
		}
	}

	reply.Term = rf.currentTerm
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
	PreLogIndex  int     // 新的⽇志条⽬紧随之前的索引值
	PreLogTerm   int     // prevLogIndex 条⽬的任期号
	Entries      []Entry // 准备存储的⽇志条⽬
	LeaderCommit int     // 领导⼈已经提交的⽇志的索引值
}

// AppendEntriesReply 附加日志和心跳包的RPC回复参数
type AppendEntriesReply struct {
	Term    int  // 当前的任期号，⽤于领导⼈去更新⾃⼰
	Success bool // 跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的⽇志时为真
}

// 发送追加日志和心跳包的RPC请求
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.changeState(FOLLOWER)
	rf.currentTerm = args.Term

	reply.Term = rf.currentTerm
	reply.Success = true
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
			// FOLLOWER --> CANDIDATE
			rf.changeState(CANDIDATE)
			// 重置选举超时计时器
			rf.resetElectionTimeout()
			rf.requestVotes()
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			// 只有领导人可以发送心跳包
			if rf.state == LEADER {
				rf.mu.Lock()
				rf.sendEntries()
				rf.mu.Unlock()
			}
		}

	}
}

func (rf *Raft) changeState(state State) {
	if state == FOLLOWER {
		rf.resetElectionTimeout()
		rf.heartbeatTimer.Stop()
	} else if state == LEADER {
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

	for i := range rf.peers {
		if rf.me != i {
			go func() {
				// 发送RPC请求
				args := &RequestVoteArgs{
					Term:        rf.currentTerm, // 候选⼈的任期号
					CandidateId: rf.me,          // 请求选票的候选⼈的 Id
				}
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)
				// 如果成功接收到回复 同时请求投票成功 那么票数+1
				if ok && reply.VoteGranted {
					numVotes += 1
				}

				// 已经出现了任期更大的节点 本节点强制转换为FOLLOWER
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					// 要强制转换成跟随者
					rf.changeState(FOLLOWER)
				}

				if numVotes > totalVotes/2 {
					// 本节点当选Leader 选举停止 开始发送心跳包
					rf.changeState(LEADER)
				}
			}()
		}
	}

}

func (rf *Raft) sendEntries() {
	for i := range rf.peers {
		if rf.me != i {
			go func() {
				// 发送RPC请求
				args := &AppendEntriesArgs{
					Term:     rf.currentTerm, // leader的任期
					LeaderId: rf.me,          // leader的id号
				}
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, args, reply)
				// 收到回复 对方的任期比当前leader的任期大 说明当前leader已经过期 变成follower
				if ok && reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.changeState(FOLLOWER)
				}
			}()
		}
	}
}

// 重置本节点的超时截止时间
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimer.Reset(randomElectionTime())
}

func randomElectionTime() time.Duration {
	// 超时时间偏差 50 ~ 150 ms
	ms := 50 + (rand.Int63() % 100)
	// 超时计时器 350 ~ 450 ms
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
	rf.logs = make([]Entry, 0)
	rf.state = FOLLOWER

	// 初始化的时候开启选举计时器 关闭心跳包 只有当选了leader才开启心跳包
	rf.electionTimer = time.NewTicker(randomElectionTime())
	rf.heartbeatTimer = time.NewTicker(HEARTBEAT)
	rf.heartbeatTimer.Stop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
