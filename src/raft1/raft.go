package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	DEBUG = iota
	INFO
	WARNING
	ERROR
)

type Role string

const (
	LEADER    = "Leader"
	CANDIDATE = "Candidate"
	FOLLOWER  = "Follower"
)

const HeartBeatInterval = 10

const ElectionTimeout = 200

type LogEntry struct {
	Index   int         // 日志索引
	Term    int         // 所属任期
	Command interface{} // 客户端命令，可存储任意类型
}

// A Go object implementing a single Raft peer.
type Raft struct {
	logLevel int // 当前日志等级
	logger   *log.Logger

	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all server
	currentRole       Role
	currentTerm       int
	votedFor          map[int]int // candidateId that received vote in current term
	lastHeartbeatTime time.Time
	logEntries        []LogEntry

	// volatile state on all servers
	commitIdx      int
	lastAppliedIdx int

	// volatile states for leaders
	// ToDo: find a better way to handle this status
	nextIndexMap  map[int]int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndexMap map[int]int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

func (rf *Raft) Log(level int, msg string, args ...interface{}) {
	if level < rf.logLevel {
		return
	}

	// 获取调用的函数名
	pc, _, _, ok := runtime.Caller(1)
	funcName := "unknown"
	if ok {
		fullFuncName := runtime.FuncForPC(pc).Name()
		lastSlash := 0
		for i := len(fullFuncName) - 1; i >= 0; i-- {
			if fullFuncName[i] == '/' || fullFuncName[i] == '.' {
				lastSlash = i
				break
			}
		}
		funcName = fullFuncName[lastSlash+1:]
	}

	// 构造日志前缀
	prefix := fmt.Sprintf("[%s] [Node %d] [%s] ", time.Now().Format("2006-01-02 15:04:05.000"), rf.me, funcName)

	// 输出日志
	rf.logger.Printf(prefix+msg, args...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = (rf.currentRole == LEADER)
	rf.Log(INFO, " Current Role: %s Current Term : %d", rf.currentRole, rf.currentTerm)
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
	// Your code here (3C).
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
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// 投票选举参数
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term            int
	LeaderId        int
	PrevLogIndex    int
	PrevLogTerm     int
	Entries         []LogEntry
	LeaderCommitIdx int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) SwitchRole(nextRole Role) {
	if rf.currentRole == nextRole {
		return
	}
	rf.Log(INFO, "from %s to %s \n", rf.currentRole, nextRole)
	rf.currentRole = nextRole
}

// AppendEntries handles incoming AppendEntries RPCs from the leader.
// Arguments:
//
//	args  - Contains details of the AppendEntries RPC:
//	  - Term: The leader's term.
//	  - LeaderId: The leader's ID (for redirecting clients).
//	  - PrevLogIndex: Index of the log entry immediately preceding new ones.
//	  - PrevLogTerm: Term of the log entry at PrevLogIndex.
//	  - Entries[]: Log entries to store (empty for heartbeat).
//	               May send more than one for efficiency.
//	  - LeaderCommit: Leader's commitIndex.
//	reply - The reply structure for the RPC:
//	  - Term: The current term of the follower (for the leader to update itself).
//	  - Success: True if the follower contained an entry matching
//	             PrevLogIndex and PrevLogTerm.
//
// Results:
//  1. Reply false if Term < currentTerm (§5.1).
//  2. Reply false if the log doesn’t contain an entry at PrevLogIndex
//     whose term matches PrevLogTerm (§5.3).
//  3. If an existing entry conflicts with a new one (same index but different terms),
//     delete the existing entry and all that follow it (§5.3).
//  4. Append any new entries not already in the log.
//  5. If LeaderCommit > commitIndex, set commitIndex =
//     min(LeaderCommit, index of the last new entry).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// now all log entries is empty heartbeat from leader
	if rf.currentTerm > args.Term {
		return
	}
	rf.Log(INFO, "Args: %v", args)
	rf.lastHeartbeatTime = time.Now()
	// 这里暂时没考虑多个leader，需要比较谁的log更新的问题
	if rf.currentRole != LEADER || (rf.currentRole == LEADER && args.Term > rf.currentTerm) {
		if rf.currentTerm != args.Term {
			rf.Log(INFO, "Update Term from %d to %d ", rf.currentTerm, args.Term)
		}
		{
			rf.currentTerm = args.Term
		}
		rf.SwitchRole(FOLLOWER)
	}
}

func (rf *Raft) SendHeartbeat() {
	// send empty log to all peers
	rf.Log(INFO, "Leader Sending HeartBeat to Peers, Current Term is %d", rf.currentTerm)
	// emptyLog := &LogEntry{}
	// emptyLog.Term = rf.currentTerm

	appendEntryArgs := &AppendEntriesArgs{}
	appendEntryArgs.Term = rf.currentTerm
	// appendEntryArgs.PrevLogTerm = emptyLog.Term
	// appendEntryArgs.PrevLogIndex = emptyLog.Index

	// appendEntryArgs.Entries = append(appendEntryArgs.Entries, *emptyLog)
	for _, peer := range rf.peers {
		appendEntriesReply := &AppendEntriesReply{}
		peer.Call("Raft.AppendEntries", appendEntryArgs, appendEntriesReply)
	}
}

// example RequestVote RPC handler.

// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	lastLog := rf.logEntries[len(rf.logEntries)-1]
	_, ok := rf.votedFor[args.Term]
	if (!ok || rf.votedFor[args.Term] == args.CandidateId) && (args.LastLogIndex >= lastLog.Index) {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		if rf.votedFor[args.Term] != args.CandidateId {
			rf.lastHeartbeatTime = time.Now()
		}
		rf.votedFor[args.Term] = args.CandidateId
		rf.Log(INFO, "[vote for term %d] Vote Granted from: %d To: %d , RequestVote Args is %v Voted for %v", args.Term, rf.me, args.CandidateId, args, rf.votedFor)
		return
	}
	rf.Log(INFO, "[vote for term %d] Vote Denied from: %d To: %d , RequestVote Args is %v  Voted for %v", args.Term, rf.me, args.CandidateId, args, rf.votedFor)
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

	// Your code here (3B).

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

func (rf *Raft) checkElectionTimeout() bool {
	currentTime := time.Now()
	elapsed := currentTime.Sub(rf.lastHeartbeatTime)
	return elapsed > ElectionTimeout*time.Millisecond
}

func (rf *Raft) startNewElection() {
	rf.SwitchRole(CANDIDATE)

	rf.currentTerm++
	rf.votedFor[rf.currentTerm] = rf.me
	rf.Log(INFO, "Start Election for term %d", rf.currentTerm)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		if rf.currentRole == LEADER {
			{
				rf.SendHeartbeat()
			}

			time.Sleep(time.Duration(HeartBeatInterval) * time.Millisecond)
		} else if rf.currentRole == CANDIDATE {
			requestVoteArgs := &RequestVoteArgs{}
			requestVoteArgs.Term = rf.currentTerm
			requestVoteArgs.CandidateId = rf.me

			lastLog := rf.logEntries[len(rf.logEntries)-1]
			requestVoteArgs.LastLogIndex = lastLog.Index
			requestVoteArgs.LastLogTerm = lastLog.Term

			// send request vote to all peers
			voteCount := 0
			for _, peer := range rf.peers {
				requestVoteReply := &RequestVoteReply{}
				ok := peer.Call("Raft.RequestVote", requestVoteArgs, requestVoteReply)
				if ok && requestVoteReply.VoteGranted {
					voteCount += 1
				}
			}
			rf.Log(INFO, "Finish one round of request vote: %d votes received", voteCount)
			// ToDo: one tricky case is request vote continue after reset to follower
			if voteCount > len(rf.peers)/2 {
				rf.SwitchRole(LEADER)
				continue
			}

			if rf.checkElectionTimeout() {

				ms := HeartBeatInterval + (rand.Int63() % ElectionTimeout)
				time.Sleep(time.Duration(ms) * time.Millisecond)
				rf.startNewElection()
			}
		} else {
			// pause for a random amount of time between 50 and 350
			// milliseconds.

			if rf.checkElectionTimeout() {
				ms := HeartBeatInterval + (rand.Int63() % ElectionTimeout)
				time.Sleep(time.Duration(ms) * time.Millisecond)
				rf.startNewElection()
			}
		}

	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3B, 3C).
	// Leader Election Initialization 3A
	firstEmptyLog := &LogEntry{}
	firstEmptyLog.Index = 0
	firstEmptyLog.Term = 0
	rf.logEntries = append(rf.logEntries, *firstEmptyLog)
	rf.currentRole = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = make(map[int]int)
	rf.logger = log.New(os.Stdout, "", 0) // 自定义日志前缀后缀
	rf.logLevel = DEBUG
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	ms := 50 + (rand.Int63() % 300)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
