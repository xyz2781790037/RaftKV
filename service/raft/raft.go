package raft
import (
	// "bytes"
	"sync"
	"sync/atomic"
	// "time"
)
type LogEntry struct {
	Command interface{} // 日志条目包含的命令
	Term    int         // 日志条目的任期号
}
type PeerState int

const (
	Leader PeerState = iota
	Follower
	Candidate
)
type Raft struct{
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*RaftPeer // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
}
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	return term, isleader
}
func (rf *Raft) persist() {

}
func (rf *Raft) readPersist(data []byte){

}
func (rf *Raft) PersistBytes() int{
	return 0;
}
type InstallSnapshotArgs struct {
	Term              int    // 领导者任期
	LeaderId          int    // 领导者 ID
	LastIncludedIndex int    // 快照的最后一个日志条目索引，包括在内
	LastIncludedTerm  int    // 快照的最后一个日志条目任期
	Data              []byte // 快照数据
}

type InstallSnapshotReply struct {
	Term int // 当前任期，让领导者更新自己的任期
}
func (rf *Raft) Snapshot(index int, snapshot []byte) {

}
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选人的任期
	CandidateId  int // 候选人的 ID
	LastLogIndex int // 候选人的最后日志条目
	LastLogTerm  int // 候选人的最后日志条目的 任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期，让候选人更新自己的任期
	VoteGranted bool // 候选人是否获得投票
}
func (rf *Raft) Start(command interface{}) (int, int, bool){
	index := -1
	term := -1
	return index, term, true
}
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// close(rf.shutdownCh) // 关闭所有计时器
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) ticker(){

}
type AppendEntriesArgs struct {
	Term         int        // 领导者任期
	LeaderId     int        // 领导者 ID：让跟随者知道领导者是谁
	PrevLogIndex int        // 领导者的nextIndex[i] - 1
	PrevLogTerm  int        // 领导者的log[prevLogIndex].Term
	Entries      []LogEntry // 日志条目，以便将新条目附加到日志，如果是新领导者，则为空
	LeaderCommit int        // 领导者已提交的日志条目索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期号，以便领导者更新自己的任期号
	Success bool // 成功附加日志条目到跟随者的日志
	// 下面两个字段用于处理冲突
	ConflictIndex int // 冲突的日志条目索引，nextIndex[i] = ConflictIndex
	ConflictTerm  int // 冲突的日志条目任期号
}
func Make(peers []*RaftPeer, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
		return rf
	}
func (rf *Raft) applier(){
	
}