package raft

import (
	// "bytes"
	"RaftKV/service/raftapi"
	"RaftKV/service/storage"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	// "time"
)

type LogEntry struct {
	Command interface{} // 日志条目包含的命令
	Term    int64       // 日志条目的任期号
}
type PeerState int64

const (
	Leader PeerState = iota
	Follower
	Candidate
)

type Raft struct {
	mu          sync.Mutex        // Lock to protect shared access to this peer's state
	peers       []*RaftPeer       // RPC end point64s of all peers
	logStore    *storage.LogStore // Object to hold this peer's persisted state
	stateStore  *storage.StateStore
	me          int64 // this peer's index int64o peers[]
	dead        int32 // set by Kill()
	currentTerm int64
	votedFor    int64
	votes       int32

	log []LogEntry // 日志条目，包含命令和任期号
	// volatile state on all servers
	commitIndex int64 // 已被集群大多数节点提交的日志条目索引
	lastApplied int64 // 此节点已应用到状态机的日志条目索引
	// volatile state on leaders
	nextIndex  []int64 // 对于第i个服务器，领导者发送心跳的时候，从nextIndex[i]开始发送日志条目
	matchIndex []int64 // 对于第i个服务器，从[1, matchIndex[i]]的日志条目和leader的日志条目一致

	state                 PeerState
	resetElectionTimerCh  chan struct{} // 重置选举定时器的通道
	sendHeartbeatAtOnceCh chan struct{} // 立即发送心跳的通道
	electionCh            chan struct{} // 选举定时器超时通知通道
	heartbeatCh           chan struct{} // 心跳定时器超时通知通道
	shutdownCh            chan struct{} // 当节点被杀死时，关闭所有通道
	applyCh              chan raftapi.ApplyMsg
	applyCond             *sync.Cond            // 用于通知 ApplyMsg 的条件变量
	// for 3D
	lastIncludedIndex int64 // 快照的最后一个日志条目索引 // 所有以rf.log为基础的索引都要减去这个值
	lastIncludedTerm  int64 // 快照的最后一个日志条目任期
}
func (rf *Raft) getLogTerm(index int64)int64{
	if index == rf.lastIncludedIndex{
		return rf.lastIncludedTerm
	}
	relativeIndex := index - rf.lastIncludedIndex
	if relativeIndex < 0 || relativeIndex >= Len(rf.log) {
		return -1
	}
	return rf.log[relativeIndex].Term
}
func (rf *Raft)getLastLogIndex() int64{
	if len(rf.log) == 0{
		return int64(rf.lastIncludedIndex)
	}
	return rf.lastIncludedIndex + Len(rf.log)
}
func (rf *Raft) getLastLogTerm() int64{
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}
func (rf *Raft) GetState() (int64, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == Leader
	return term, isLeader
}
func (rf *Raft) persist() {

}
func (rf *Raft) readPersist(data []byte) {

}

func (rf *Raft) PersistBytes() int64 {
	return 0
}

type InstallSnapshotArgs struct {
	Term              int64  // 领导者任期
	LeaderId          int64  // 领导者 ID
	LastIncludedIndex int64  // 快照的最后一个日志条目索引，包括在内
	LastIncludedTerm  int64  // 快照的最后一个日志条目任期
	Data              []byte // 快照数据
}

type InstallSnapshotReply struct {
	Term int64 // 当前任期，让领导者更新自己的任期
}

func (rf *Raft) Snapshot(index int64, snapshot []byte) {

}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int64 // 候选人的任期
	CandidateId  int64 // 候选人的 ID
	LastLogIndex int64 // 候选人的最后日志条目
	LastLogTerm  int64 // 候选人的最后日志条目的 任期
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64 // 当前任期，让候选人更新自己的任期
	VoteGranted bool  // 候选人是否获得投票
}

func (rf *Raft) Propose(command any) (int64, int64, bool) {
	var index int64 = -1
	var term int64 = -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.killed() {
		return index,term, false
	}
	index = rf.getLastLogIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})
	rf.persist()
	// 立马发送心跳
	rf.sendHeartbeatAtOnce()
	return index,term, true
}
func (rf *Raft) Shutdown() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// close(rf.shutdownCh) // 关闭所有计时器
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionCh:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if !isLeader {
				go rf.sendRequestVote()
			}
		case <-rf.heartbeatCh:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if isLeader {
				go rf.sendAppendEntries()
			}
		case <-rf.shutdownCh:
			return
		}
	}
}

func (rf *Raft) electionTimer() {
	timer := time.NewTimer(RandomElectionTimeout())
	defer timer.Stop()

	for !rf.killed() {
		select {
		case <-timer.C:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if !isLeader {
				select {
				case rf.electionCh <- struct{}{}:
				default:
				}
			}
			timer.Reset(RandomElectionTimeout())
		case <-rf.resetElectionTimerCh:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(RandomElectionTimeout())
		case <-rf.shutdownCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

type AppendEntriesArgs struct {
	Term         int64      // 领导者任期
	LeaderId     int64      // 领导者 ID：让跟随者知道领导者是谁
	PrevLogIndex int64      // 领导者的nextIndex[i] - 1
	PrevLogTerm  int64      // 领导者的log[prevLogIndex].Term
	Entries      []LogEntry // 日志条目，以便将新条目附加到日志，如果是新领导者，则为空
	LeaderCommit int64      // 领导者已提交的日志条目索引
}

type AppendEntriesReply struct {
	Term    int64 // 当前任期号，以便领导者更新自己的任期号
	Success bool  // 成功附加日志条目到跟随者的日志
	// 下面两个字段用于处理冲突
	ConflictIndex int64 // 冲突的日志条目索引，nextIndex[i] = ConflictIndex
	ConflictTerm  int64 // 冲突的日志条目任期号
}

func Make(peers []*RaftPeer, me int64,
	stateStore *storage.StateStore,
	logStore *storage.LogStore, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.stateStore = stateStore
	rf.logStore = logStore
	rf.me = me
	rf.mu = sync.Mutex{}
	atomic.StoreInt32(&rf.dead, 0)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votes = 1

	rf.resetElectionTimerCh = make(chan struct{}, 1)
	rf.sendHeartbeatAtOnceCh = make(chan struct{}, 1)
	rf.electionCh = make(chan struct{}, 1)
	rf.heartbeatCh = make(chan struct{}, 1)
	rf.shutdownCh = make(chan struct{}, 1)
	go rf.applier()
	go rf.electionTimer()
	// go rf.hea
	go rf.ticker()
	return rf
}
func (rf *Raft) applier() {

}
func RandomElectionTimeout() time.Duration {
	// 测试器要求你的 Raft 在旧 leader 失败后的 5 秒内选出一个新的 leader。
	return time.Duration(250+rand.Intn(400)) * time.Millisecond
}
func (rf *Raft) resetElectionTimer() {
	select {
	case rf.resetElectionTimerCh <- struct{}{}:
	default:
	}
}
func (rf *Raft) sendHeartbeatAtOnce() {
	select {
	case rf.sendHeartbeatAtOnceCh <- struct{}{}:
	default:
	}
}
func Len(command any) int64 {
	v := reflect.ValueOf(command)

	switch v.Kind() {
	case reflect.Array, reflect.Slice, reflect.String, reflect.Map, reflect.Chan:
		return int64(v.Len())
	default:
		return 0
	}
}