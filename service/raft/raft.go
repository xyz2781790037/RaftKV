package raft

import (
	// "bytes"
	pb "RaftKV/proto/raftpb"
	"RaftKV/service/raftapi"
	"RaftKV/service/storage"
	"RaftKV/tool"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"google.golang.org/protobuf/proto"
)

type PeerState int64

const (
	Leader PeerState = iota
	Follower
	Candidate
)

type Raft struct {
	mu          sync.Mutex        // Lock to protect shared access to this peer's state
	peers       []*RaftPeer       // RPC end point64s of all peers
	store    *storage.Store // Object to hold this peer's persisted state
	me          int64 // this peer's index int64o peers[]
	dead        int32 // set by Kill()
	pb.UnimplementedRaftServer
	currentTerm int64
	votedFor    int64
	votes       int32

	log []*pb.LogEntry // 日志条目，包含命令和任期号
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
	applyCh               chan raftapi.ApplyMsg
	applyCond             *sync.Cond // 用于通知 ApplyMsg 的条件变量
	// for 3D
	lastIncludedIndex int64 // 快照的最后一个日志条目索引 // 所有以rf.log为基础的索引都要减去这个值
	lastIncludedTerm  int64 // 快照的最后一个日志条目任期
}

func (rf *Raft) getLogTerm(index int64) int64 {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	relativeIndex := index - rf.lastIncludedIndex
	if relativeIndex < 0 || relativeIndex >= Len(rf.log) {
		return -1
	}
	return rf.log[relativeIndex].Term
}
func (rf *Raft) getLastLogIndex() int64 {
	if len(rf.log) == 0 {
		return int64(rf.lastIncludedIndex)
	}
	return rf.lastIncludedIndex + Len(rf.log)
}
func (rf *Raft) getLastLogTerm() int64 {
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
func (rf *Raft) GetRaftStateSize() int64 {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    
    // 你需要调用存储层的接口来获取当前字节数
    // 如果你的 store 封装了 persister，通常是 store.RaftStateSize()
    return rf.store.RaftStateSize()
}
func (rf *Raft) persist() {
	rf.store.State.SaveState(rf.currentTerm, rf.votedFor)
	rf.store.Log.SaveLogs(rf.log)
}
func (rf *Raft) readPersist(data []byte) {
	term, votedFor, okState := rf.store.State.LoadState()
	if okState {
		rf.currentTerm = term
		rf.votedFor = votedFor
	} else {
		// 第一次启动，初始化默认值
		rf.currentTerm = 0
		rf.votedFor = -1
	}

	snapMeta, _, okSnap := rf.store.Log.LoadSnapshot()
	if okSnap && snapMeta != nil {
        rf.lastIncludedIndex = snapMeta.LastIncludedIndex
        rf.lastIncludedTerm = snapMeta.LastIncludedTerm
        // 注意：snapshotData (第二个返回值) 在这里不需要处理
        // 因为应用层 (KVServer) 会自己加载快照来恢复状态机
    } else {
        rf.lastIncludedIndex = 0
        rf.lastIncludedTerm = 0
    }
	logs, okLog := rf.store.Log.LoadLogs()
    if okLog {
        rf.log = logs
    } else {
        rf.log = make([]*pb.LogEntry, 0)
    }
}

func (rf *Raft) Snapshot(index int64, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed(){
		return
	}
	if index <= rf.lastIncludedIndex{
		return
	}
	sliceIndex := index - rf.lastIncludedIndex
	var lastIncludedTerm int64
	if sliceIndex > 0 && sliceIndex <= Len(rf.log){
		lastIncludedTerm = rf.log[sliceIndex].Term
	}else{
		if index > rf.getLastLogIndex(){
			return
		}
		lastIncludedTerm = rf.lastIncludedTerm
	}

	if sliceIndex < Len(rf.log){
		newLog := make([]*pb.LogEntry,len(rf.log[sliceIndex:]))
		copy(newLog,rf.log[sliceIndex:])
		rf.log = newLog
	}else{
		rf.log = make([]*pb.LogEntry, 0)
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = lastIncludedTerm

	if index > rf.commitIndex{
		rf.commitIndex = index
	}
	if index > rf.lastApplied{
		rf.lastApplied = index
	}
	rf.store.Log.SaveSnapshot(rf.lastIncludedTerm,rf.lastIncludedIndex,snapshot)
	rf.store.Log.SaveLogs(rf.log)
	rf.store.State.SaveState(rf.currentTerm,rf.votedFor)
	if rf.state == Leader {
		rf.sendHeartbeatAtOnce()
	}
	rf.applyCond.Signal()
}

func (rf *Raft) Propose(command proto.Message) (int64, int64, bool) {
	var index int64 = -1
	var term int64 = -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.killed() {
		return index, term, false
	}
	cmdBytes, err := proto.Marshal(command)
	if err != nil {
		tool.Log.Error("Propose marshal error", "err", err)
		return -1, -1, false
	}
	index = rf.getLastLogIndex() + 1
	term = rf.currentTerm
	rf.log = append(rf.log, &pb.LogEntry{
		Term:    term,
		Command: cmdBytes,
	})
	rf.persist()
	// 立马发送心跳
	rf.sendHeartbeatAtOnce()
	return index, term, true
}
func (rf *Raft) Shutdown() {
	atomic.StoreInt32(&rf.dead, 1)
	// 关闭所有计时器
	close(rf.shutdownCh)
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
func (rf *Raft) heartbeatTimer() {
	timer := time.NewTimer(StableHeartbeatTimeout())
	defer timer.Stop()
	for !rf.killed() {
		select {
		case <-timer.C:
			rf.mu.Lock()
			isLeader := rf.state == Leader
			rf.mu.Unlock()
			if isLeader {
				select {
				case rf.heartbeatCh <- struct{}{}:
				default:
				}
			}
			timer.Reset(StableHeartbeatTimeout())
		case <-rf.sendHeartbeatAtOnceCh:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(0)
		case <-rf.shutdownCh:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
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

func Make(peers []*RaftPeer, me int64,
	state *storage.Store, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.store = state
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
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied && rf.lastIncludedIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.lastApplied < rf.lastIncludedIndex {
			_, snapshotData, ok := rf.store.Log.LoadSnapshot()
			if !ok {
				tool.Log.Error("Failed to load snapshot in applier")
				snapshotData = nil
			}
			snapshotMsg := raftapi.ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      snapshotData,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.lastApplied = rf.lastIncludedIndex

			rf.mu.Unlock()
			rf.applyCh <- snapshotMsg
			continue
		}
		if rf.lastApplied < rf.commitIndex {
			startIndex := rf.lastApplied + 1
			endIndex := rf.commitIndex
			count := endIndex - startIndex + 1
			applyMsgs := make([]raftapi.ApplyMsg, count)
			var i int64 = 0
			for ; i < count; i++ {
				logIndex := startIndex + i
				sliceIndex := logIndex - rf.lastIncludedIndex
				if sliceIndex < 0 || sliceIndex > Len(rf.log) {
					tool.Log.Error("Applier index out of bound", "index", sliceIndex, "logLen", len(rf.log))
					break
				}
				applyMsgs[i] = raftapi.ApplyMsg{
					CommandValid:  true,
					Command:       rf.log[sliceIndex].Command,
					CommandIndex:  logIndex,
					SnapshotValid: false,
				}
			}
			// 乐观更新 lastApplied
			rf.lastApplied = endIndex
			rf.mu.Unlock()

			for _, msg := range applyMsgs {
				rf.applyCh <- msg
			}
			continue
		}
		rf.mu.Unlock()
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

const HeartbeatTimeout = 100

func RandomElectionTimeout() time.Duration {
	return time.Duration(250+rand.Intn(400)) * time.Millisecond
}
func StableHeartbeatTimeout() time.Duration {
	// 测试器要求 leader 每秒发送检测信号 RPC 不超过 10 次。
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}
