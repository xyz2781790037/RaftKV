package raft

import (
	// "bytes"
	pb "RaftKV/proto/raftpb"
	"RaftKV/service/raftapi"
	"RaftKV/service/storage"
	"RaftKV/tool"
	"context"
	"math/rand"
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
	mu    sync.Mutex     // Lock to protect shared access to this peer's state
	peers *PeerManager   // RPC end point64s of all peers
	store *storage.Store // Object to hold this peer's persisted state
	me    int64          // this peer's index int64o peers[]
	dead  int32          // set by Kill()
	pb.UnimplementedRaftServer
	currentTerm int64
	votedFor    int64
	votes       int32

	log []*pb.LogEntry // 日志条目，包含命令和任期号
	// volatile state on all servers
	commitIndex int64 // 已被集群大多数节点提交的日志条目索引
	lastApplied int64 // 此节点已应用到状态机的日志条目索引
	// volatile state on leaders
	nextIndex  map[int64]int64 // 对于第i个服务器，领导者发送心跳的时候，从nextIndex[i]开始发送日志条目
	matchIndex map[int64]int64 // 对于第i个服务器，从[1, matchIndex[i]]的日志条目和leader的日志条目一致

	state                 PeerState
	resetElectionTimerCh  chan struct{} // 重置选举定时器的通道
	sendHeartbeatAtOnceCh chan struct{} // 立即发送心跳的通道
	electionCh            chan struct{} // 选举定时器超时通知通道
	heartbeatCh           chan struct{} // 心跳定时器超时通知通道
	shutdownCh            chan struct{} // 当节点被杀死时，关闭所有通道
	readIndexNotifyCh     chan struct{} //读取数据的通道
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
	if relativeIndex < 0 || relativeIndex >= int64(len(rf.log)) {
		return -1
	}
	return rf.log[relativeIndex].Term
}
func (rf *Raft) getLastLogIndex() int64 {
	if int64(len(rf.log)) == 0 {
		return int64(rf.lastIncludedIndex)
	}
	return rf.lastIncludedIndex + int64(len(rf.log)) - 1
}
func (rf *Raft) getLastLogTerm() int64 {
	if int64(len(rf.log)) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[int64(len(rf.log))-1].Term
}
func (rf *Raft) ReadIndex() (int64, bool) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, false
	}
	readIndex := rf.commitIndex
	rf.sendHeartbeatAtOnce()
	rf.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	select {
	case <-rf.readIndexNotifyCh:
		return readIndex, true
	case <-ctx.Done():
		return -1, false
	}
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
	// tool.Log.Info("调用Save in persist")
	rf.store.State.SaveState(rf.currentTerm, rf.votedFor)
	// rf.store.Log.AppendLog(rf.log)
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
		dummy := &pb.LogEntry{Term: rf.lastIncludedTerm, Command: nil}
		rf.log = append([]*pb.LogEntry{dummy}, logs...)
	} else {
		rf.log = []*pb.LogEntry{{Term: 0, Command: nil}}
	}
}

func (rf *Raft) Snapshot(index int64, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}
	if index <= rf.lastIncludedIndex {
		return
	}
	sliceIndex := index - rf.lastIncludedIndex
	var lastIncludedTerm int64
	if sliceIndex > 0 && sliceIndex <= int64(len(rf.log)) {
		lastIncludedTerm = rf.log[sliceIndex].Term
	} else {
		if index > rf.getLastLogIndex() {
			return
		}
		lastIncludedTerm = rf.lastIncludedTerm
	}

	if sliceIndex < int64(len(rf.log)) {
		newLog := make([]*pb.LogEntry, len(rf.log[sliceIndex:]))
		copy(newLog, rf.log[sliceIndex:])
		rf.log = newLog
	} else {
		rf.log = make([]*pb.LogEntry, 0)
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = lastIncludedTerm

	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	rf.store.Log.SaveSnapshot(rf.lastIncludedTerm, rf.lastIncludedIndex, snapshot)
	rf.store.Log.RewriteLogs(rf.log)
	tool.Log.Info("调用Save in snapshot")
	rf.store.State.SaveState(rf.currentTerm, rf.votedFor)
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
	var cmdBytes []byte
	var err error
	if command != nil {
		cmdBytes, err = proto.Marshal(command)
		if err != nil {
			tool.Log.Error("Propose marshal error", "err", err)
			return -1, -1, false
		}
	} else {
		cmdBytes = []byte{}
	}
	index = rf.getLastLogIndex() + 1
	term = rf.currentTerm
	log := &pb.LogEntry{
		Term:    term,
		Command: cmdBytes,
	}
	rf.log = append(rf.log, log)
	rf.store.Log.AppendLog([]*pb.LogEntry{log})
	// tool.Log.Info("no调用persist in start() ")
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
				tool.Log.Info("Start to a new elecion", "id", rf.me)
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

func Make(peers *PeerManager, me int64,
	state *storage.Store, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.store = state
	rf.me = me
	rf.mu = sync.Mutex{}
	rf.applyCond = sync.NewCond(&rf.mu)
	atomic.StoreInt32(&rf.dead, 0)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votes = 1
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.nextIndex = make(map[int64]int64)
	rf.matchIndex = make(map[int64]int64)

	rf.log = []*pb.LogEntry{{Term: 0, Command: nil}}
	rf.resetElectionTimerCh = make(chan struct{}, 1)
	rf.sendHeartbeatAtOnceCh = make(chan struct{}, 1)
	rf.electionCh = make(chan struct{}, 1)
	rf.heartbeatCh = make(chan struct{}, 1)
	rf.shutdownCh = make(chan struct{}, 1)
	rf.readIndexNotifyCh = make(chan struct{}, 100)
	rf.readPersist(nil)

	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	go rf.applier()
	go rf.electionTimer()
	go rf.heartbeatTimer()
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
				if sliceIndex < 0 || sliceIndex > int64(len(rf.log)) {
					tool.Log.Error("Applier index out of bound", "index", sliceIndex, "log", int64(len(rf.log)))
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

const HeartbeatTimeout = 100

func RandomElectionTimeout() time.Duration {
	return time.Duration(600+rand.Intn(400)) * time.Millisecond
}
func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}
func (rf *Raft) AddNode(id int64, addr string) {
	isNew := rf.peers.AddPeer(id, addr)

	// 2. 共识层状态初始化 (Raft 自己的逻辑)
	if isNew && rf.state == Leader {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 初始化新节点的进度
		rf.nextIndex[id] = rf.getLastLogIndex() + 1
		rf.matchIndex[id] = 0
	}
}

// 封装业务层的 RemoveServer
func (rf *Raft) RemoveNode(id int64) {
	// 1. 网络层移除
	rf.peers.RemovePeer(id)

	// 2. 共识层清理
	rf.mu.Lock()
	defer rf.mu.Unlock()
	delete(rf.nextIndex, id)
	delete(rf.matchIndex, id)
}
