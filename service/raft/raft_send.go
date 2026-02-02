package raft

import (
	pb "RaftKV/proto/raftpb"
	"RaftKV/tool"
	"fmt"
	"sort"
)

const MaxLogEntriesPerRPC = 2000

func (rf *Raft) sendRequestVote() {
	rf.mu.Lock()
	if rf.killed() || rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.becomeCandidate()
	rf.resetElectionTimer()
	args := pb.RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()
	peers := rf.peers.CloneList()
	for _,peer := range peers {
		if peer.id == rf.me {
			continue
		}
		go func(peer *RaftPeer) {
			reply, ok := peer.CallRequestVote(&args)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.killed() || rf.currentTerm != args.Term || rf.state != Candidate || rf.votedFor != args.CandidateId {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				tool.Log.Info("调用persist in send ")
				rf.persist()
				return
			}
			if reply.VoteGranted {
				rf.votes++
				if rf.votes >= int32(rf.peers.QuorumSize()) && rf.state == Candidate && rf.currentTerm == args.Term {
					rf.becomeLeader()
					rf.sendHeartbeatAtOnce()
				}
			}
		}(peer)
	}
}
func (rf *Raft) sendAppendEntries() {
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	leaderID := rf.me
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()
	heartbeatAckCount := 1
	peers := rf.peers.CloneList()
	notified := false
	for _, peer := range peers {
		if peer.id == rf.me {
			continue
		}
		go func(peer *RaftPeer) {
			rf.mu.Lock()
			if rf.killed() || rf.state != Leader || rf.currentTerm != currentTerm {
				rf.mu.Unlock()
				return
			}
			i := peer.id
			nextIndex := rf.nextIndex[i]
			if nextIndex <= rf.lastIncludedIndex {
				rf.mu.Unlock()
				go rf.sendInstallSnapshot(i, peer)
				return
			}
			
			prevLogIndex := nextIndex - 1
			prevLogTerm := rf.getLogTerm(prevLogIndex)

			entries := []*pb.LogEntry{}
			// 假设一个 Follower 掉线了 1 小时，落后了 100 万条日志（几百 MB 数据）。 当它重连时，Leader 会尝试把这 100 万条日志打包进这一个 RPC 发送出去（进行改进：限制这个数量）
			if nextIndex <= rf.getLastLogIndex() {
				entries = rf.getEntriesToSend(nextIndex)
			}
			rf.mu.Unlock()
			args := pb.AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}
			reply, ok := peer.CallAppendEntries(&args)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.killed() || rf.state != Leader || rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				tool.Log.Info("调用persist in send ")
				rf.persist()
				return
			}
			if reply.Success {
				rf.matchIndex[i] = prevLogIndex + int64(len(args.Entries))
				rf.nextIndex[i] = rf.matchIndex[i] + 1
				rf.updateCommitIndex()
				if len(entries) == 0 {
					heartbeatAckCount++
					if heartbeatAckCount == rf.peers.QuorumSize() && !notified {
						notified = true
						close(rf.readIndexNotifyCh)
						rf.readIndexNotifyCh = make(chan struct{})
					}
				}
			} else {
				if reply.ConflictTerm == -1 {
					rf.nextIndex[i] = reply.ConflictIndex
				} else {
					var conflictIndex int64 = -1
					for i := rf.getLastLogIndex(); i >= rf.lastIncludedIndex; i-- {
						if rf.getLogTerm(i) == reply.ConflictTerm {
							conflictIndex = i
							break
						}
					}
					if conflictIndex != -1 {
						rf.nextIndex[i] = conflictIndex + 1
					} else {
						rf.nextIndex[i] = reply.ConflictIndex
					}
				}
			}
		}(peer)
	}

}
func (rf *Raft) sendInstallSnapshot(server int64, peer *RaftPeer) {
	snapMeta, snapshotData, ok := rf.store.Log.LoadSnapshot()
	if !ok {
		tool.Log.Error("Failed to load snapshot in applier")
		snapshotData = nil
	}
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	args := pb.InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: snapMeta.LastIncludedIndex,
		LastIncludedTerm:  snapMeta.LastIncludedTerm,
		Data:              snapshotData,
	}
	rf.mu.Unlock()

	reply, ok := peer.CallInstallSnapshot(&args)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.currentTerm != args.Term || rf.state != Leader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		tool.Log.Info("调用persist in send ")
		rf.persist()
		return
	}
	if args.LastIncludedIndex > rf.matchIndex[server] {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}

}

func (rf *Raft) updateCommitIndex() {
	// 假设 rf.matchIndex 已经是 []int64 类型了
	// 假设 rf.commitIndex, rf.getLastLogIndex() 都是 int64
	allIDs := rf.peers.ListIDs()
	// 1. 集所有节点的 matchIndex (用 int64 切片)
	// 这里要注意：make 的长度是 int，但里面的内容是 int64收
	matchIndexes := make([]int64, 0, len(allIDs))
	// matchIndexes := make(map[int]int64)

	// 3. 把值从 Map 提取到切片中
	for _, id := range allIDs {
		if id == rf.me {
			// Leader 自己的进度就是最后一条日志的索引
			matchIndexes = append(matchIndexes, rf.getLastLogIndex())
			continue
		}
		// 从 Map 中取值并放入切片
		matchIndexes = append(matchIndexes, rf.matchIndex[id])
	}

	// 2. 排序 (关键修改！)
	// sort.Ints 不能排 int64，必须用 sort.Slice 自定义比较函数
	sort.Slice(matchIndexes, func(i, j int) bool {
		return matchIndexes[i] < matchIndexes[j]
	})

	// 3. 取中位数
	// 比如 3 个节点，len/2 = 1，取排序后第 2 个
	// 比如 5 个节点，len/2 = 2，取排序后第 3 个
	// 这个位置的值，保证了至少有 (N/2 + 1) 个节点达到了这个值
	quorum := rf.peers.QuorumSize()
    newCommitIndex := matchIndexes[len(allIDs) - quorum]

	// 4. 检查 Term (Raft 论文 Figure 8 的限制)
	// 只有当前 Term 的日志被复制过半，才能提交
	// 注意：所有比较都要用 int64
	if newCommitIndex > rf.commitIndex {
		// rf.getLogTerm 接收 int64，rf.currentTerm 是 int64
		if rf.getLogTerm(newCommitIndex) == rf.currentTerm {
			rf.commitIndex = newCommitIndex

			// 唤醒 Apply 协程，去把日志应用到状态机
			rf.applyCond.Signal()

			// 调试日志（可选）
			// DPrintf("Leader %d updated commitIndex to %d", rf.me, rf.commitIndex)
		}
	}
}
func (rf *Raft) getEntriesToSend(nextIndex int64) []*pb.LogEntry {
	lastLogIndex := rf.getLastLogIndex()

	// 如果需要的日志比我有的还新，或者需要发快照，直接返回 nil
	if nextIndex > lastLogIndex {
		return nil
	}

	// 真正的切片起始下标 (因为 log[0] 对应 lastIncludedIndex)
	// 假设 lastIncludedIndex=0, log=[dummy, A, B]. nextIndex=1.
	// realIndex = 1 - 0 = 1. 取 log[1] (A). 正确。
	realIndex := nextIndex - rf.lastIncludedIndex

	// 防御：如果计算出的下标不合法
	if realIndex < 0 || realIndex >= int64(len(rf.log)) {
		return nil
	}

	// 限制单次发送数量 (防止包过大)
	endIndex := lastLogIndex + 1
	if endIndex > nextIndex+MaxLogEntriesPerRPC {
		endIndex = nextIndex + MaxLogEntriesPerRPC
	}

	realEnd := endIndex - rf.lastIncludedIndex
	if realEnd > int64(len(rf.log)) {
		realEnd = int64(len(rf.log))
	}

	//再次防御
	if realIndex >= realEnd {
		return nil
	}

	// 深度拷贝，防止并发问题
	entries := make([]*pb.LogEntry, realEnd-realIndex)
	copy(entries, rf.log[realIndex:realEnd])
	return entries
}
func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = int64(rf.me)
	rf.votes = 1
}
func (rf *Raft) becomeFollower(term int64) {
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = term
}
func (rf *Raft) becomeLeader() {
	if rf.state == Leader {
		return
	}
	rf.state = Leader
	lastLogIndex := rf.getLastLogIndex()
	allIDs := rf.peers.ListIDs()
	for _, id := range allIDs {
		if id == rf.me {
			continue
		}
		rf.nextIndex[id] = lastLogIndex + 1
		rf.matchIndex[id] = 0
	}
	go func(){
		rf.Propose(nil)
	}()
	fmt.Println("\033[1;36m", rf.me, "become new Leader", "Term=", rf.currentTerm, "\033[0m")
}
