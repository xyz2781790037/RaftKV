package raft

import (
	pb "RaftKV/proto/raftpb"
	"RaftKV/service/raftapi"
	"RaftKV/tool"
	"context"
)

func (rf *Raft) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	tool.Log.Warn("Receive RequestVote 0", "me", rf.me, "candidate", args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &pb.RequestVoteReply{}
	select {
	case <-ctx.Done():
		tool.Log.Warn("exit RequestVote 1", "me", rf.me, "candidate", args.CandidateId)
		return reply, nil
	default:
	}
	if rf.killed() {
		tool.Log.Warn("exit RequestVote 2", "me", rf.me, "candidate", args.CandidateId)
		return reply, nil
	}
	persistNeeded := false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		tool.Log.Warn("exit RequestVote 3", "me", rf.me, "candidate", args.CandidateId)
		return reply, nil
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		persistNeeded = true
	}
	reply.Term = rf.currentTerm
	upToDate := func() bool {
		lastLogIndex := rf.getLastLogIndex()
		lastLogTerm := rf.getLastLogTerm()
		if args.LastLogTerm != lastLogTerm {
			tool.Log.Warn("exit RequestVote 5", "me", rf.me, "candidate", args.CandidateId)
			return args.LastLogTerm > lastLogTerm
		}
		tool.Log.Warn("exit RequestVote 4", "me", rf.me, "candidate", args.CandidateId)
		return args.LastLogIndex >= lastLogIndex
	}()
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
		persistNeeded = true
	} else {
		reply.VoteGranted = false
	}
	if persistNeeded {
		tool.Log.Info("调用persist in raft_server ")
		rf.persist()
	}
	tool.Log.Info("vote end", "result", reply.VoteGranted, "votedId", args.CandidateId, "Term", reply.Term)
	return reply, nil
}

func (rf *Raft) AppendEntries(ctx context.Context, args *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &pb.AppendEntriesReply{}

	if rf.killed() {
		return reply, nil
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		tool.Log.Warn("Reject stale Leader", "me", rf.me, "stale_leader", args.LeaderId, "my_term", rf.currentTerm, "args_term", args.Term)
		return reply, nil
	}
	persistNeeded := false
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		persistNeeded = true
	}
	rf.resetElectionTimer()
	lastLogIndex := rf.getLastLogIndex()
	if args.PrevLogIndex > lastLogIndex || args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		if args.PrevLogIndex > lastLogIndex {
			reply.ConflictIndex = lastLogIndex + 1
			reply.ConflictTerm = -1
		} else {
			reply.ConflictIndex = rf.lastIncludedIndex + 1
			reply.ConflictTerm = -1
		}
		if persistNeeded {
			tool.Log.Info("调用persist in raft_server ")
			rf.persist()
		}
		return reply, nil
	}
	if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex && rf.getLogTerm(i-1) == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i

		if persistNeeded {
			tool.Log.Info("调用persist in raft_server ")
			rf.persist()
		}
		return reply, nil
	}
	logInsertIndex := args.PrevLogIndex + 1
	var newEntriesStartIndex int64 = 0
	for {
		if newEntriesStartIndex >= int64(len(args.Entries)) {
			break
		}
		if logInsertIndex > rf.getLastLogIndex() {
			// rf.log = append(rf.log, args.Entries[newEntriesStartIndex:]...)
			newEntries := args.Entries[newEntriesStartIndex:] // 1. 獲取新日誌切片
			rf.log = append(rf.log, newEntries...)            // 2. 更新內存

			rf.store.Log.AppendLog(newEntries)
			persistNeeded = true
			break
		}

		currentTerm := rf.getLogTerm(logInsertIndex)
		newEntryTerm := args.Entries[newEntriesStartIndex].Term
		if currentTerm != newEntryTerm {
			sliceIndex := int(logInsertIndex - rf.lastIncludedIndex)
			rf.log = rf.log[:sliceIndex]
			rf.log = append(rf.log, args.Entries[newEntriesStartIndex:]...)
			rf.store.Log.AppendLog(args.Entries[newEntriesStartIndex:])
			persistNeeded = true
			break
		}

		logInsertIndex++
		newEntriesStartIndex++
	}

	if args.LeaderCommit > rf.commitIndex {
		indexOfLastNewEntry := args.PrevLogIndex + int64(len(args.Entries))
		if args.LeaderCommit < indexOfLastNewEntry {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = indexOfLastNewEntry
		}
		rf.applyCond.Signal()
	}
	if persistNeeded {
		// tool.Log.Info("调用persist in raft_server ")
		rf.persist()
	}

	reply.Success = true
	return reply, nil
}

func (rf *Raft) InstallSnapshot(ctx context.Context, args *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	rf.mu.Lock()

	reply := &pb.InstallSnapshotReply{}
	if rf.killed() {
		rf.mu.Unlock()
		return reply, nil
	}
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return reply, nil
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.becomeFollower(args.Term)
	}
	rf.resetElectionTimer()
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return reply, nil
	}
	indexDiff := args.LastIncludedIndex - rf.lastIncludedIndex
	sliceIndex := indexDiff - 1
	hasMatchingEntry := false
	if sliceIndex < int64(len(rf.log)) && sliceIndex >= 0 {
		if rf.log[sliceIndex].Term == args.LastIncludedTerm {
			hasMatchingEntry = true
		}
	}
	if hasMatchingEntry {
		newLog := make([]*pb.LogEntry, len(rf.log[sliceIndex+1:]))
		copy(newLog, rf.log[sliceIndex+1:])
		rf.log = newLog
	} else {
		rf.log = make([]*pb.LogEntry, 0)
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.store.Log.SaveSnapshot(rf.lastIncludedTerm, rf.lastIncludedIndex, args.Data)
	rf.store.Log.AppendLog(rf.log)
	tool.Log.Info("调用Save in raft_server")
	rf.store.State.SaveState(rf.currentTerm, rf.votedFor)

	snapshotMsg := raftapi.ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- snapshotMsg
	return reply, nil
}
