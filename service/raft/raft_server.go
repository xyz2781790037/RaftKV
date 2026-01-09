package raft

import (
	pb "RaftKV/proto/raftpb"
	"context"
)
 

func (rf *Raft) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    reply := &pb.RequestVoteReply{}
	select{
	case <-ctx.Done():
        return reply, nil 
    default:
    }
    if rf.killed() {
        return reply, nil 
    }
	persistNeeded := false
    if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return reply,nil
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		persistNeeded = true
	}
	reply.Term = rf.currentTerm
	upToDate := func() bool{
		lastLogIndex := rf.getLastLogIndex()
		lastLogTerm := rf.getLastLogTerm()
		if args.LastLogTerm != lastLogTerm {
			return args.LastLogTerm > lastLogTerm
		}
		return args.LastLogIndex >= lastLogIndex
	}()
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
		persistNeeded = true
	}else{
		reply.VoteGranted = false
	}
	if persistNeeded { 
		rf.persist() 
	}
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
	if args.Term < rf.currentTerm{
		reply.Success = false
		return reply,nil
	}
	persistNeeded := false
	if args.Term > rf.currentTerm{
		rf.becomeFollower(args.Term)
		persistNeeded = true
	}
    rf.resetElectionTimer()
	lastLogIndex := rf.getLastLogIndex()
	if args.PrevLogIndex > lastLogIndex || args.PrevLogIndex < rf.lastIncludedIndex{
		reply.Success = false
		if args.PrevLogIndex > lastLogIndex{
			reply.ConflictIndex = lastLogIndex + 1
			reply.ConflictTerm = -1
		}else{
			reply.ConflictIndex = rf.lastIncludedIndex + 1
			reply.ConflictTerm = -1
		}
		if persistNeeded {
			rf.persist()
		}
		return reply, nil
	}
    if rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm{
		reply.Success = false
		reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex && rf.getLogTerm(i - 1) == reply.ConflictTerm{
			i--
		}
		reply.ConflictIndex = i

		if persistNeeded { 
			rf.persist() 
		}
        return reply, nil
	}
	logInsertIndex := args.PrevLogIndex + 1
	var newEntriesStartIndex int64 = 0
	for {
		if newEntriesStartIndex >= Len(args.Entries){
			break
		}
		if logInsertIndex > rf.getLastLogIndex(){
			rf.log = append(rf.log, args.Entries[newEntriesStartIndex:]...)
			persistNeeded = true
			break
		}

		currentTerm := rf.getLogTerm(logInsertIndex)
		newEntryTerm := args.Entries[newEntriesStartIndex].Term
		if currentTerm != newEntryTerm{
			sliceIndex := int(logInsertIndex - rf.lastIncludedIndex)
			rf.log = rf.log[:sliceIndex]
			rf.log = append(rf.log, args.Entries[newEntriesStartIndex:]...)
			persistNeeded = true
			break
		}

		logInsertIndex++
        newEntriesStartIndex++
	}

	if args.LeaderCommit > rf.commitIndex{
		indexOfLastNewEntry := args.PrevLogIndex + Len(args.Entries)
		if args.LeaderCommit < indexOfLastNewEntry{
			rf.commitIndex = args.LeaderCommit
		}else{
			rf.commitIndex = indexOfLastNewEntry
		}
		rf.applyCond.Signal()
	}
	if persistNeeded {
        rf.persist()
    }

    reply.Success = true
    return reply, nil
}

func (rf *Raft) InstallSnapshot(ctx context.Context, args *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    
    reply := &pb.InstallSnapshotReply{}
    
    if rf.killed() {
        return reply, nil
    }
    
    
    return reply, nil
}