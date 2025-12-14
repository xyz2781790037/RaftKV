package raft

import (
	"context"
)
 

func (rf *Raft) RequestVote(ctx context.Context, args *RequestVoteArgs) (*RequestVoteReply, error) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    reply := &RequestVoteReply{}
    if rf.killed() {
        return reply, nil 
    }
    if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return reply,nil
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
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
		rf.persist()
	}else{
		reply.VoteGranted = false
	}

    return reply, nil
}

func (rf *Raft) AppendEntries(ctx context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    
    reply := &AppendEntriesReply{}
    
    if rf.killed() {
        return reply, nil
    }
    
    
    return reply, nil
}

func (rf *Raft) InstallSnapshot(ctx context.Context, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    
    reply := &InstallSnapshotReply{}
    
    if rf.killed() {
        return reply, nil
    }
    
    
    return reply, nil
}