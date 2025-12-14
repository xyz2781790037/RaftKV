package raft

import (
	pb "RaftKV/proto/raftpb"
)

func (rf *Raft) sendInstallSnapshot(server int, peer *RaftPeer) {

}
func (rf *Raft) sendRequestVote() {
	rf.mu.Lock()
	if rf.killed() || rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.becomeCandidate()
	rf.resetElectionTimer()
	args := pb.RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		
	}
	rf.mu.Unlock()
	for i, peer := range rf.peers {
		if i == int(rf.me) {
			continue
		}
		go func(peers *RaftPeer) {
			reply := &pb.RequestVoteReply{}
			reply, ok := peers.CallRequestVote(&args)
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
				return
			}
			if reply.VoteGranted{
				rf.votes++
				if rf.votes > int32(len(rf.peers) / 2) && rf.state == Candidate && rf.currentTerm == args.Term{
					rf.becomeLeader()
					rf.sendHeartbeatAtOnce()
				}
			}
		}(peer)
	}
}
func (rf *Raft) sendAppendEntries() {
	
}
func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = int64(rf.me)
	rf.votes = 1
	rf.persist()
}
func (rf *Raft) becomeFollower(term int64) {
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = term
	rf.persist()
}
func (rf *Raft)becomeLeader(){
	rf.state =  Leader
	lastLogIndex := rf.getLastLogIndex()
	for i := range rf.peers {
		if i == int(rf.me) {
			continue
		}
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
}