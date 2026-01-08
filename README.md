# RaftKV
### struct
```Go
type InstallSnapshotArgs struct {
	Term              int64  // 领导者任期
	LeaderID          int64  // 领导者 ID
	LastIncludedIndex int64  // 快照的最后一个日志条目索引，包括在内
	LastIncludedTerm  int64  // 快照的最后一个日志条目任期
	Data              []byte // 快照数据
}

type InstallSnapshotReply struct {
	Term int64 // 当前任期，让领导者更新自己的任期
}
```
```Go

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int64 // 候选人的任期
	CandidateID  int64 // 候选人的 ID
	LastLogIndex int64 // 候选人的最后日志条目
	LastLogTerm  int64 // 候选人的最后日志条目的 任期
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64 // 当前任期，让候选人更新自己的任期
	VoteGranted bool  // 候选人是否获得投票
}
```
```Go
type AppendEntriesArgs struct {
	Term         int64      // 领导者任期
	LeaderID     int64      // 领导者 ID：让跟随者知道领导者是谁
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
```