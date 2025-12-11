package raftapi

type Raft interface {
	Propose(command interface{}) (int64, int64, bool)

	GetState() (int64, bool)

	// For Snaphots (3D)
	Snapshot(index int64, snapshot []byte)

	Shutdown()
}
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
