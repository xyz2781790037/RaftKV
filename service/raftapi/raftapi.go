package raftapi

import (
	"google.golang.org/protobuf/proto"
)

type Raft interface {
	Propose(command proto.Message) (int64, int64, bool)

	GetState() (int64, bool)
	GetRaftStateSize() (int64)
	Snapshot(index int64, snapshot []byte)

	Shutdown()
}
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int64
	SnapshotIndex int64
}
