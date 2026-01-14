package storage

import (
	"RaftKV/tool"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	pb "RaftKV/proto/raftpb"
	"google.golang.org/protobuf/proto"
)
type Store struct {
	State *StateStore
	Log *LogStore
}
type StateStore struct {
	mu       sync.Mutex
	filePath string
}
func NewStore(dataDir string, nodeID int64) *Store{
	s := NewStateStore(dataDir,nodeID)
	l := NewLogStore(dataDir,nodeID)
	return &Store{
		State: s,
		Log: l,
	}
}
func NewStateStore(dataDir string, nodeID int64) *StateStore {
	filePath := filepath.Join(dataDir, "raftNode_"+strconv.Itoa(int(nodeID))+".dat")
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		tool.Log.Error("Failed to create data directory", "err", err)
	}
	s := &StateStore{
		filePath: filePath,
	}
	return s
}
func (s *StateStore) SaveState(term int64, votedFor int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs := &pb.RaftState{
		CurrentTerm: term,
		VotedFor:    votedFor,
	}
	stateData, err := proto.Marshal(rs)
	if err != nil {
		tool.Log.Error("Failed to save state", "err", err)
		return
	}
	if err := os.WriteFile(s.filePath, stateData, 0644); err != nil {
		tool.Log.Error("Failed to write data in path", "err", err, "Path", s.filePath)
	} else {
		tool.Log.Info("succeed to write")
	}
}
func (s *StateStore) LoadState() (term int64, votedFor int64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			tool.Log.Info("State is empty")
			return 0, -1, true
		}
		tool.Log.Warn("Failed to read data in path", "err", err, "Path", s.filePath)
		return -1, -1, false
	}
	rs := &pb.RaftState{}
	err2 := proto.Unmarshal(data, rs)
	if err2 != nil {
		tool.Log.Error("Failed to ", "err", err2)
		return -1, -1, false
	}
	return rs.CurrentTerm, rs.VotedFor, true
}

type LogStore struct {
	mu           sync.Mutex
	filePath     string
	snapshotPath string
}

func NewLogStore(dataDir string, nodeID int64) *LogStore {
	filePath := filepath.Join(dataDir, "raftLogNode_"+strconv.Itoa(int(nodeID))+".dat")
	snapshotFilePath := filepath.Join(dataDir, "raftSnapshotNode_"+strconv.Itoa(int(nodeID))+".dat")
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		tool.Log.Error("Failed to create data directory", "err", err)
	}
	return &LogStore{
		filePath: filePath,
		snapshotPath: snapshotFilePath,
	}
}
func (l *LogStore) SaveLogs(logs []*pb.LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rl := &pb.RaftLog{
		Entries: logs,
	}
	stateData, err := proto.Marshal(rl)
	if err != nil {
		tool.Log.Error("Failed to save state", "err", err)
		return
	}

	if err := os.WriteFile(l.filePath, stateData, 0644); err != nil {
		tool.Log.Error("Failed to write data in path", "err", err, "Path", l.filePath)
	} else {
		tool.Log.Info("succeed to write")
	}
}
func (l *LogStore) SaveSnapshot(lastTerm int64, lastIndex int64, data []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fs := &pb.FullSnapshot{
		Metadata: &pb.SnapshotState{
			LastIncludedIndex: lastIndex,
			LastIncludedTerm:  lastTerm,
		},
		Data: data,
	}
	stateData, err := proto.Marshal(fs)
	if err != nil {
		tool.Log.Error("Failed to save state", "err", err)
		return
	}

	if err := os.WriteFile(l.snapshotPath, stateData, 0644); err != nil {
		tool.Log.Error("Failed to write data in path", "err", err, "Path", l.snapshotPath)
	} else {
		tool.Log.Info("succeed to write")
	}
}
func (l *LogStore) LoadLogs() ([]*pb.LogEntry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	data, err := os.ReadFile(l.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			tool.Log.Info("Log is empty")
			return []*pb.LogEntry{}, true
		}
		tool.Log.Error("Failed to read data in path", "err", err, "Path", l.filePath)
		return nil, false
	}
	sl := &pb.RaftLog{}
	err2 := proto.Unmarshal(data, sl)
	if err2 != nil {
		tool.Log.Error("Failed to ", "err", err2)
		return nil, false
	}
	return sl.Entries, true
}
func (l *LogStore) LoadSnapshot() (*pb.SnapshotState, []byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	data, err := os.ReadFile(l.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			tool.Log.Info("snapShot is empty")
			return nil, nil, true
		}
		tool.Log.Error("Failed to read data in path", "err", err, "Path", l.snapshotPath)
		return nil, nil, false
	}
	sl := &pb.FullSnapshot{}
	err2 := proto.Unmarshal(data, sl)
	if err2 != nil {
		tool.Log.Error("Failed to ", "err", err2)
		return nil, nil, false
	}
	return sl.Metadata, sl.Data, true
}
func (s *Store) RaftStateSize() int64 {
    var totalSize int64 = 0
    if info, err := os.Stat(s.State.filePath); err == nil {
        totalSize += info.Size()
    }
    if info, err := os.Stat(s.Log.filePath); err == nil {
        totalSize += info.Size()
    }
    return totalSize
}