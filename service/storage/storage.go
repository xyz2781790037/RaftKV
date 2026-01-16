package storage

import (
	"RaftKV/tool"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	pb "RaftKV/proto/raftpb"

	"google.golang.org/protobuf/proto"
)

type Store struct {
	State *StateStore
	Log   *LogStore
}
type StateStore struct {
	mu       sync.Mutex
	filePath string
}

func NewRaftStorage(dataDir string, nodeID int64) *Store {
	s := NewStateStore(dataDir, nodeID)
	l := NewLogStore(dataDir, nodeID)
	return &Store{
		State: s,
		Log:   l,
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
func SafeWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return err
	}
	return nil
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
	if err := SafeWriteFile(s.filePath, stateData); err != nil {
		tool.Log.Error("Failed to write data in path", "err", err, "Path", s.filePath)
	} else {
		tool.Log.Info("succeed to write", "rs", rs)
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
		filePath:     filePath,
		snapshotPath: snapshotFilePath,
	}
}

// func (l *LogStore) SaveLogs(logs []*pb.LogEntry) {
// 	l.mu.Lock()
// 	defer l.mu.Unlock()
// 	rl := &pb.RaftLog{
// 		Entries: logs,
// 	}
// 	stateData, err := proto.Marshal(rl)
// 	if err != nil {
// 		tool.Log.Error("Failed to save state", "err", err)
// 		return
// 	}

//		if err := SafeWriteFile(l.filePath, stateData); err != nil {
//			tool.Log.Error("Failed to write data in path", "err", err, "Path", l.filePath)
//		}
//		//else {
//		// 	tool.Log.Info("succeed to write","logs",logs)
//		// }
//	}
func (l *LogStore) AppendLog(logs []*pb.LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.OpenFile(l.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		tool.Log.Error("Failed to write data in path", "err", err, "Path", l.filePath)
	}
	defer f.Close()
	for _, entry := range logs {
		stateData, err := proto.Marshal(entry)
		if err != nil {
			tool.Log.Error("Failed to save state", "err", err)
			return
		}
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(stateData)))

		if _, err := f.Write(lenBuf); err != nil {
			tool.Log.Error("AppendLog write length error", "err", err)
			return
		}

		if _, err := f.Write(stateData); err != nil {
			tool.Log.Error("AppendLog write data error", "err", err)
			return
		}
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
		tool.Log.Info("succeed to write in snapshot")
	}
}

//	func (l *LogStore) LoadLogs() ([]*pb.LogEntry, bool) {
//		l.mu.Lock()
//		defer l.mu.Unlock()
//		data, err := os.ReadFile(l.filePath)
//		if err != nil {
//			if os.IsNotExist(err) {
//				tool.Log.Info("Log is empty")
//				return []*pb.LogEntry{}, true
//			}
//			tool.Log.Error("Failed to read data in path", "err", err, "Path", l.filePath)
//			return nil, false
//		}
//		sl := &pb.RaftLog{}
//		err2 := proto.Unmarshal(data, sl)
//		if err2 != nil {
//			tool.Log.Error("Failed to ", "err", err2)
//			return nil, false
//		}
//		return sl.Entries, true
//	}
func (l *LogStore) LoadLogs() ([]*pb.LogEntry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	f, err := os.Open(l.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			tool.Log.Info("Log file not exist, return empty")
			return []*pb.LogEntry{}, true
		}
		tool.Log.Error("LoadLogs open file error", "err", err)
		return nil, false
	}
	defer f.Close()
	var logs []*pb.LogEntry
	lenBuf := make([]byte, 4)
	for {
		_, err := io.ReadFull(f, lenBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			tool.Log.Error("LoadLogs read length error", "err", err)
			return nil, false
		}
		size := binary.LittleEndian.Uint32(lenBuf)
		data := make([]byte, size)
		_, err = io.ReadFull(f, data)
		if err != nil {
			tool.Log.Error("LoadLogs read data error", "err", err)
			return nil, false
		}
		entry := &pb.LogEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			tool.Log.Error("LoadLogs unmarshal error", "err", err)
			return nil, false
		}
		logs = append(logs, entry)
	}
	return logs, true
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
func (l *LogStore) RewriteLogs(logs []*pb.LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	f, err := os.OpenFile(l.filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		tool.Log.Error("RewriteLogs open file error", "err", err)
		return
	}
	defer f.Close()
	lenBuf := make([]byte, 4)
	for _, entry := range logs {
		data, err := proto.Marshal(entry)
		if err != nil {
			tool.Log.Error("RewriteLogs marshal error", "err", err)
			return
		}
		binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
		if _, err := f.Write(lenBuf); err != nil {
			tool.Log.Error("RewriteLogs write length error", "err", err)
			return
		}
		if _, err := f.Write(data); err != nil {
			tool.Log.Error("RewriteLogs write data error", "err", err)
			return
		}
	}
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
