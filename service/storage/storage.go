// package storage

// import (
// 	"RaftKV/tool"
// 	"bufio"
// 	"encoding/binary"
// 	"io"
// 	"os"
// 	"path/filepath"
// 	"strconv"
// 	"sync"

// 	pb "RaftKV/proto/raftpb"

// 	"google.golang.org/protobuf/proto"
// )

// type Store struct {
// 	State *StateStore
// 	Log   *LogStore
// }
// type StateStore struct {
// 	mu       sync.Mutex
// 	filePath string
// }

// func NewRaftStorage(dataDir string, nodeID int64) *Store {
// 	s := NewStateStore(dataDir, nodeID)
// 	l := NewLogStore(dataDir, nodeID)
// 	return &Store{
// 		State: s,
// 		Log:   l,
// 	}
// }
// func NewStateStore(dataDir string, nodeID int64) *StateStore {
// 	filePath := filepath.Join(dataDir, "raftNode_"+strconv.Itoa(int(nodeID))+".dat")
// 	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
// 		tool.Log.Error("Failed to create data directory", "err", err)
// 	}
// 	s := &StateStore{
// 		filePath: filePath,
// 	}
// 	return s
// }
// func SafeWriteFile(path string, data []byte) error {
// 	dir := filepath.Dir(path)
// 	if err := os.MkdirAll(dir, 0755); err != nil {
// 		return err
// 	}
//     // 使用 Sync 保证安全
//     f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
//     if err != nil { return err }
//     defer f.Close()
//     if _, err := f.Write(data); err != nil { return err }
//     if err := f.Sync(); err != nil { return err }
//     return nil
// }
// func (s *StateStore) SaveState(term int64, votedFor int64) {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	rs := &pb.RaftState{
// 		CurrentTerm: term,
// 		VotedFor:    votedFor,
// 	}
// 	stateData, err := proto.Marshal(rs)
// 	if err != nil {
// 		tool.Log.Error("Failed to save state", "err", err)
// 		return
// 	}
// 	if err := SafeWriteFile(s.filePath, stateData); err != nil {
// 		tool.Log.Error("Failed to write data in path", "err", err, "Path", s.filePath)
// 	}
// }
// func (s *StateStore) LoadState() (term int64, votedFor int64, ok bool) {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	data, err := os.ReadFile(s.filePath)
// 	if err != nil {
// 		if os.IsNotExist(err) {
// 			tool.Log.Info("State is empty")
// 			return 0, -1, true
// 		}
// 		tool.Log.Warn("Failed to read data in path", "err", err, "Path", s.filePath)
// 		return -1, -1, false
// 	}
// 	rs := &pb.RaftState{}
// 	err2 := proto.Unmarshal(data, rs)
// 	if err2 != nil {
// 		tool.Log.Error("Failed to ", "err", err2)
// 		return -1, -1, false
// 	}
// 	return rs.CurrentTerm, rs.VotedFor, true
// }

// type LogStore struct {
// 	mu           sync.Mutex
// 	filePath     string
// 	snapshotPath string
// }

// func NewLogStore(dataDir string, nodeID int64) *LogStore {
// 	filePath := filepath.Join(dataDir, "raftLogNode_"+strconv.Itoa(int(nodeID))+".dat")
// 	snapshotFilePath := filepath.Join(dataDir, "raftSnapshotNode_"+strconv.Itoa(int(nodeID))+".dat")
// 	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
// 		tool.Log.Error("Failed to create data directory", "err", err)
// 	}
// 	return &LogStore{
// 		filePath:     filePath,
// 		snapshotPath: snapshotFilePath,
// 	}
// }
// func (l *LogStore) AppendLog(logs []*pb.LogEntry) {
//     l.mu.Lock()
//     defer l.mu.Unlock()
//     dir := filepath.Dir(l.filePath)
// 	if _, err := os.Stat(dir); os.IsNotExist(err) {
// 		os.MkdirAll(dir, 0755)
// 	}

//     f, err := os.OpenFile(l.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
//     if err != nil {
//         tool.Log.Error("Failed to write data in path", "err", err, "Path", l.filePath)
//         return
//     }
//     defer f.Close()
//     w := bufio.NewWriterSize(f, 4096*4) // 给大点，16KB

//     lenBuf := make([]byte, 4)
//     for _, entry := range logs {
//         stateData, err := proto.Marshal(entry)
//         if err != nil {
//             continue
//         }
//         binary.LittleEndian.PutUint32(lenBuf, uint32(len(stateData)))
//         if _, err := w.Write(lenBuf); err != nil {
//             return
//         }
//         if _, err := w.Write(stateData); err != nil {
//             return
//         }
//     }
    
//     if err := w.Flush(); err != nil {
//         tool.Log.Error("AppendLog flush error", "err", err)
//         return
//     }
//     if err := f.Sync(); err != nil {
//         tool.Log.Error("AppendLog Sync error", "err", err)
//     }
// }
// func (l *LogStore) RewriteLogs(logs []*pb.LogEntry) {
// 	l.mu.Lock()
// 	defer l.mu.Unlock()

// 	// 使用 O_TRUNC 清空文件
// 	f, err := os.OpenFile(l.filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
// 	if err != nil {
// 		tool.Log.Error("RewriteLogs open file error", "err", err)
// 		return
// 	}
// 	defer f.Close()

// 	w := bufio.NewWriterSize(f, 4096) 

// 	lenBuf := make([]byte, 4)
// 	for _, entry := range logs {
// 		data, err := proto.Marshal(entry)
// 		if err != nil {
// 			continue
// 		}
// 		binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
		
// 		// 这里的 Write 是写内存 buffer，极快
// 		if _, err := w.Write(lenBuf); err != nil { 
// 			return
// 		}
// 		if _, err := w.Write(data); err != nil {
// 			return
// 		}
// 	}
// 	if err := w.Flush(); err != nil {
// 		tool.Log.Error("RewriteLogs flush error", "err", err)
// 		return
// 	}
    
//     // 确保落盘
//     f.Sync()
// }
// func (l *LogStore) SaveSnapshot(lastTerm int64, lastIndex int64, data []byte) {
// 	l.mu.Lock()
// 	defer l.mu.Unlock()
// 	fs := &pb.FullSnapshot{
// 		Metadata: &pb.SnapshotState{
// 			LastIncludedIndex: lastIndex,
// 			LastIncludedTerm:  lastTerm,
// 		},
// 		Data: data,
// 	}
// 	stateData, err := proto.Marshal(fs)
// 	if err != nil {
// 		tool.Log.Error("Failed to save state", "err", err)
// 		return
// 	}

//     // 【快照也必须 Sync】
//     f, err := os.OpenFile(l.snapshotPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
//     if err != nil {
//         tool.Log.Error("Failed to open snapshot", "err", err)
//         return
//     }
//     defer f.Close()
    
//     if _, err := f.Write(stateData); err != nil {
//         tool.Log.Error("Failed to write snapshot", "err", err)
//         return
//     }
//     if err := f.Sync(); err != nil {
//         tool.Log.Error("Failed to sync snapshot", "err", err)
//         return
//     }
    
// 	tool.Log.Info("succeed to write in snapshot")
// }

// func (l *LogStore) LoadLogs() ([]*pb.LogEntry, bool) {
// 	l.mu.Lock()
// 	defer l.mu.Unlock()

// 	f, err := os.Open(l.filePath)
// 	if err != nil {
// 		if os.IsNotExist(err) {
// 			tool.Log.Info("Log file not exist, return empty")
// 			return []*pb.LogEntry{}, true
// 		}
// 		tool.Log.Error("LoadLogs open file error", "err", err)
// 		return nil, false
// 	}
// 	defer f.Close()
// 	var logs []*pb.LogEntry
// 	lenBuf := make([]byte, 4)
// 	for {
// 		_, err := io.ReadFull(f, lenBuf)
// 		if err == io.EOF {
// 			break
// 		}
// 		if err != nil {
// 			tool.Log.Error("LoadLogs read length error", "err", err)
// 			return nil, false
// 		}
// 		size := binary.LittleEndian.Uint32(lenBuf)
// 		data := make([]byte, size)
// 		_, err = io.ReadFull(f, data)
// 		if err != nil {
// 			tool.Log.Error("LoadLogs read data error", "err", err)
// 			return nil, false
//         }
//         entry := &pb.LogEntry{}
//         if err := proto.Unmarshal(data, entry); err != nil {
//             tool.Log.Error("LoadLogs unmarshal error", "err", err)
//             return nil, false
//         }
//         logs = append(logs, entry)
//     }
//     return logs, true
// }
// func (l *LogStore) LoadSnapshot() (*pb.SnapshotState, []byte, bool) {
//     l.mu.Lock()
//     defer l.mu.Unlock()
//     data, err := os.ReadFile(l.snapshotPath)
//     if err != nil {
//         if os.IsNotExist(err) {
//             tool.Log.Info("snapShot is empty")
//             return nil, nil, true
//         }
//         tool.Log.Error("Failed to read data in path", "err", err, "Path", l.snapshotPath)
//         return nil, nil, false
//     }
//     sl := &pb.FullSnapshot{}
//     err2 := proto.Unmarshal(data, sl)
//     if err2 != nil {
//         tool.Log.Error("Failed to ", "err", err2)
//         return nil, nil, false
//     }
//     return sl.Metadata, sl.Data, true
// }

// // func (l *LogStore) RewriteLogs(logs []*pb.LogEntry) {
// //     l.mu.Lock()
// //     defer l.mu.Unlock()

// //     f, err := os.OpenFile(l.filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
// //     if err != nil {
// //         tool.Log.Error("RewriteLogs open file error", "err", err)
// //         return
// //     }
// //     defer f.Close()
// //     lenBuf := make([]byte, 4)
// //     for _, entry := range logs {
// //         data, err := proto.Marshal(entry)
// //         if err != nil {
// //             tool.Log.Error("RewriteLogs marshal error", "err", err)
// //             return
// //         }
// //         binary.LittleEndian.PutUint32(lenBuf, uint32(len(data)))
// //         if _, err := f.Write(lenBuf); err != nil {
// //             tool.Log.Error("RewriteLogs write length error", "err", err)
// //             return
// //         }
// //         if _, err := f.Write(data); err != nil {
// //             tool.Log.Error("RewriteLogs write data error", "err", err)
// //             return
// //         }
// //     }
    
// //     f.Sync()
// // }
// func (s *Store) RaftStateSize() int64 {
//     var totalSize int64 = 0
//     if info, err := os.Stat(s.State.filePath); err == nil {
//         totalSize += info.Size()
//     }
//     if info, err := os.Stat(s.Log.filePath); err == nil {
//         totalSize += info.Size()
//     }
//     return totalSize
// }
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

func NewRaftStorage(dataDir string, nodeID int64) *Store {
	s := NewStateStore(dataDir, nodeID)
	l := NewLogStore(dataDir, nodeID)
	return &Store{
		State: s,
		Log:   l,
	}
}

func (s *Store) RaftStateSize() int64 {
	var totalSize int64 = 0
	if info, err := os.Stat(s.State.filePath); err == nil {
		totalSize += info.Size()
	}
	
	// 从内存中直接获取安全精准的大小
	s.Log.mu.Lock()
	totalSize += s.Log.offset
	s.Log.mu.Unlock()
	
	return totalSize
}

// =================== State Store ===================

type StateStore struct {
	mu       sync.Mutex
	filePath string
}

func NewStateStore(dataDir string, nodeID int64) *StateStore {
	filePath := filepath.Join(dataDir, "raftNode_"+strconv.Itoa(int(nodeID))+".dat")
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		tool.Log.Error("Failed to create data directory", "err", err)
	}
	return &StateStore{
		filePath: filePath,
	}
}

func SafeWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Sync()
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
	if err2 := proto.Unmarshal(data, rs); err2 != nil {
		tool.Log.Error("Failed to unmarshal state", "err", err2)
		return -1, -1, false
	}
	return rs.CurrentTerm, rs.VotedFor, true
}

// =================== Log Store (Append-Only 重构版) ===================

type LogStore struct {
	mu           sync.Mutex
	filePath     string
	snapshotPath string
	
	fd           *os.File            // 常驻文件句柄，避免反复 Open/Close
	entries      []*pb.LogEntry      // 内存镜像，用于智能比对
	offsetMap    map[uint64]int64    // Index -> 文件物理起始 Offset
	offset       int64               // 当前文件的真实写入游标
}

func NewLogStore(dataDir string, nodeID int64) *LogStore {
	filePath := filepath.Join(dataDir, "raftLogNode_"+strconv.Itoa(int(nodeID))+".dat")
	snapshotFilePath := filepath.Join(dataDir, "raftSnapshotNode_"+strconv.Itoa(int(nodeID))+".dat")
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		tool.Log.Error("Failed to create data directory", "err", err)
	}

	// 初始化时打开读写句柄并常驻
	fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		tool.Log.Error("Failed to open log store file", "err", err)
	}

	return &LogStore{
		filePath:     filePath,
		snapshotPath: snapshotFilePath,
		fd:           fd,
		entries:      make([]*pb.LogEntry, 0),
		offsetMap:    make(map[uint64]int64),
	}
}

// LoadLogs 引擎启动时加载全量日志并重建 Index 索引
func (l *LogStore) LoadLogs() ([]*pb.LogEntry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.fd.Seek(0, io.SeekStart)
	l.entries = make([]*pb.LogEntry, 0)
	l.offsetMap = make(map[uint64]int64)
	l.offset = 0

	lenBuf := make([]byte, 4)
	for {
		startOffset := l.offset
		if _, err := io.ReadFull(l.fd, lenBuf); err != nil {
			if err == io.EOF { break }
			// 🚨 发生断电脏写，安全截断受损尾部
			tool.Log.Warn("LoadLogs: Detected torn write, truncating tail", "offset", startOffset)
			l.fd.Truncate(startOffset)
			break
		}
		
		size := binary.LittleEndian.Uint32(lenBuf)
		data := make([]byte, size)
		if _, err := io.ReadFull(l.fd, data); err != nil {
			tool.Log.Warn("LoadLogs: Detected torn data, truncating tail", "offset", startOffset)
			l.fd.Truncate(startOffset)
			break
		}

		entry := &pb.LogEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			tool.Log.Error("LoadLogs unmarshal error", "err", err)
			return nil, false
		}

		l.entries = append(l.entries, entry)
		l.offsetMap[uint64(entry.Index)] = startOffset
		l.offset += int64(4 + size)
	}

	// 拷贝一份给 Raft，防止 Raft 在外部无锁污染底层数据
	res := make([]*pb.LogEntry, len(l.entries))
	copy(res, l.entries)
	return res, true
}

// RewriteLogs 智能重写引擎（核心魔改点）
// 外部 Raft 依然传入全量数组，但底层只做 $O(N)$ 增量写入或 $O(1)$ 截断
func (l *LogStore) RewriteLogs(logs []*pb.LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 1. 如果 Raft 传了空数组，清空一切
	if len(logs) == 0 {
		l.fd.Truncate(0)
		l.fd.Seek(0, io.SeekStart)
		l.entries = nil
		l.offsetMap = make(map[uint64]int64)
		l.offset = 0
		return
	}

	canIncremental := false
	matchLen := 0

	// 2. 检查能否进行增量操作（第一条日志的 Index 和内部一致）
	if len(l.entries) > 0 && logs[0].Index == l.entries[0].Index {
		canIncremental = true
		for i := 0; i < len(logs) && i < len(l.entries); i++ {
			// 找到最长一致前缀
			if logs[i].Index == l.entries[i].Index && logs[i].Term == l.entries[i].Term {
				matchLen++
			} else {
				break
			}
		}
	}

	if canIncremental {
		// 【完美命中】Raft 传进来的数组和磁盘一模一样，直接返回，0 磁盘开销！
		if matchLen == len(logs) && matchLen == len(l.entries) {
			return
		}

		// 【遇到截断/回退】Raft 要丢弃部分尾部日志
		var truncOffset int64 = 0
		if matchLen > 0 {
			if matchLen < len(l.entries) {
				// 获取第一条需要丢弃日志的物理偏移量
				firstMismatchIndex := l.entries[matchLen].Index
				truncOffset = l.offsetMap[uint64(firstMismatchIndex)]
			} else {
				truncOffset = l.offset
			}
		}

		if truncOffset != l.offset {
			l.fd.Truncate(truncOffset) // O(1) 修改文件系统元数据，瞬间斩断
			l.fd.Seek(truncOffset, io.SeekStart)
			l.offset = truncOffset
		}
		l.entries = l.entries[:matchLen]

		// 【追加新日志】把超出 matchLen 后的新日志一把追加
		newLogs := logs[matchLen:]
		if len(newLogs) > 0 {
			l.appendLogsLocked(newLogs)
		}
		l.fd.Sync()
		return
	}

	// 3. 【触发回退机制】Raft 做了快照 (Snapshot)，日志前缀被截断了，需要一次全量覆盖写
	l.fd.Truncate(0)
	l.fd.Seek(0, io.SeekStart)
	l.entries = make([]*pb.LogEntry, 0, len(logs))
	l.offsetMap = make(map[uint64]int64)
	l.offset = 0

	l.appendLogsLocked(logs)
	l.fd.Sync()
}

// AppendLog 直接追加新日志接口
func (l *LogStore) AppendLog(logs []*pb.LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.appendLogsLocked(logs)
	l.fd.Sync()
}

// appendLogsLocked 底层用户态缓冲批量写入（无系统调用风暴）
func (l *LogStore) appendLogsLocked(logs []*pb.LogEntry) {
	if len(logs) == 0 {
		return
	}

	// 在内存中分配一块大 Buffer，合并成 1 次系统调用
	buf := make([]byte, 0, 1024)
	var sizeBuf [4]byte

	for _, entry := range logs {
		l.offsetMap[uint64(entry.Index)] = l.offset

		data, _ := proto.Marshal(entry)
		binary.LittleEndian.PutUint32(sizeBuf[:], uint32(len(data)))
		
		buf = append(buf, sizeBuf[:]...)
		buf = append(buf, data...)

		l.entries = append(l.entries, entry)
		l.offset += int64(4 + len(data))
	}
	
	// 一把梭哈进内核
	l.fd.Write(buf)
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

	f, err := os.OpenFile(l.snapshotPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		tool.Log.Error("Failed to open snapshot", "err", err)
		return
	}
	defer f.Close()

	if _, err := f.Write(stateData); err != nil {
		tool.Log.Error("Failed to write snapshot", "err", err)
		return
	}
	if err := f.Sync(); err != nil {
		tool.Log.Error("Failed to sync snapshot", "err", err)
		return
	}
	tool.Log.Info("succeed to write in snapshot")
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
	if err2 := proto.Unmarshal(data, sl); err2 != nil {
		tool.Log.Error("Failed to unmarshal snapshot", "err", err2)
		return nil, nil, false
	}
	return sl.Metadata, sl.Data, true
}