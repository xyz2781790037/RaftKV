package kvraft

import (
	kvpb "RaftKV/proto/kvpb"
	pb "RaftKV/proto/raftpb"
	"RaftKV/service/raft"
	"RaftKV/service/raftapi"
	"RaftKV/service/storage"
	"RaftKV/tool"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
)

type OpResult struct {
	Err   kvpb.Error
	Value string
	Term  int64
}
type KVServer struct {
	// 继承 gRPC 生成的 Unimplemented 接口 (必须有)
	kvpb.UnimplementedRaftKVServer

	mu      sync.Mutex
	me      int64
	rf      raftapi.Raft
	applyCh chan raftapi.ApplyMsg
	dead    int32 // 原子操作，用于 Kill

	maxraftstate     int64 // 快照阈值
	lastAppliedIndex int64
	// --- 状态机 (State Machine) ---
	db             map[string]string // 内存数据库
	lastOperations map[int64]int64   // 去重表: ClientId -> LastSeqId

	// --- 通知机制 ---
	// LogIndex -> Result Channel
	// RPC 把 Log 扔给 Raft 后，就订阅这个 Channel 等结果
	notifyChs map[int64]chan OpResult
}

func (kv *KVServer) GetRaft() pb.RaftServer {
	return kv.rf.(*raft.Raft)
}
func (kv *KVServer) waitRaft(op *kvpb.Op) OpResult {
	kv.mu.Lock()
	index, term, isLeader := kv.rf.Propose(op)
	if !isLeader {
		kv.mu.Unlock()
		return OpResult{
			kvpb.Error_ERR_WRONG_LEADER, "", term,
		}
	}

	if lastSeq, ok := kv.lastOperations[op.ClientId]; ok && lastSeq >= op.SeqId {
		tool.Log.Info("请求已完成！", "op", op.Operation)
		val := ""
		kv.mu.Unlock()
		return OpResult{Err: kvpb.Error_OK, Value: val, Term: int64(term)}
	}
	if kv.notifyChs == nil {
		kv.notifyChs = make(map[int64]chan OpResult)
		tool.Log.Info("notifyChs通知为空")
	}
	ch := make(chan OpResult, 1)
	kv.notifyChs[index] = ch
	kv.mu.Unlock()
	tool.Log.Info("notifyChs通知传入成功")
	// defer func() {
	// 	kv.mu.Lock()
	// 	delete(kv.notifyChs, index)
	// 	kv.mu.Unlock()
	// }()
	ctx, cancel := context.WithTimeout(context.Background(), 2000*time.Millisecond)
	defer cancel()
	select {
	case res := <-ch:
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
		// 检查 Term 匹配 (防止脑裂后的脏读)
		if res.Term != int64(term) {
			return OpResult{
				kvpb.Error_ERR_WRONG_LEADER, "", term,
			}
		}
		tool.Log.Info("true to return res")
		return res
	case <-ctx.Done():
		// 千萬別直接返回 Timeout！先看看是不是已經做完了！
		kv.mu.Lock()
		if lastSeq, ok := kv.lastOperations[op.ClientId]; ok && lastSeq >= op.SeqId {
			// 如果是 Get，還得去讀一下 Value
			val := ""
			kv.mu.Unlock()
			tool.Log.Info("✅ 虽然超時但检漏成功", "index", index)
			// 返回成功，假裝沒超時！
			return OpResult{Err: kvpb.Error_OK, Value: val, Term: int64(term)}
		}
		kv.mu.Unlock()
		return OpResult{
			Err: kvpb.Error_ERR_TIMEOUT,
		}
	}
}
func (kv *KVServer) Get(ctx context.Context, args *kvpb.GetArgs) (*kvpb.GetReply, error) {
	reply := &kvpb.GetReply{}
	readIndex, isLeader := kv.rf.ReadIndex()
	if !isLeader {
		reply.Err = kvpb.Error_ERR_WRONG_LEADER
		return reply, nil
	}

	timer := time.NewTimer(1000 * time.Millisecond)
	defer timer.Stop()
	for {
		kv.mu.Lock()
		if kv.lastAppliedIndex >= readIndex {
			if val, ok := kv.db[args.Key]; ok {
				reply.Value = val
				reply.Err = kvpb.Error_OK
			} else {
				reply.Value = ""
				reply.Err = kvpb.Error_ERR_NO_KEY
			}
			kv.mu.Unlock()
			return reply, nil
		}
		kv.mu.Unlock()
		select{
		case <-timer.C:
			reply.Err = kvpb.Error_ERR_TIMEOUT
			return reply,nil
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func (kv *KVServer) PutAppend(ctx context.Context, args *kvpb.PutAppendArgs) (*kvpb.PutAppendReply, error) {
	op := &kvpb.Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
	}
	res := kv.waitRaft(op)
	reply := kvpb.PutAppendReply{
		Err: res.Err,
	}
	return &reply, nil
}
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func StartKVServer(server *raft.PeerManager, me int64, persister *storage.Store, maxraftstate int64) *KVServer {
	kv := &KVServer{
		mu:           sync.Mutex{},
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg, 100),

		// 必须初始化 Map，否则写入时会 panic
		db:             make(map[string]string),
		lastOperations: make(map[int64]int64),
		notifyChs:      make(map[int64]chan OpResult),
	}
	kv.rf = raft.Make(server, me, persister, kv.applyCh)
	meta, snapshotData, ok := persister.Log.LoadSnapshot()

	if ok && len(snapshotData) > 0 {
		tool.Log.Info("Found snapshot on disk", "bytes", len(snapshotData), "lastIndex", meta.LastIncludedIndex)
		kv.Restore(snapshotData)
		tool.Log.Info("KV State restored from snapshot", "db_size", len(kv.db))
	} else {
		tool.Log.Warn("No snapshot found on disk (or empty)", "ok", ok, "len", len(snapshotData))
	}
	go kv.applier()

	return kv
}
func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		tool.Log.Info("进入applier")
		if kv.killed() {
			tool.Log.Info("applier killed")
			return
		}
		kv.mu.Lock()
		if msg.CommandValid {
			tool.Log.Info("进入applier1")
			cmdBytes, ok := msg.Command.([]byte)
			if !ok {
				// 防御性编程：万一传过来的不是 bytes，打印个日志跳过
				tool.Log.Error("Invalid command type, expected []byte")
				continue
			}

			// 2. 创建一个新的空对象
			op := &kvpb.Op{}

			// 3. 反序列化：把 bytes 还原成 Op 结构体
			// 注意：这里需要引入 google.golang.org/protobuf/proto
			if err := proto.Unmarshal(cmdBytes, op); err != nil {
				tool.Log.Error("Failed to unmarshal command", "err", err)
				continue
			}
			var result OpResult
			result.Err = kvpb.Error_OK

			currentTerm, _ := kv.rf.GetState()
			result.Term = currentTerm
			isRepeated := false
			if op.Operation != "Get" {
				lastSeq, ok := kv.lastOperations[op.ClientId]
				if ok && lastSeq >= op.SeqId {
					isRepeated = true
				}
				tool.Log.Info("进入oplast", "isRepeated", isRepeated)
			}
			if !isRepeated {
				switch op.Operation {
				case "Put":
					kv.db[op.Key] = op.Value
				case "Append":
					kv.db[op.Key] += op.Value
				case "Delete":
					delete(kv.db, op.Key)
				}
				// if op.Operation != "Get" {
				kv.lastOperations[op.ClientId] = op.SeqId
				// }
			} else {
				result.Err = kvpb.Error_OK
			}
			kv.lastAppliedIndex = msg.CommandIndex
			if ch, ok := kv.notifyChs[msg.CommandIndex]; ok {
				// 强制发送，不要用 select default，看看会不会阻塞
				// 或者加上详细日志
				select {
				case ch <- result:
					tool.Log.Info("✅ 通知成功发送", "index", msg.CommandIndex)
				default:
					tool.Log.Warn("❌ 通知发送失败：通道已满或无人接收", "index", msg.CommandIndex)
				}
				delete(kv.notifyChs, msg.CommandIndex)
			} else {
				// 这行日志很重要！看看是不是根本没找到对应的 Channel
				tool.Log.Warn("❓ 没人等待这个 Index，通知丢弃", "index", msg.CommandIndex)
			}
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
				snapshotData := kv.Snapshot()
				kv.rf.Snapshot(msg.CommandIndex, snapshotData)
			}
		} else if msg.SnapshotValid {
			tool.Log.Info("进入applier2")
			kv.Restore(msg.Snapshot)
			for index := range kv.notifyChs {
				if index <= msg.SnapshotIndex {
					select {
					case kv.notifyChs[index] <- OpResult{Err: kvpb.Error_ERR_WRONG_LEADER, Value: ""}:
					default:
					}

				}
				delete(kv.notifyChs, index)
			}
			tool.Log.Info("Loaded snapshot", "index", msg.SnapshotIndex)
		}
		kv.mu.Unlock()
	}
}
func (kv *KVServer) Snapshot() []byte {
	if kv.db == nil {
		kv.db = make(map[string]string)
	}
	s := &kvpb.KVSnapshot{
		Data:           kv.db,
		LastOperations: kv.lastOperations,
	}
	snapshot, err := proto.Marshal(s)
	if err != nil {
		tool.Log.Error("Failed to Snapshot in kv", "err", err)
		return nil
	}

	return snapshot
}
func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 || data == nil {
		kv.db = make(map[string]string)
		kv.lastOperations = make(map[int64]int64)
		return // 空数据不处理
	}
	store := &kvpb.KVSnapshot{}
	err := proto.Unmarshal(data, store)
	if err != nil {
		tool.Log.Error("Failed to Restore in kv", "err", err)
		return
	}
	kv.db = store.Data
	kv.lastOperations = store.LastOperations
	if kv.db == nil {
		kv.db = make(map[string]string)
	}
	if kv.lastOperations == nil {
		kv.lastOperations = make(map[int64]int64)
	}
}