package kvraft

import (
	kvpb "RaftKV/proto/kvpb"
	pb "RaftKV/proto/raftpb"
	"fmt"

	// "RaftKV/service/bgdb"
	"RaftKV/service/bgdb"
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
	// 继承 gRPC 生成的 Unimplemented 接口
	kvpb.UnimplementedRaftKVServer

	mu      sync.Mutex
	me      int64
	rf      raftapi.Raft
	applyCh chan raftapi.ApplyMsg
	dead    int32 // 原子操作，用于 Kill

	maxraftstate     int64 // 快照阈值
	lastSnapshotTime time.Time
	lastAppliedIndex int64

	// --- 状态机 (State Machine) ---
	db *bgdb.KVEngine

	// --- 通知机制 ---
	notifyChs map[int64]chan OpResult
	applyCond *sync.Cond
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

	if kv.db.IsDuplicate(op.ClientId, op.SeqId) {
		tool.Log.Debug("请求已完成(Cache Hit)！", "op", op.Operation)
		kv.mu.Unlock()
		return OpResult{Err: kvpb.Error_OK, Value: "", Term: int64(term)}
	}
	if kv.notifyChs == nil {
		kv.notifyChs = make(map[int64]chan OpResult)
		// tool.Log.Info("notifyChs通知为空")
	}
	ch := make(chan OpResult, 1)
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

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
		// tool.Log.Info("true to return res")
		return res
	case <-ctx.Done():
		kv.mu.Lock()
		isDup := false
		if op.Operation != "Get" {
			isDup = kv.db.IsDuplicate(op.ClientId, op.SeqId)
		}
		if isDup {
			kv.mu.Unlock()
			return OpResult{Err: kvpb.Error_OK, Value: "", Term: int64(term)}
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for kv.lastAppliedIndex < readIndex {
		if kv.killed() {
			reply.Err = kvpb.Error_ERR_WRONG_LEADER
			return reply, nil
		}

		if ctx.Err() != nil {
			reply.Err = kvpb.Error_ERR_TIMEOUT
			return reply, nil
		}

		kv.applyCond.Wait()
	}
	val, err := kv.db.Get(args.Key)
	if err == nil {
		reply.Value = val
		reply.Err = kvpb.Error_OK
	} else {
		reply.Value = ""
		reply.Err = kvpb.Error_ERR_NO_KEY
	}

	return reply, nil
}
func (kv *KVServer) BatchGet(ctx context.Context, args *kvpb.BatchGetArgs) (*kvpb.BatchGetReply, error) {
	reply := &kvpb.BatchGetReply{
		Values: make(map[string]string),
	}
	readIndex, isLeader := kv.rf.ReadIndex()
	if !isLeader {
		reply.Err = kvpb.Error_ERR_WRONG_LEADER
		return reply, nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for kv.lastAppliedIndex < readIndex {
		if kv.killed() {
			reply.Err = kvpb.Error_ERR_WRONG_LEADER
			return reply, nil
		}

		if ctx.Err() != nil {
			reply.Err = kvpb.Error_ERR_TIMEOUT
			return reply, nil
		}

		kv.applyCond.Wait()
	}
	for _, key := range args.Keys {
		val, err := kv.db.Get(key)
		if err == nil {
			reply.Values[key] = val
		} else {
			reply.Values[key] = ""
		}
	}
	tool.Log.Debug("BatchGet received", "count", len(args.Keys))
	reply.Err = kvpb.Error_OK
	return reply, nil
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
	kv.rf.Shutdown()
	kv.db.Close()
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
		applyCh:      make(chan raftapi.ApplyMsg, 10000),
		notifyChs:    make(map[int64]chan OpResult),
	}
	kv.applyCond = sync.NewCond(&kv.mu)
	dbPath := fmt.Sprintf("data/kv-%d", me)
	db, err := bgdb.NewKVEngine(dbPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to open DB: %v", err))
	}
	kv.db = db

	kv.rf = raft.Make(server, me, persister, kv.applyCh)
	meta, snapshotData, ok := persister.Log.LoadSnapshot()

	if ok && len(snapshotData) > 0 {
		tool.Log.Debug("Found snapshot on disk", "bytes", len(snapshotData), "lastIndex", meta.LastIncludedIndex)
		kv.Restore(snapshotData)

		// 更新 lastAppliedIndex，确保 Get 能正常工作
		kv.lastAppliedIndex = meta.LastIncludedIndex
		tool.Log.Info("Raft found snapshot", "index", meta.LastIncludedIndex)
	} else {
		tool.Log.Warn("No snapshot found on disk (or empty)", "ok", ok, "len", len(snapshotData))
	}
	go kv.applier()

	return kv
}
func (kv *KVServer) applier() {

	var lastSnapshottedIndex int64 = 0
	kv.lastSnapshotTime = time.Now()
	batchOps := make([]raftapi.ApplyMsg, 0, 100)

	for {

		if kv.killed() {
			return
		}

		firstMsg, ok := <-kv.applyCh
		if !ok {
			return
		}
		batchOps = append(batchOps, firstMsg)

	DrainLoop:
		for len(batchOps) < 100 {
			select {
			case msg, ok := <-kv.applyCh:
				if !ok {
					return
				}
				batchOps = append(batchOps, msg)
			default:

				break DrainLoop
			}
		}

		kv.mu.Lock()

		for _, msg := range batchOps {
			if msg.CommandValid {
				if msg.Command == nil {
					continue
				}

				op := &kvpb.Op{}
				cmdBytes, ok := msg.Command.([]byte)
				if ok {
					if err := proto.Unmarshal(cmdBytes, op); err != nil {
						tool.Log.Error("Failed to unmarshal command", "err", err)
						continue
					}
				} else if cmdOp, ok := msg.Command.(*kvpb.Op); ok {
					op = cmdOp
				} else {
					tool.Log.Error("Invalid command type, expected []byte")
					continue
				}

				var result OpResult
				result.Err = kvpb.Error_OK
				currentTerm, _ := kv.rf.GetState()
				result.Term = currentTerm

				isDup := false
				if op.Operation != "Get" {
					isDup = kv.db.IsDuplicate(op.ClientId, op.SeqId)
				}

				if isDup {
					result.Err = kvpb.Error_OK
					tool.Log.Debug("命中去重，跳过写入", "ClientId", op.ClientId, "SeqId", op.SeqId, "Key", op.Key)
				} else {
					if op.Operation != "Get" {
						err := kv.db.ApplyCommand(op.Operation, []byte(op.Key), []byte(op.Value), op.ClientId, op.SeqId)
						if err != nil {
							panic(fmt.Sprintf("🔥 严重错误: 状态机应用失败! Index=%d, Err=%v", msg.CommandIndex, err))
						}
					}
				}

				kv.lastAppliedIndex = msg.CommandIndex
				if ch, ok := kv.notifyChs[msg.CommandIndex]; ok {
					select {
					case ch <- result:
					default:
						tool.Log.Warn("❌ 通知发送失败：通道已满或无人接收", "index", msg.CommandIndex)
					}
					delete(kv.notifyChs, msg.CommandIndex)
				}

			} else if msg.SnapshotValid {
				kv.Restore(msg.Snapshot)

				for index := range kv.notifyChs {
					delete(kv.notifyChs, index)
				}

				kv.lastAppliedIndex = msg.SnapshotIndex
				lastSnapshottedIndex = msg.SnapshotIndex
			}
		}
		if kv.maxraftstate != -1 {
			lastIndex := kv.lastAppliedIndex
			currentSize := kv.rf.GetRaftStateSize()
			isOverSize := currentSize >= kv.maxraftstate
			isEnoughGap := (lastIndex - lastSnapshottedIndex) >= 5000
			isCoolDown := time.Since(kv.lastSnapshotTime) >= 20*time.Second
			isEmergency := currentSize >= kv.maxraftstate*2
			if isEmergency || (isOverSize && isEnoughGap && isCoolDown) {
				snapshotData := kv.Snapshot()
				kv.rf.Snapshot(lastIndex, snapshotData)
				lastSnapshottedIndex = lastIndex
				kv.lastSnapshotTime = time.Now()
			}
		}
		kv.applyCond.Broadcast()
		kv.mu.Unlock()
		batchOps = batchOps[:0]
	}
}
func (kv *KVServer) Snapshot() []byte {
	data, err := kv.db.GetSnapshot()
	if err != nil {
		tool.Log.Error("Snapshot failed", "err", err)
		return nil
	}
	return data
}

// Restore 从快照恢复 (用于 InstallSnapshot)
func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}
	err := kv.db.RestoreSnapshot(data)
	if err != nil {
		tool.Log.Error("Restore failed", "err", err)
	}
}
