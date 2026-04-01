package kvraft

import (
	kvpb "RaftKV/proto/kvpb"
	pb "RaftKV/proto/raftpb"
	"RaftKV/proto/shardpb"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"

	"RaftKV/service/bgdb"
	"RaftKV/service/raft"
	"RaftKV/service/raftapi"
	"RaftKV/service/shardmaster"
	"RaftKV/service/storage"
	"RaftKV/tool"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const NShards = 10
const (
	Serving int = iota // 0: 正常服务
	Pulling            // 1: 正在拉取旧数据，拒绝服务
	Pushing            // 2: 正在送出旧数据，拒绝写入
	GCing              // 3: 等待清理过期数据
)

type OpResult struct {
	Err   kvpb.Error
	Value string
	Term  int64
}
type KVServer struct {
	// 继承 gRPC 生成的 Unimplemented 接口
	kvpb.UnimplementedRaftKVServer

	mu               sync.Mutex
	me               int64
	rf               raftapi.Raft
	applyCh          chan raftapi.ApplyMsg
	dead             int32 // 原子操作，用于 Kill
	maxraftstate     int64 // 快照阈值
	lastSnapshotTime time.Time
	lastAppliedIndex int64

	db *bgdb.KVEngine

	notifyChs      map[int64]chan OpResult
	applyCond      *sync.Cond
	isSnapshotting atomic.Int32
	watchManager   *WatchManager

	gid         int64                   // 当前机器所属的 Replica Group ID
	smClerk     *shardmaster.ShardClerk // 与 ShardMaster 通信的专属客户端
	config      shardpb.Config
	lastConfig  shardpb.Config // 保存上一版路由表，Pulling 时需要知道去哪个旧组拉数据
	shardStates [10]int        // 记录 0~9 号分片当前所处的状态 (默认初始化为 0 即 Serving)
}
func (kv *KVServer) persistState() {
	cfgBytes, _ := proto.Marshal(&kv.config)
	lastCfgBytes, _ := proto.Marshal(&kv.lastConfig)
	state := &kvpb.PersistentState{
		ConfigBytes:     cfgBytes,
		LastConfigBytes: lastCfgBytes,
		ShardStates:     make([]int32, 10),
	}
	for i := 0; i < 10; i++ {
		state.ShardStates[i] = int32(kv.shardStates[i])
	}
	data, err := proto.Marshal(state)
	if err == nil {
		path := fmt.Sprintf("data/kv-group-%d-node-%d/state.json", kv.gid, kv.me)
		os.WriteFile(path, data, 0644)
	} else {
		tool.Log.Error("KVServer 持久化序列化失败", "err", err)
	}
}
func StartKVServer(server *raft.PeerManager, me int64, persister *storage.Store, maxraftstate int64, gid int64, masterAddrs []string) *KVServer {
	kv := &KVServer{
		mu:           sync.Mutex{},
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg, 10000),
		notifyChs:    make(map[int64]chan OpResult),
		watchManager: NewWatchManager(),
		gid:          gid,
		smClerk:      shardmaster.MakeClerk(masterAddrs),
		lastConfig:   shardpb.Config{Num: 0, Shards: make(map[int64]int64), Groups: make(map[int64]*shardpb.GroupServers)},
		shardStates:  [10]int{},
	}
	kv.config = shardpb.Config{Num: 0, Shards: make(map[int64]int64), Groups: make(map[int64]*shardpb.GroupServers)}
	kv.applyCond = sync.NewCond(&kv.mu)
	dbPath := fmt.Sprintf("data/kv-group-%d-node-%d", gid, me)
	db, err := bgdb.NewKVEngine(dbPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to open DB: %v", err))
	}
	kv.db = db
	statePath := fmt.Sprintf("%s/state.json", dbPath)
	if data, err := os.ReadFile(statePath); err == nil {
		state := &kvpb.PersistentState{}
		if err := proto.Unmarshal(data, state); err == nil {
			proto.Unmarshal(state.ConfigBytes, &kv.config)
			proto.Unmarshal(state.LastConfigBytes, &kv.lastConfig)
			for i := 0; i < 10; i++ {
				if i < len(state.ShardStates) {
					kv.shardStates[i] = int(state.ShardStates[i])
				}
			}
			tool.Log.Info("从磁盘完美恢复路由配置与分片状态", "ConfigNum", kv.config.Num)
		}
	}
	kv.rf = raft.Make(server, me, persister, kv.applyCh)
	meta, snapshotData, ok := persister.Log.LoadSnapshot()

	if ok && len(snapshotData) > 0 {
		tool.Log.Debug("Found snapshot on disk", "bytes", len(snapshotData), "lastIndex", meta.LastIncludedIndex)
		kv.Restore(snapshotData)
		kv.lastAppliedIndex = meta.LastIncludedIndex
		tool.Log.Info("Raft found snapshot", "index", meta.LastIncludedIndex)
	} else {
		tool.Log.Warn("No snapshot found on disk (or empty)", "ok", ok, "len", len(snapshotData))
	}
	go kv.applier()
	go kv.pollConfigLoop()
	go kv.migrationLoop()
	// go func() {
	// 	for !kv.killed() {
	// 		time.Sleep(5 * time.Second)
	// 		kv.mu.Lock()
	// 		applied := kv.lastAppliedIndex
	// 		kv.mu.Unlock()
			
	// 		_, isLeader := kv.rf.GetState()
	// 		testKey := "123:33" 
	// 		val, err := kv.db.Get(testKey)
	// 		if err != nil {
	// 			val = "查无此Key"
	// 		}
			
	// 		tool.Log.Info("节点本地探针", "NodeID", kv.me, "IsLeader", isLeader, "Raft进度", applied, "本地查Key结果", val)
	// 	}
	// }()
	return kv
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
		tool.Log.Debug("请求已完成(Cache Hit)！", "op", op.Operation, "key", op.Key)
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
	if !kv.checkGroup(args.Key) {
		reply.Err = kvpb.Error_ERR_WRONG_GROUP
		return reply, nil
	}
	readIndex, isLeader := kv.rf.ReadIndex()
	if !isLeader {
		reply.Err = kvpb.Error_ERR_WRONG_LEADER
		return reply, nil
	}

	for {
		kv.mu.Lock()
		applied := kv.lastAppliedIndex
		isKilled := kv.killed()
		kv.mu.Unlock()

		if isKilled {
			reply.Err = kvpb.Error_ERR_WRONG_LEADER
			return reply, nil
		}
		if applied >= readIndex {
			break
		}
		if ctx.Err() != nil {
			tool.Log.Warn("Get 请求等待状态机超时！", "当前进度", applied, "目标进度", readIndex)
			reply.Err = kvpb.Error_ERR_TIMEOUT
			return reply, nil
		}
		time.Sleep(2 * time.Millisecond)
	}

	val, err := kv.db.Get(args.Key)
	if err == nil {
		reply.Value = val
		reply.Err = kvpb.Error_OK
	} else {
		tool.Log.Warn("BadgerDB 找不到数据！", "Key", args.Key, "错误", err)
		reply.Value = ""
		reply.Err = kvpb.Error_ERR_NO_KEY
	}
	return reply, nil
}
func (kv *KVServer) BatchGet(ctx context.Context, args *kvpb.BatchGetArgs) (*kvpb.BatchGetReply, error) {
	reply := &kvpb.BatchGetReply{
		Values: make(map[string]string),
	}

	for _, key := range args.Keys {
		if !kv.checkGroup(key) {
			reply.Err = kvpb.Error_ERR_WRONG_GROUP
			return reply, nil
		}
	}

	readIndex, isLeader := kv.rf.ReadIndex()
	if !isLeader {
		reply.Err = kvpb.Error_ERR_WRONG_LEADER
		return reply, nil
	}
	for {
		kv.mu.Lock()
		applied := kv.lastAppliedIndex
		isKilled := kv.killed()
		kv.mu.Unlock()

		if isKilled {
			reply.Err = kvpb.Error_ERR_WRONG_LEADER
			return reply, nil
		}
		if applied >= readIndex {
			break
		}
		if ctx.Err() != nil {
			reply.Err = kvpb.Error_ERR_TIMEOUT
			return reply, nil
		}
		time.Sleep(2 * time.Millisecond) // 释放 CPU 调度
	}

	for i, key := range args.Keys {
		var err error
		var val string

		var ts uint64 = 0
		if i < len(args.Tss) {
			ts = args.Tss[i]
		}

		if ts == 0 {
			val, err = kv.db.Get(key)
		} else {
			val, err = kv.db.GetAt(key, ts)
		}

		if err == nil {
			reply.Values[key] = val
		} else {
			reply.Values[key] = ""
		}
	}

	reply.Err = kvpb.Error_OK
	return reply, nil
}
func (kv *KVServer) PutAppend(ctx context.Context, args *kvpb.PutAppendArgs) (*kvpb.PutAppendReply, error) {
	reply := &kvpb.PutAppendReply{}
	if !kv.checkGroup(args.Key) {
		reply.Err = kvpb.Error_ERR_WRONG_GROUP
		return reply, nil
	}
	op := &kvpb.Op{
		Operation:     args.Op,
		Key:           args.Key,
		Value:         args.Value,
		ClientId:      args.ClientId,
		SeqId:         args.SeqId,
		Ttl:           args.Ttl,
		ExpectedValue: args.ExpectedValue,
	}
	res := kv.waitRaft(op)
	reply.Err = res.Err
	return reply, nil
}
func (kv *KVServer) SendAddNode(ctx context.Context, args *kvpb.AddNodeArgs) (*kvpb.AddNodeReply, error) {
	op := &kvpb.Op{
		Operation: "AddNode",
		Key:       strconv.FormatInt(args.NodeId, 10), // Key 存节点 ID
		Value:     args.Addr,                          // Value 存节点 IP
		ClientId:  time.Now().UnixNano(),              // 随机生成防去重
		SeqId:     1,
	}
	res := kv.waitRaft(op)
	return &kvpb.AddNodeReply{Err: res.Err}, nil
}
func (kv *KVServer) SendRemoveNode(ctx context.Context, args *kvpb.RemoveNodeArgs) (*kvpb.RemoveNodeReply, error) {
	op := &kvpb.Op{
		Operation: "RemoveNode",
		Key:       strconv.FormatInt(args.NodeId, 10),
		ClientId:  time.Now().UnixNano(),
		SeqId:     1,
	}
	res := kv.waitRaft(op)
	return &kvpb.RemoveNodeReply{Err: res.Err}, nil
}
func (kv *KVServer) Kill() {
	if atomic.CompareAndSwapInt32(&kv.dead, 0, 1) {
		kv.rf.Shutdown()
		kv.db.Close()
	}
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *KVServer) applier() {
	var lastSnapshottedIndex int64 = 0
	kv.lastSnapshotTime = time.Now()
	batchOps := make([]raftapi.ApplyMsg, 0, 1000)
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
		for len(batchOps) < 1000 {
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
				if msg.CommandIndex <= kv.lastAppliedIndex {
					continue
				}

				op := &kvpb.Op{}
				cmdBytes, ok := msg.Command.([]byte)
				if ok {
					if err := proto.Unmarshal(cmdBytes, op); err != nil {
						kv.lastAppliedIndex = msg.CommandIndex
						continue
					}
				} else if cmdOp, ok := msg.Command.(*kvpb.Op); ok {
					op = cmdOp
				} else {
					kv.lastAppliedIndex = msg.CommandIndex
					continue
				}

				if op == nil || op.Operation == "NoOp" || op.Operation == "" {
					kv.lastAppliedIndex = msg.CommandIndex
					continue
				}

				var result OpResult
				result.Err = kvpb.Error_OK
				currentTerm, _ := kv.rf.GetState()
				result.Term = currentTerm

				isDup := false
				if op.Operation == "Put" || op.Operation == "Append" || op.Operation == "Delete" || op.Operation == "CAS" {
					isDup = kv.db.IsDuplicate(op.ClientId, op.SeqId)
				}

				if isDup {
					result.Err = kvpb.Error_OK
				} else {
					if op.Operation == "AddNode" {
						nodeId, _ := strconv.ParseInt(op.Key, 10, 64)
						if r, ok := kv.rf.(*raft.Raft); ok {
							r.AddNode(nodeId, op.Value)
						}
					} else if op.Operation == "RemoveNode" {
						nodeId, _ := strconv.ParseInt(op.Key, 10, 64)
						if r, ok := kv.rf.(*raft.Raft); ok {
							r.RemoveNode(nodeId)
						}
						if nodeId == kv.me {
							go kv.Kill()
						}
					} else if op.Operation == "UpdateConfig" {
						kv.UpdateConfig(op)
					} else if op.Operation == "InsertShard" {
						decoded, err := base64.StdEncoding.DecodeString(op.Value)
						if err == nil {
							payload := &kvpb.MigrationPayload{}
							if err := proto.Unmarshal(decoded, payload); err == nil {
								if kv.config.Num == payload.ConfigNum && kv.shardStates[payload.Shard] == Pulling {
									kv.mu.Unlock()
									err := kv.db.InsertShardData(payload.Data, payload.SeqCache)
									kv.mu.Lock()

									if err != nil {
										tool.Log.Error("迁移数据落盘失败", "err", err)
									} else {
										kv.shardStates[payload.Shard] = Serving
										kv.persistState()
										tool.Log.Info("分片数据落盘完成，状态转为 Serving", "Shard", payload.Shard, "GroupID", kv.gid)
									}
								}
							} else {
								tool.Log.Error("解析 MigrationPayload 失败", "err", err)
							}
						}
					} else if op.Operation == "GCShard" {
						var shard int
						var configNum int64
						_, err := fmt.Sscanf(op.Value, "%d,%d", &shard, &configNum)
						if err == nil {
							if kv.config.Num == configNum && (kv.shardStates[shard] == Pushing || kv.shardStates[shard] == GCing) {
								kv.mu.Unlock()
								err := kv.db.DeleteShardData(shard, key2shard)
								kv.mu.Lock()

								if err != nil {
									tool.Log.Error("清理分片数据失败", "err", err)
								} else {
									kv.shardStates[shard] = Serving
									kv.persistState()
									tool.Log.Info("🗑️ [GC成功] 已物理删除历史分片数据", "GroupID", kv.gid, "Shard", shard, "ConfigNum", configNum)
								}
							}
						}
					} else if op.Operation == "CancelPush" {
						var shard int
						var configNum int64
						_, err := fmt.Sscanf(op.Value, "%d,%d", &shard, &configNum)
						if err == nil {
							if kv.config.Num == configNum && kv.shardStates[shard] == Pushing {
								kv.shardStates[shard] = Serving
								kv.persistState()
								tool.Log.Info("🛡️ 触发自救：取消 Pushing 状态，完美保留本地数据", "Shard", shard)
							}
						}
					} else if op.Operation != "Get" {
						kv.mu.Unlock()
						ts, err := kv.db.ApplyCommand(op.Operation, []byte(op.Key), []byte(op.Value), op.Ttl, op.ClientId, op.SeqId, []byte(op.ExpectedValue))
						
						if err == nil && op.Operation == "Append" {
							finalVal, getErr := kv.db.Get(op.Key)
							if getErr == nil {
								op.Value = finalVal 
							}
						}
						kv.mu.Lock()

						if err != nil {
							if op.Operation == "CAS" {
								result.Err = kvpb.Error_ERR_CAS_FAILED
							} else {
								tool.Log.Fatal("状态机应用失败! ", "Index", msg.CommandIndex, "Err", err)
							}
						} else {
							go kv.WatchSub(op, ts)
						}
					}
				}

				kv.lastAppliedIndex = msg.CommandIndex

				if ch, ok := kv.notifyChs[msg.CommandIndex]; ok {
					select {
					case ch <- result:
					default:
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
				if kv.isSnapshotting.CompareAndSwap(0, 1) {
					snapshotTs := kv.db.CurrentTs()
					lastSnapshottedIndex = lastIndex
					kv.lastSnapshotTime = time.Now()

					go func(index int64, ts uint64) {
						defer kv.isSnapshotting.Store(0)

						snapshotData, err := kv.db.GetSnapshot(ts)
						if err != nil {
							return
						}
						if len(snapshotData) > 0 {
							kv.rf.Snapshot(index, snapshotData)
						}
					}(lastIndex, snapshotTs)
				}
			}
		}

		kv.mu.Unlock()

		for i := range batchOps {
			batchOps[i] = raftapi.ApplyMsg{}
		}
		batchOps = batchOps[:0]
	}
}

func (kv *KVServer) pollConfigLoop() {
	var inFlightNum int64 = -1
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			canFetchNext := true
			for _, state := range kv.shardStates {
				if state != Serving {
					canFetchNext = false
					break
				}
			}
			currentNum := kv.config.Num

			var states [10]int
			copy(states[:], kv.shardStates[:])
			cfg := kv.config
			gid := kv.gid
			kv.mu.Unlock()
			if !canFetchNext {
				latestCfg := kv.smClerk.Query(-1)
				if latestCfg.Num >= currentNum {
					for shard := 0; shard < 10; shard++ {
						if states[shard] == Pushing {
							targetGID := cfg.Shards[int64(shard)]

							if _, exists := latestCfg.Groups[targetGID]; !exists && targetGID != 0 {
								b64Str := fmt.Sprintf("%d,%d", shard, currentNum)
								op := &kvpb.Op{
									Operation: "CancelPush", 
									Value:     b64Str,
									ClientId:  gid,
									SeqId:     currentNum,
								}
								kv.rf.Propose(op)
								tool.Log.Warn("触发边界自救：目标集群已被移除，强行解除 Pushing 并保留本地数据", "Shard", shard)
							}
						}

					}
				}
			}

			kv.mu.Lock()
			canFetchNextAgain := true
			for _, state := range kv.shardStates {
				if state != Serving {
					canFetchNextAgain = false
					break
				}
			}
			kv.mu.Unlock()

			if canFetchNextAgain && inFlightNum < currentNum+1 {
				nextNum := currentNum + 1
				nextConfig := kv.smClerk.Query(nextNum)
				if nextConfig.Num == nextNum {
					configBytes, err := proto.Marshal(&nextConfig)
					if err == nil {
						b64Str := base64.StdEncoding.EncodeToString(configBytes)
						op := &kvpb.Op{
							Operation: "UpdateConfig",
							Value:     b64Str,
							ClientId:  gid,
							SeqId:     nextConfig.Num,
						}
						kv.rf.Propose(op)
						inFlightNum = nextNum
						tool.Log.Info("Leader 发现新版本路由表，已发起共识提案", "GroupID", gid, "ConfigNum", nextNum)
					}
				}
			}
		} else {
			inFlightNum = -1
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *KVServer) Snapshot() []byte {
	ts := kv.db.CurrentTs()
	data, err := kv.db.GetSnapshot(ts)
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
func (kv *KVServer) WatchSub(op *kvpb.Op, ts uint64) {
	kv.watchManager.Dispatch(WatchEvent{
		Op:    op.Operation,
		Key:   op.Key,
		Value: op.Value,
		Ts:    ts,
	})
}
func key2shard(key string) int {
	var hash uint32 = 0
	for i := 0; i < len(key); i++ {
		hash = hash*31 + uint32(key[i])
	}
	return int(hash % NShards)
}
func (kv *KVServer) checkGroup(key string) bool {
	shard := int64(key2shard(key))
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num == 0 {
		return false
	}
	if kv.config.Shards[shard] != kv.gid {
		return false
	}
	if kv.shardStates[shard] != Serving {
		return false
	}
	return true
}
func (kv *KVServer) FetchShardData(ctx context.Context, args *kvpb.FetchShardArgs) (*kvpb.FetchShardReply, error) {
	reply := &kvpb.FetchShardReply{
		Err:      kvpb.Error_OK,
		Data:     make(map[string]string),
		SeqCache: make(map[int64]int64),
	}

	readIndex, isLeader := kv.rf.ReadIndex()
	if !isLeader {
		reply.Err = kvpb.Error_ERR_WRONG_LEADER
		return reply, nil
	}

	// 阻塞等待状态机追齐进度
	for {
		kv.mu.Lock()
		applied := kv.lastAppliedIndex
		isKilled := kv.killed()
		kv.mu.Unlock()

		if isKilled {
			reply.Err = kvpb.Error_ERR_WRONG_LEADER
			return reply, nil
		}
		if applied >= readIndex {
			break
		}
		if ctx.Err() != nil {
			reply.Err = kvpb.Error_ERR_TIMEOUT
			return reply, nil
		}
		time.Sleep(2 * time.Millisecond)
	}

	// 数据库已 100% 恢复完毕，安全打包数据
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	if args.ConfigNum > kv.config.Num {
		reply.Err = kvpb.Error_ERR_WRONG_GROUP
		return reply, nil
	}
	
	reply.ConfigNum = args.ConfigNum
	reply.Data = kv.db.GetShardData(int(args.Shard), key2shard)
	reply.SeqCache = kv.db.GetSeqCache()
	tool.Log.Info("成功交接分片数据", "我的组", kv.gid, "交出分片", args.Shard, "数据条数", len(reply.Data))
	
	return reply, nil
}

func (kv *KVServer) GCShardData(ctx context.Context, args *kvpb.GCShardArgs) (*kvpb.GCShardReply, error) {
	reply := &kvpb.GCShardReply{Err: kvpb.Error_OK}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = kvpb.Error_ERR_WRONG_LEADER
		return reply, nil
	}

	kv.mu.Lock()
	if kv.config.Num < args.ConfigNum {
		reply.Err = kvpb.Error_ERR_WRONG_GROUP
		kv.mu.Unlock()
		return reply, nil
	}

	if kv.shardStates[args.Shard] == Pushing || kv.shardStates[args.Shard] == GCing {
		kv.mu.Unlock()

		b64Str := fmt.Sprintf("%d,%d", args.Shard, args.ConfigNum)
		op := &kvpb.Op{
			Operation: "GCShard",
			Value:     b64Str,
			ClientId:  int64(kv.gid),
			SeqId:     args.ConfigNum,
		}

		_, _, isLeader := kv.rf.Propose(op)
		if !isLeader {
			reply.Err = kvpb.Error_ERR_WRONG_LEADER
			return reply, nil
		}

		for i := 0; i < 20; i++ {
			kv.mu.Lock()
			if kv.shardStates[args.Shard] == Serving {
				kv.mu.Unlock()
				return reply, nil
			}
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
		reply.Err = kvpb.Error_ERR_TIMEOUT
		return reply, nil
	}
	kv.mu.Unlock()
	return reply, nil
}
func (kv *KVServer) UpdateConfig(op *kvpb.Op) {
	configBytes, err := base64.StdEncoding.DecodeString(op.Value)
	if err != nil {
		tool.Log.Error("路由表 Base64 解码失败 (请检查是否是不匹配的脏数据)", "err", err)
		return
	}

	var newCfg shardpb.Config
	if err := proto.Unmarshal(configBytes, &newCfg); err == nil {
		if newCfg.Num == kv.config.Num+1 {
			for i := int64(0); i < 10; i++ {
				oldGID := kv.config.Shards[i]
				newGID := newCfg.Shards[i]
				if oldGID != kv.gid && newGID == kv.gid {
					if oldGID == 0 {
						kv.shardStates[i] = Serving
					} else {
						kv.shardStates[i] = Pulling
					}
				}
				if oldGID == kv.gid && newGID != kv.gid {
					if newGID != 0 {
						kv.shardStates[i] = Pushing
					}
				}
			}
			kv.lastConfig = kv.config
			kv.config = newCfg
			kv.persistState()
			tool.Log.Info("状态机应用新路由表", "GroupID", kv.gid, "ConfigNum", kv.config.Num, "States", kv.shardStates)
		}
	} else {
		tool.Log.Error("路由表 Proto 反序列化失败", "err", err)
	}
}

type MigrationPayload struct {
	ConfigNum int64
	Shard     int64
	Data      map[string]string
	SeqCache  map[string]int64
}

func (kv *KVServer) migrationLoop() {
	inFlightPulls := make(map[int]int64)

	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			for shard := 0; shard < 10; shard++ {
				if kv.shardStates[shard] == Pulling {
					if inFlightPulls[shard] == kv.config.Num {
						continue
					}

					inFlightPulls[shard] = kv.config.Num
					oldGID := kv.lastConfig.Shards[int64(shard)]
					if oldServers, ok := kv.lastConfig.Groups[oldGID]; ok {
						go func(shardId int, configNum int64, servers []string) {
							success := kv.pullDataFromOldGroup(shardId, configNum, servers)

							if !success {
								kv.mu.Lock()
								if inFlightPulls[shardId] == configNum {
									delete(inFlightPulls, shardId)
								}
								kv.mu.Unlock()
							}
						}(shard, kv.config.Num, oldServers.Servers)
					}
				}
			}
			kv.mu.Unlock()
		} else {
			inFlightPulls = make(map[int]int64)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *KVServer) pullDataFromOldGroup(shard int, configNum int64, oldServers []string) bool {
	args := &kvpb.FetchShardArgs{
		Shard:     int64(shard),
		ConfigNum: configNum,
	}

	for _, server := range oldServers {
		var maxMsgSize = 1024 * 1024 * 1024
		conn, err := grpc.NewClient(server,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxMsgSize),
				grpc.MaxCallSendMsgSize(maxMsgSize),
			),
		)
		if err != nil {
			continue
		}
		client := kvpb.NewRaftKVClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		reply, err := client.FetchShardData(ctx, args)
		cancel()
		conn.Close()

		if err == nil && reply.Err == kvpb.Error_OK {
			payload := &kvpb.MigrationPayload{
				ConfigNum: configNum,
				Shard:     int64(shard),
				Data:      reply.Data,
				SeqCache:  reply.SeqCache,
			}
			data, err := proto.Marshal(payload) 
			if err != nil {
				tool.Log.Error("序列化 MigrationPayload 失败", "err", err)
				return false
			}

			op := &kvpb.Op{
				Operation: "InsertShard",
				Value:     base64.StdEncoding.EncodeToString(data),
			}

			_, _, isLeader := kv.rf.Propose(op)
			if !isLeader {
				return false
			}

			go func(targetShard int, targetConfig int64, targetAddrs []string) {
				tool.Log.Debug("等待 InsertShard 被状态机应用...", "Shard", targetShard)

				for {
					kv.mu.Lock()
					state := kv.shardStates[targetShard]
					kv.mu.Unlock()
					if state == Serving {
						break
					}
					time.Sleep(50 * time.Millisecond)
				}

				tool.Log.Debug("数据已安全落盘，准备通知旧组进行清理...", "Shard", targetShard)
				gcArgs := &kvpb.GCShardArgs{
					Shard:     int64(targetShard),
					ConfigNum: targetConfig,
				}

				for {
					for _, addr := range targetAddrs {
						conn, err := grpc.NewClient(addr,
							grpc.WithTransportCredentials(insecure.NewCredentials()),
							grpc.WithDefaultCallOptions(
								grpc.MaxCallRecvMsgSize(1024*1024*1024),
								grpc.MaxCallSendMsgSize(1024*1024*1024),
							),
						)
						if err != nil {
							continue
						}

						client := kvpb.NewRaftKVClient(conn)
						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						reply, err := client.GCShardData(ctx, gcArgs)
						cancel()
						conn.Close()

						if err == nil && reply.Err == kvpb.Error_OK {
							tool.Log.Info("已成功通知旧组清理过期数据", "OldGroupAddr", addr, "Shard", targetShard)
							return
						}
						time.Sleep(100 * time.Millisecond)
					}
					time.Sleep(1 * time.Second)
				}
			}(shard, configNum, oldServers)

			return true
		}
	}
	return false
}