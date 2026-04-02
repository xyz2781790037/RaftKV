package shardmaster

import (
	"RaftKV/proto/raftpb"
	"RaftKV/proto/shardpb"
	"RaftKV/service/raft"
	"RaftKV/service/raftapi"
	"RaftKV/service/storage"
	"RaftKV/tool"
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
)

type OpResult struct {
	Err  string
	Term int64
}
type ShardMaster struct {
	shardpb.UnimplementedShardMasterServer
	mu      sync.Mutex
	me      int64
	rf      raftapi.Raft
	applyCh chan raftapi.ApplyMsg
	dead    int32

	configs   []*shardpb.Config
	seqCache  map[int64]int64
	notifyChs map[int64]chan OpResult
}
func (sm *ShardMaster) persistState() {
	state := &shardpb.SMPersistentState{
		Configs:  sm.configs,
		SeqCache: sm.seqCache,
	}
	data, err := proto.Marshal(state)
	if err != nil {
		tool.Log.Error("Master 持久化序列化失败", "err", err)
		return
	}

	dir := fmt.Sprintf("data/master/node_%d", sm.me)
	if err := os.MkdirAll(dir, 0755); err != nil {
		tool.Log.Error("创建 Master 数据目录失败", "dir", dir, "err", err)
	}
	path := fmt.Sprintf("%s/state.json", dir)
	if err := os.WriteFile(path, data, 0644); err != nil {
		tool.Log.Error("Master 写入 state.json 失败", "path", path, "err", err)
	} else {
		tool.Log.Info("Master 路由表已使用 Proto 成功落盘", "ConfigNum", sm.configs[len(sm.configs)-1].Num, "Path", path)
	}
}
func (sm *ShardMaster) waitRaft(op *shardpb.Op) OpResult {
	sm.mu.Lock()
	index, _, isLeader := sm.rf.Propose(op)
	if !isLeader {
		sm.mu.Unlock()
		return OpResult{
			Err: "ErrWrongLeader",
		}
	}
	ch := make(chan OpResult, 1)
	sm.notifyChs[index] = ch
	sm.mu.Unlock()

	select {
	case res := <-ch:
		return res
	case <-time.After(3 * time.Second):
		sm.mu.Lock()
		delete(sm.notifyChs, index)
		sm.mu.Unlock()
		return OpResult{
			Err: "ErrTimeout",
		}
	}
}
func (sm *ShardMaster) Join(ctx context.Context, args *shardpb.JoinArgs) (*shardpb.JoinReply, error) {
	op := &shardpb.Op{
		Operation: "Join",
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		JoinArgs:  args,
	}
	res := sm.waitRaft(op)
	return &shardpb.JoinReply{
		WrongLeader: res.Err == "ErrWrongLeader",
		Err:         res.Err,
	}, nil
}
func (sm *ShardMaster) Leave(ctx context.Context, args *shardpb.LeaveArgs) (*shardpb.LeaveReply, error) {
	op := &shardpb.Op{
		Operation: "Leave",
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		LeaveArgs: args,
	}
	res := sm.waitRaft(op)
	return &shardpb.LeaveReply{
		WrongLeader: res.Err == "ErrWrongLeader",
		Err:         res.Err,
	}, nil
}
func (sm *ShardMaster) Move(ctx context.Context, args *shardpb.MoveArgs) (*shardpb.MoveReply, error) {
	op := &shardpb.Op{
		Operation: "Move",
		ClientId:  args.ClientId,
		SeqId:     args.SeqId,
		MoveArgs:  args,
	}
	res := sm.waitRaft(op)
	return &shardpb.MoveReply{
		WrongLeader: res.Err == "ErrWrongLeader",
		Err:         res.Err,
	}, nil
}
func (sm *ShardMaster) Query(ctx context.Context, args *shardpb.QueryArgs) (*shardpb.QueryReply, error) {
	reply := &shardpb.QueryReply{}

	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = "ErrWrongLeader"
		return reply, nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if args.Num == -1 || args.Num >= int64(len(sm.configs)) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	
	reply.Err = ""
	return reply, nil
}
func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Shutdown()
}
func (sm *ShardMaster) createNextConfig() *shardpb.Config {
	lastConfig := sm.configs[len(sm.configs)-1]
	nextConfig := &shardpb.Config{
		Num:    lastConfig.Num + 1,
		Shards: make(map[int64]int64),
		Groups: make(map[int64]*shardpb.GroupServers),
	}
	for k, v := range lastConfig.Shards {
		nextConfig.Shards[k] = v
	}
	for k, v := range lastConfig.Groups {
		srvs := make([]string, len(v.Servers))
		copy(srvs, v.Servers)
		nextConfig.Groups[k] = &shardpb.GroupServers{
			Servers: srvs,
		}
	}
	return nextConfig
}
func (sm *ShardMaster) rebalance(config *shardpb.Config) {
	if len(config.Groups) == 0 {
		for i := int64(0); i < 10; i++ {
			config.Shards[i] = 0
		}
		return
	}
	var gids []int64
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Slice(gids, func(i, j int) bool {
		return gids[i] < gids[j]
	})
	shardCount := make(map[int64][]int64)
	for _, gid := range gids {
		shardCount[gid] = make([]int64, 0)
	}
	var unassigned []int64
	for i := int64(0); i < 10; i++ {
		gid := config.Shards[i]
		if _, exists := config.Groups[gid]; exists {
			shardCount[gid] = append(shardCount[gid], i)
		} else {
			unassigned = append(unassigned, i)
		}
	}
	for {
		minGID, maxGID := getMinMaxGID(shardCount, gids)
		if len(unassigned) > 0 {
			targetShard := unassigned[0]
			unassigned = unassigned[1:]
			shardCount[minGID] = append(shardCount[minGID], targetShard)
			config.Shards[targetShard] = minGID
			continue
		}
		if len(shardCount[maxGID])-len(shardCount[minGID]) <= 1 {
			break
		}

		targetShard := shardCount[maxGID][0]
		shardCount[maxGID] = shardCount[maxGID][1:]
		shardCount[minGID] = append(shardCount[minGID], targetShard)
		config.Shards[targetShard] = minGID
	}
}
func getMinMaxGID(shardCount map[int64][]int64, sortedGIDs []int64) (minGID int64, maxGID int64) {
	minGID = sortedGIDs[0]
	maxGID = sortedGIDs[0]
	for _, gid := range sortedGIDs {
		if len(shardCount[gid]) < len(shardCount[minGID]) {
			minGID = gid
		}
		if len(shardCount[gid]) > len(shardCount[maxGID]) {
			maxGID = gid
		}
	}
	return minGID, maxGID
}
func (sm *ShardMaster) applyJoin(args *shardpb.JoinArgs) {
	nextConfig := sm.createNextConfig()
	added := false
	for gid, servers := range args.Servers {
		if len(servers.Servers) == 0 {
			tool.Log.Warn("拦截无效 Join 请求：空机器组，拒绝将其加入路由表", "GID", gid)
			continue
		}
		nextConfig.Groups[gid] = servers
		added = true
	}
	if !added {
		return
	}

	sm.rebalance(nextConfig)
	sm.configs = append(sm.configs, nextConfig)
	sm.persistState()
	tool.Log.Info("ShardMaster 集群新增组", "ConfigNum", nextConfig.Num, "Groups", nextConfig.Groups)
}
func (sm *ShardMaster) applyLeave(args *shardpb.LeaveArgs) {
	nextConfig := sm.createNextConfig()
	for _, gid := range args.GIDs {
		delete(nextConfig.Groups, gid)
	}
	sm.rebalance(nextConfig)
	sm.configs = append(sm.configs, nextConfig)
	sm.persistState()
	tool.Log.Info("ShardMaster 集群移除组", "ConfigNum", nextConfig.Num)
}
func (sm *ShardMaster) applyMove(args *shardpb.MoveArgs) {
	nextConfig := sm.createNextConfig()
	nextConfig.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, nextConfig)
	sm.persistState()
}

func Make(servers *raft.PeerManager, me int64, state *storage.Store, applyCh chan raftapi.ApplyMsg) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.configs = make([]*shardpb.Config, 1)
	sm.configs[0] = &shardpb.Config{
		Num:    0,
		Shards: make(map[int64]int64),
		Groups: make(map[int64]*shardpb.GroupServers),
	}
	sm.seqCache = make(map[int64]int64)
	sm.notifyChs = make(map[int64]chan OpResult)
	for i := int64(0); i < 10; i++ {
		sm.configs[0].Shards[i] = 0
	}
	
	path := fmt.Sprintf("data/master/node_%d/state.json", sm.me)
	if data, err := os.ReadFile(path); err == nil {
		pState := &shardpb.SMPersistentState{}
		if err := proto.Unmarshal(data, pState); err == nil {
			sm.configs = pState.Configs
			sm.seqCache = pState.SeqCache
			tool.Log.Info("ShardMaster 从磁盘完美恢复路由表", "ConfigNum", sm.configs[len(sm.configs)-1].Num, "Path", path)
		} else {
			tool.Log.Error("反序列化 Master state 失败", "err", err)
		}
	} else {
		tool.Log.Warn("未找到 Master 历史状态，将作为全新节点或依赖 Raft 回放启动", "path", path, "err", err)
	}
	
	sm.applyCh = applyCh
	sm.rf = raft.Make(servers, me, state, sm.applyCh)
	go sm.applier()

	return sm
}
func (sm *ShardMaster) GetRaft() raftpb.RaftServer {
	return sm.rf.(*raft.Raft)
}

func (sm *ShardMaster) applier() {
	for !sm.killed() {
		msg := <-sm.applyCh
		if msg.CommandValid {
			if msg.Command == nil {
				continue
			}
			op := &shardpb.Op{}
			cmdBytes, ok := msg.Command.([]byte)
			if ok {
				if len(cmdBytes) == 0 {
					continue
				}
				if err := proto.Unmarshal(cmdBytes, op); err != nil {
					tool.Log.Error("ShardMaster Unmarshal err", "err", err)
					continue
				}
			} else if cmdOp, ok := msg.Command.(*shardpb.Op); ok {
				op = cmdOp
			} else if cmdOp, ok := msg.Command.(shardpb.Op); ok {
				op = &cmdOp
			} else {
				tool.Log.Error("Raft 回放了无法识别的 Command 类型！", "type", fmt.Sprintf("%T", msg.Command))
				continue
			}
			
			sm.mu.Lock()
			var res OpResult
			isDup := false
			if op.Operation != "Query" {
				if lastSeq, ok := sm.seqCache[op.ClientId]; ok && lastSeq >= op.SeqId {
					isDup = true
				}
			}
			if !isDup {
				if op.Operation == "Join" {
					sm.seqCache[op.ClientId] = op.SeqId
					sm.applyJoin(op.JoinArgs)
				} else if op.Operation == "Leave" {
					sm.seqCache[op.ClientId] = op.SeqId
					sm.applyLeave(op.LeaveArgs)
				} else if op.Operation == "Move" {
					sm.seqCache[op.ClientId] = op.SeqId
					sm.applyMove(op.MoveArgs)
				}
			}
			if ch, ok := sm.notifyChs[int64(msg.CommandIndex)]; ok {
				ch <- res
				delete(sm.notifyChs, int64(msg.CommandIndex))
			}
			sm.mu.Unlock()
		}
	}
}