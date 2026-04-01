package kvraft

import (
	kvpb "RaftKV/proto/kvpb"
	"RaftKV/proto/shardpb"
	"RaftKV/service/shardmaster"
	"RaftKV/tool"
	"context"
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	// "google.golang.org/grpc/keepalive"
)

type batchGetReq struct {
	key    string
	ts     uint64
	respCh chan string
}
type Clerk struct {
	sm     *shardmaster.ShardClerk
	config shardpb.Config
	conns  map[string]kvpb.RaftKVClient
	mu     sync.Mutex

	clientId int64
	seqId    int64
	batchCh  chan *batchGetReq
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

func MakeClerk(servers []string) *Clerk {
	ck := &Clerk{
		sm:       shardmaster.MakeClerk(servers),
		conns:    make(map[string]kvpb.RaftKVClient),
		clientId: nrand(),
		seqId:    1,
		batchCh:  make(chan *batchGetReq, 10000),
	}
	ck.config = ck.sm.Query(-1)
	go ck.batcher()
	return ck
}
func (ck *Clerk) refreshConfig() {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.config = ck.sm.Query(-1)
}
func (ck *Clerk) getClient(addr string) kvpb.RaftKVClient {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if client, ok := ck.conns[addr]; ok {
		return client
	}
	// kacp := keepalive.ClientParameters{
	// 	Time:                2 * time.Second, // 每 2 秒发送一次 Ping
	// 	Timeout:             1 * time.Second, // 1 秒内收不到 Pong 就认为连接已断开
	// 	PermitWithoutStream: true,
	// }
	var maxMsgSize = 1024 * 1024 * 1024
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// grpc.WithKeepaliveParams(kacp),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize),
		),
	)
	if err != nil {
		tool.Log.Warn("Newclient error", "err", err)
		return nil
	}
	client := kvpb.NewRaftKVClient(conn)
	ck.conns[addr] = client
	return client
}
func (ck *Clerk) Get(key string) string {
	respCh := make(chan string, 1)
	ck.batchCh <- &batchGetReq{
		key:    key,
		ts:     0,
		respCh: respCh,
	}
	val := <-respCh

	return val
}
func (ck *Clerk) GetAt(key string, ts uint64) string {
	respCh := make(chan string, 1)

	ck.batchCh <- &batchGetReq{
		key:    key,
		ts:     ts,
		respCh: respCh,
	}
	val := <-respCh
	return val
}

func (ck *Clerk) PutAppend(key string, value string, op string, ttl int64) {
	newSeqId := atomic.AddInt64(&ck.seqId, 1)
	args := &kvpb.PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    newSeqId,
		Ttl:      ttl,
	}

	for {
		shard := key2shard(key)
		ck.mu.Lock()
		gid := ck.config.Shards[int64(shard)]
		var servers []string
		if group, ok := ck.config.Groups[gid]; ok {
			servers = group.Servers
		}
		ck.mu.Unlock()
		if servers != nil && len(servers) > 0 {
			for i := 0; i < len(servers); i++ {
				client := ck.getClient(servers[i])
				if client == nil {
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				reply, err := client.PutAppend(ctx, args)
				cancel()
				if err != nil || reply.Err == kvpb.Error_ERR_WRONG_LEADER || reply.Err == kvpb.Error_ERR_TIMEOUT {
					continue
				}
				if reply.Err == kvpb.Error_OK {
					return
				}
				if reply.Err == kvpb.Error_ERR_WRONG_GROUP {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.refreshConfig()
	}
}

// 🚨 修改点：增加 gid 参数。必须明确告诉客户端，你要给哪个机器组(GID)添加节点
func (ck *Clerk) AddNode(gid int64, nodeId int64, addr string) {
	args := &kvpb.AddNodeArgs{
		NodeId: nodeId,
		Addr:   addr,
	}

	for {
		ck.mu.Lock()
		cfg := ck.config
		ck.mu.Unlock()

		// 1. 找到该 GID 对应的物理机列表
		group, ok := cfg.Groups[gid]
		if !ok || len(group.Servers) == 0 {
			// 如果这个组不存在，尝试刷新路由表
			time.Sleep(100 * time.Millisecond)
			ck.refreshConfig()
			continue
		}

		// 2. 在这个组内轮询找 Leader 发送动态扩容请求
		success := false
		for i := 0; i < len(group.Servers); i++ {
			client := ck.getClient(group.Servers[i])
			if client == nil {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			reply, err := client.SendAddNode(ctx, args)
			cancel()

			if err != nil || reply.Err == kvpb.Error_ERR_WRONG_LEADER || reply.Err == kvpb.Error_ERR_TIMEOUT {
				continue // 找错 Leader 或超时，试下一台
			}

			if reply.Err == kvpb.Error_OK {
				tool.Log.Info("节点扩容请求发送成功！", "GroupID", gid, "新NodeID", nodeId)
				success = true
				break
			}
		}

		if success {
			return
		}
		time.Sleep(100 * time.Millisecond)
		ck.refreshConfig()
	}
}

func (ck *Clerk) RemoveNode(gid int64, nodeId int64) {
	args := &kvpb.RemoveNodeArgs{
		NodeId: nodeId,
	}

	for {
		ck.mu.Lock()
		cfg := ck.config
		ck.mu.Unlock()

		group, ok := cfg.Groups[gid]
		if !ok || len(group.Servers) == 0 {
			time.Sleep(100 * time.Millisecond)
			ck.refreshConfig()
			continue
		}

		success := false
		for i := 0; i < len(group.Servers); i++ {
			client := ck.getClient(group.Servers[i])
			if client == nil {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			reply, err := client.SendRemoveNode(ctx, args)
			cancel()

			if err != nil || reply.Err == kvpb.Error_ERR_WRONG_LEADER || reply.Err == kvpb.Error_ERR_TIMEOUT {
				continue
			}

			if reply.Err == kvpb.Error_OK {
				tool.Log.Info("移除节点请求发送成功！", "GroupID", gid, "剔除的NodeID", nodeId)
				success = true
				break
			}
		}

		if success {
			return
		}
		time.Sleep(100 * time.Millisecond)
		ck.refreshConfig()
	}
}
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put", 0)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append", 0)
}

func (ck *Clerk) Delete(key string) {
	ck.PutAppend(key, "", "Delete", 0)
}
func (ck *Clerk) PutWithTTL(key string, value string, ttlSeconds int64) {
	ck.PutAppend(key, value, "Put", ttlSeconds)
}

func (ck *Clerk) CAS(key string, expectedOldValue string, newValue string) bool {
	newSeqId := atomic.AddInt64(&ck.seqId, 1)
	args := &kvpb.PutAppendArgs{
		Key:           key,
		Value:         newValue,
		Op:            "CAS",
		ClientId:      ck.clientId,
		SeqId:         newSeqId,
		Ttl:           0,
		ExpectedValue: expectedOldValue,
	}

	for {
		shard := key2shard(key)
		ck.mu.Lock()
		gid := ck.config.Shards[int64(shard)]
		var servers []string
		if group, ok := ck.config.Groups[gid]; ok {
			servers = group.Servers
		}
		ck.mu.Unlock()
		if servers != nil && len(servers) > 0 {
			for i := 0; i < len(servers); i++ {
				client := ck.getClient(servers[i])
				if client == nil {
					continue
				}

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				reply, err := client.PutAppend(ctx, args)
				cancel()

				if err != nil || reply.Err == kvpb.Error_ERR_WRONG_LEADER || reply.Err == kvpb.Error_ERR_TIMEOUT {
					continue
				}
				if reply.Err == kvpb.Error_OK {
					return true
				}
				if reply.Err == kvpb.Error_ERR_CAS_FAILED {
					return false
				}
				if reply.Err == kvpb.Error_ERR_WRONG_GROUP {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.refreshConfig()
	}
}
func (ck *Clerk) batcher() {
	const MaxBatchSize = 100
	const BatchTimeout = 2 * time.Millisecond

	buffer := make([]*batchGetReq, 0, MaxBatchSize)
	ticker := time.NewTicker(BatchTimeout)
	defer ticker.Stop()
	for {
		select {
		case req := <-ck.batchCh:
			buffer = append(buffer, req)
			if len(buffer) >= MaxBatchSize {
				ck.dispatchBatch(buffer)
				buffer = make([]*batchGetReq, 0, MaxBatchSize)
			}
		case <-ticker.C:
			if len(buffer) > 0 {
				ck.dispatchBatch(buffer)
				buffer = make([]*batchGetReq, 0, MaxBatchSize)
			}
		}
	}
}
func (ck *Clerk) dispatchBatch(buffer []*batchGetReq) {
	ck.mu.Lock()
	cfg := ck.config
	ck.mu.Unlock()
	reqsByGid := make(map[int64][]*batchGetReq)
	for _, req := range buffer {
		shard := key2shard(req.key)
		gid := cfg.Shards[int64(shard)]
		reqsByGid[gid] = append(reqsByGid[gid], req)
	}

	for gid, reqs := range reqsByGid {
		if gid != 0 {
			go ck.flushBatchForGroup(gid, reqs)
		} else {
			// 如果分片还没分配机器，直接返回空
			for _, req := range reqs {
				req.respCh <- ""
			}
		}
	}
}
func (ck *Clerk) flushBatchForGroup(gid int64, reqs []*batchGetReq) {
	keys := make([]string, len(reqs))
	tss := make([]uint64, len(reqs))
	for i, r := range reqs {
		keys[i] = r.key
		tss[i] = r.ts
	}

	args := &kvpb.BatchGetArgs{
		Keys:     keys,
		ClientId: ck.clientId,
		Tss:      tss,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}

	for {
		ck.mu.Lock()
		cfg := ck.config
		ck.mu.Unlock()

		group, ok := cfg.Groups[gid]
		if !ok || len(group.Servers) == 0 {
			time.Sleep(100 * time.Millisecond)
			ck.refreshConfig()
			for _, r := range reqs {
				ck.batchCh <- r
			}
			return
		}

		success := false
		for i := 0; i < len(group.Servers); i++ {
			client := ck.getClient(group.Servers[i])
			if client == nil {
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			reply, err := client.BatchGet(ctx, args)
			cancel()

			if err != nil || reply.Err == kvpb.Error_ERR_WRONG_LEADER || reply.Err == kvpb.Error_ERR_TIMEOUT {
				continue
			}

			if reply.Err == kvpb.Error_OK {
				for _, req := range reqs {
					if val, ok := reply.Values[req.key]; ok {
						req.respCh <- val
					} else {
						req.respCh <- ""
					}
				}
				success = true
				break
			}

			if reply.Err == kvpb.Error_ERR_WRONG_GROUP {
				break 
			}
		}

		if success {
			return
		}

		time.Sleep(100 * time.Millisecond)
		ck.refreshConfig()
		for _, r := range reqs {
			ck.batchCh <- r
		}
		return
	}
}
func (ck *Clerk) Watch(ctx context.Context, key string, isPrefix bool, callback func(op, key, val string, ts uint64)) {
	if isPrefix {
		go ck.watchAllGroups(ctx, key, isPrefix, callback)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		shard := key2shard(key)
		ck.mu.Lock()
		gid := ck.config.Shards[int64(shard)]
		ck.mu.Unlock()

		if gid != 0 {
			ck.watchLoop(ctx, gid, key, isPrefix, callback) // 阻塞监听，一旦被踢出会返回
		} else {
			time.Sleep(500 * time.Millisecond)
			ck.refreshConfig()
		}
	}
}

func (ck *Clerk) watchAllGroups(ctx context.Context, key string, isPrefix bool, callback func(op, key, val string, ts uint64)) {
	activeGroups := make(map[int64]context.CancelFunc)
	defer func() {
		for _, cancel := range activeGroups {
			cancel()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		ck.mu.Lock()
		cfg := ck.config
		ck.mu.Unlock()

		// 动态发现系统中的所有 Group，并为它们各自开启一个监听管道
		for gid := range cfg.Groups {
			if _, exists := activeGroups[gid]; !exists {
				groupCtx, cancel := context.WithCancel(ctx)
				activeGroups[gid] = cancel
				go ck.watchLoop(groupCtx, gid, key, isPrefix, callback)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
			ck.refreshConfig() // 每两秒检查一次有没有新的物理组加入
		}
	}
}

func (ck *Clerk) watchLoop(ctx context.Context, gid int64, key string, isPrefix bool, callback func(op, key, val string, ts uint64)) {
	var lastTs uint64 = 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		ck.mu.Lock()
		var servers []string
		if group, ok := ck.config.Groups[gid]; ok {
			servers = group.Servers
		}
		ck.mu.Unlock()

		if servers != nil && len(servers) > 0 {
			for i := 0; i < len(servers); i++ {
				client := ck.getClient(servers[i])
				if client == nil {
					continue
				}

				req := &kvpb.WatchRequest{Key: key, IsPrefix: isPrefix, StartTs: lastTs}
				streamCtx, cancel := context.WithCancel(ctx)
				stream, err := client.Watch(streamCtx, req)
				if err != nil {
					cancel()
					continue
				}

				for {
					resp, err := stream.Recv()
					if err != nil {
						cancel()
						break // 遇到断线、或者被服务器 (ERR_WRONG_GROUP_MIGRATED) 踢出
					}
					lastTs = resp.Ts
					callback(resp.Op, resp.Key, resp.Value, resp.Ts)
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
			ck.refreshConfig()
			if !isPrefix {
				shard := key2shard(key)
				ck.mu.Lock()
				newGid := ck.config.Shards[int64(shard)]
				ck.mu.Unlock()
				if newGid != gid {
					return
				}
			}
		}
	}
}
