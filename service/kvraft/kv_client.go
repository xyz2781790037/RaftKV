// package kvraft

// import (
// 	kvpb "RaftKV/proto/kvpb"
// 	"RaftKV/tool"
// 	"context"
// 	"crypto/rand"
// 	"math/big"
// 	"sync/atomic"
// 	"time"

// 	"google.golang.org/grpc"
// 	"google.golang.org/grpc/codes"
// 	"google.golang.org/grpc/credentials/insecure"
// 	"google.golang.org/grpc/status"
// )

// type batchGetReq struct {
// 	key    string
// 	ts     uint64
// 	respCh chan string // 用于把结果传回给对应的 Get 调用者
// }
// type Clerk struct {
// 	servers  []kvpb.RaftKVClient // gRPC 客户端连接池
// 	clientId int64               // 客户端唯一标识
// 	seqId    int64               // 请求序列号
// 	leaderId int                 // 当前认为的 Leader 索引
// 	batchCh  chan *batchGetReq
// }

// func nrand() int64 {
// 	max := big.NewInt(int64(1) << 62)
// 	bigx, _ := rand.Int(rand.Reader, max)
// 	return bigx.Int64()
// }

// func MakeClerk(servers []string) *Clerk {
// 	ck := &Clerk{
// 		servers:  make([]kvpb.RaftKVClient, len(servers)),
// 		clientId: nrand(),
// 		seqId:    1,
// 		leaderId: 0,
// 		batchCh:  make(chan *batchGetReq, 10000),
// 	}

// 	var maxMsgSize = 1024 * 1024 * 1024
// 	for i, addr := range servers {
// 		// 建立长连接
// 		conn, err := grpc.NewClient(addr,
// 			grpc.WithTransportCredentials(insecure.NewCredentials()),
// 			grpc.WithDefaultCallOptions(
// 				grpc.MaxCallRecvMsgSize(maxMsgSize), // 接收限制调大
// 				grpc.MaxCallSendMsgSize(maxMsgSize), // 发送限制调大
// 			),
// 		)
// 		if err != nil {
// 			tool.Log.Warn("Newclient error", "err", err)
// 			continue
// 		}
// 		ck.servers[i] = kvpb.NewRaftKVClient(conn)
// 	}
// 	go ck.batcher()
// 	return ck
// }

// func (ck *Clerk) Get(key string) string {
// 	respCh := make(chan string, 1)

// 	ck.batchCh <- &batchGetReq{
// 		key:    key,
// 		ts:     0,
// 		respCh: respCh,
// 	}
// 	val := <-respCh
// 	return val
// }
// func (ck *Clerk) GetAt(key string, ts uint64) string {
// 	respCh := make(chan string, 1)

// 	ck.batchCh <- &batchGetReq{
// 		key:    key,
// 		ts:     ts,
// 		respCh: respCh,
// 	}
// 	val := <-respCh
// 	return val
// }
// func (ck *Clerk) PutAppend(key string, value string, op string, ttl int64) {

// 	// ck.seqId++
// newSeqId := atomic.AddInt64(&ck.seqId, 1)
// 	args := &kvpb.PutAppendArgs{
// 		Key:      key,
// 		Value:    value,
// 		Op:       op,
// 		ClientId: ck.clientId,
// 		SeqId:    newSeqId,
// 		Ttl:      ttl,
// 	}

// 	timeoutCount := 0

// 	for {
// 		client := ck.servers[ck.leaderId]
// 		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
// 		reply, err := client.PutAppend(ctx, args)
// 		cancel()
// 		if err != nil {
// 			st, ok := status.FromError(err)
// 			if ok && st.Code() == codes.DeadlineExceeded {
// 				if timeoutCount < 1 {
// 					timeoutCount++
// 					tool.Log.Warn("请求超时，原地重试", "leaderId", ck.leaderId)
// 					time.Sleep(50 * time.Millisecond)
// 					continue
// 				}
// 			}

// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			timeoutCount = 0
// 			time.Sleep(10 * time.Millisecond)
// 			continue
// 		}

// 		timeoutCount = 0

// 		switch reply.Err {
// 		case kvpb.Error_ERR_WRONG_LEADER:
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			continue

// 		case kvpb.Error_ERR_TIMEOUT:
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			time.Sleep(10 * time.Millisecond)
// 			continue

// 		case kvpb.Error_OK:
// 			return
// 		}
// 	}
// }

// func (ck *Clerk) Put(key string, value string) {
// 	ck.PutAppend(key, value, "Put", 0)
// }

// func (ck *Clerk) Append(key string, value string) {
// 	ck.PutAppend(key, value, "Append", 0)
// }

// func (ck *Clerk) Delete(key string) {
// 	ck.PutAppend(key, "", "Delete", 0)
// }
// func (ck *Clerk) batcher() {
// 	const MaxBatchSize = 100
// 	const BatchTimeout = 2 * time.Millisecond

// 	buffer := make([]*batchGetReq, 0, MaxBatchSize)
// 	keys := make([]string, 0, MaxBatchSize)
// 	tss := make([]uint64,0,MaxBatchSize)

// 	ticker := time.NewTicker(BatchTimeout)
// 	defer ticker.Stop()

// 	for {
// 		select {
// 		case req := <-ck.batchCh:
// 			buffer = append(buffer, req)
// 			keys = append(keys, req.key)
// 			tss = append(tss, req.ts)

// 			if len(buffer) >= MaxBatchSize {

// 				reqsToSend := buffer
// 				keysToSend := keys
// 				tssToSend := tss

// 				go ck.flushBatch(reqsToSend, keysToSend,tssToSend)

// 				buffer = make([]*batchGetReq, 0, MaxBatchSize)
// 				keys = make([]string, 0, MaxBatchSize)
// 				tss = make([]uint64, 0, MaxBatchSize)
// 			}

// 		case <-ticker.C:
// 			if len(buffer) > 0 {
// 				reqsToSend := buffer
// 				keysToSend := keys
// 				tssToSend := tss

// 				go ck.flushBatch(reqsToSend, keysToSend,tssToSend)

// 				buffer = make([]*batchGetReq, 0, MaxBatchSize)
// 				keys = make([]string, 0, MaxBatchSize)
// 				tss = make([]uint64, 0, MaxBatchSize)
// 			}
// 		}
// 	}
// }

// func (ck *Clerk) flushBatch(reqs []*batchGetReq, keys []string,tss []uint64) {
// 	args := &kvpb.BatchGetArgs{
// 		Keys:     keys,
// 		ClientId: ck.clientId,
// 		Tss:  tss,
// 		SeqId:    atomic.AddInt64(&ck.seqId, 1),
// 	}

// 	for {
// 		client := ck.servers[ck.leaderId]
// 		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 批处理稍微给长点时间
// 		reply, err := client.BatchGet(ctx, args)
// 		cancel()

// 		if err != nil || reply.Err == kvpb.Error_ERR_WRONG_LEADER || reply.Err == kvpb.Error_ERR_TIMEOUT {
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			time.Sleep(10 * time.Millisecond)
// 			continue
// 		}

// 		if reply.Err == kvpb.Error_OK {
// 			for _, req := range reqs {
// 				val, ok := reply.Values[req.key]
// 				if ok {
// 					req.respCh <- val
// 				} else {
// 					req.respCh <- ""
// 				}
// 			}
// 			return
// 		}
// 	}
// }
// func (ck *Clerk) PutWithTTL(key string, value string, ttlSeconds int64) {
// 	ck.PutAppend(key, value, "Put", ttlSeconds)
// }
package kvraft

import (
	kvpb "RaftKV/proto/kvpb"
	"RaftKV/tool"
	"context"
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	// "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	// "google.golang.org/grpc/status"
)

type batchGetReq struct {
	key    string
	ts     uint64
	respCh chan string 
}
type Clerk struct {
	servers  []kvpb.RaftKVClient 
	clientId int64               
	seqId    int64               
	leaderId uint32
	batchCh  chan *batchGetReq
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

func MakeClerk(servers []string) *Clerk {
	ck := &Clerk{
		servers:  make([]kvpb.RaftKVClient, len(servers)),
		clientId: nrand(),
		seqId:    1,
		leaderId: 0,
		batchCh:  make(chan *batchGetReq, 10000),
	}

	var maxMsgSize = 1024 * 1024 * 1024
	for i, addr := range servers {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxMsgSize), 
				grpc.MaxCallSendMsgSize(maxMsgSize), 
			),
		)
		if err != nil {
			tool.Log.Warn("Newclient error", "err", err)
			continue
		}
		ck.servers[i] = kvpb.NewRaftKVClient(conn)
	}
	go ck.batcher()
	return ck
}

func (ck *Clerk) changeLeader(oldLeader uint32) {
	newLeader := (oldLeader + 1) % uint32(len(ck.servers))
	atomic.CompareAndSwapUint32(&ck.leaderId, oldLeader, newLeader)
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
		leader := atomic.LoadUint32(&ck.leaderId)
		client := ck.servers[leader]
		
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		reply, err := client.PutAppend(ctx, args)
		cancel()
		
		if err != nil {
			ck.changeLeader(leader)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		switch reply.Err {
		case kvpb.Error_ERR_WRONG_LEADER:
			ck.changeLeader(leader)
			time.Sleep(50 * time.Millisecond)
			continue

		case kvpb.Error_ERR_TIMEOUT:
			ck.changeLeader(leader)
			time.Sleep(50 * time.Millisecond)
			continue

		case kvpb.Error_OK:
			return
		}
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
func (ck *Clerk) batcher() {
	const MaxBatchSize = 100
	const BatchTimeout = 2 * time.Millisecond

	buffer := make([]*batchGetReq, 0, MaxBatchSize)
	keys := make([]string, 0, MaxBatchSize)
	tss := make([]uint64, 0, MaxBatchSize)

	ticker := time.NewTicker(BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case req := <-ck.batchCh:
			buffer = append(buffer, req)
			keys = append(keys, req.key)
			tss = append(tss, req.ts)

			if len(buffer) >= MaxBatchSize {

				reqsToSend := buffer
				keysToSend := keys
				tssToSend := tss

				go ck.flushBatch(reqsToSend, keysToSend, tssToSend)

				buffer = make([]*batchGetReq, 0, MaxBatchSize)
				keys = make([]string, 0, MaxBatchSize)
				tss = make([]uint64, 0, MaxBatchSize)
			}

		case <-ticker.C:
			if len(buffer) > 0 {
				reqsToSend := buffer
				keysToSend := keys
				tssToSend := tss

				go ck.flushBatch(reqsToSend, keysToSend, tssToSend)

				buffer = make([]*batchGetReq, 0, MaxBatchSize)
				keys = make([]string, 0, MaxBatchSize)
				tss = make([]uint64, 0, MaxBatchSize)
			}
		}
	}
}

func (ck *Clerk) flushBatch(reqs []*batchGetReq, keys []string, tss []uint64) {
	args := &kvpb.BatchGetArgs{
		Keys:     keys,
		ClientId: ck.clientId,
		Tss:      tss,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}

	for {
		leader := atomic.LoadUint32(&ck.leaderId) // 原子读取
		client := ck.servers[leader]
		
		// 批处理超时也可以缩短，防止一直占用协程资源
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) 
		reply, err := client.BatchGet(ctx, args)
		cancel()
		// tool.Log.Info("sss","err",err)
		if err != nil || reply.Err == kvpb.Error_ERR_WRONG_LEADER || reply.Err == kvpb.Error_ERR_TIMEOUT {
			ck.changeLeader(leader)
			time.Sleep(50 * time.Millisecond) // 	
			continue
		}

		if reply.Err == kvpb.Error_OK {
			for _, req := range reqs {
				val, ok := reply.Values[req.key]
				if ok {
					req.respCh <- val
				} else {
					req.respCh <- ""
				}
			}
			return
		}
	}
}
func (ck *Clerk) PutWithTTL(key string, value string, ttlSeconds int64) {
	ck.PutAppend(key, value, "Put", ttlSeconds)
}