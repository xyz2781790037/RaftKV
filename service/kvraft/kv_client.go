// package kvraft

// import (
// 	kvpb "RaftKV/proto/kvpb"
// 	"RaftKV/tool"
// 	"context"
// 	"crypto/rand"
// 	"math/big"
// 	"time"

// 	"google.golang.org/grpc"
// 	"google.golang.org/grpc/credentials/insecure"
// )

// type Clerk struct {
// 	servers  []kvpb.RaftKVClient // gRPC 客户端连接池
// 	clientId int64               // 客户端唯一标识
// 	seqId    int64               // 请求序列号
// 	leaderId int                 // 当前认为的 Leader 索引
// }
// const maxMsgSize = 1024 * 1024 * 64
// func nrand() int64 {
//     max := big.NewInt(int64(1) << 62)
//     bigx, _ := rand.Int(rand.Reader, max)
//     return bigx.Int64()
// }
// func MakeClerk( servers []string) *Clerk {
//     ck := &Clerk{
// 		servers:  make([]kvpb.RaftKVClient, len(servers)),
// 		clientId: nrand(),
// 		seqId:    1,
// 		leaderId: 0,
// 	}
// 	for i,addr := range servers{
// 		conn,err := grpc.NewClient(addr,grpc.WithTransportCredentials(insecure.NewCredentials()),grpc.WithDefaultCallOptions(
// 				grpc.MaxCallRecvMsgSize(maxMsgSize),
// 				grpc.MaxCallSendMsgSize(maxMsgSize),
// 			),)
// 		if err != nil {
// 			tool.Log.Warn("Newclient error", "err", err)
// 			continue
// 		}
// 		ck.servers[i] = kvpb.NewRaftKVClient(conn)
// 	}
//     return ck
// }
// func (ck *Clerk) Get(key string) string {
// 	args := &kvpb.GetArgs{
// 		Key: key,
// 		ClientId: ck.clientId,
// 		SeqId: ck.seqId,
// 	}
// 	for {
// 		client := ck.servers[ck.leaderId]
// 		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
// 		reply, err := client.Get(ctx, args)
// 		cancel()
// 		if err != nil {
// 			tool.Log.Warn("RPC连接失败，稍后重试", "leaderId", ck.leaderId, "err", err)
// 			time.Sleep(20 * time.Millisecond)
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			continue
// 		}
// 		switch reply.Err {
// 		case kvpb.Error_ERR_WRONG_LEADER, kvpb.Error_ERR_TIMEOUT:
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			if reply.Err == kvpb.Error_ERR_TIMEOUT {
// 				time.Sleep(10 * time.Millisecond)
// 			}
// 			continue

// 		case kvpb.Error_ERR_NO_KEY:
// 			return ""

// 		case kvpb.Error_OK:
// 			return reply.Value
// 		}
// 	}
// }

// // PutAppend shared logic
// func (ck *Clerk) PutAppend(key string, value string, op string) {
//     ck.seqId++

// 	args := &kvpb.PutAppendArgs{
// 		Key:      key,
// 		Value:    value,
// 		Op:       op,
// 		ClientId: ck.clientId,
// 		SeqId:    ck.seqId,
// 	}

// 	for {
// 		client := ck.servers[ck.leaderId]
// 		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)

// 		reply, err := client.PutAppend(ctx, args)
// 		cancel()

// 		if err != nil {
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			time.Sleep(20 * time.Millisecond)
// 			continue
// 		}

// 		switch reply.Err {
// 		case kvpb.Error_ERR_WRONG_LEADER, kvpb.Error_ERR_TIMEOUT:
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			time.Sleep(10 * time.Millisecond)
// 			tool.Log.Info("换下一个进行","err",reply.Err)
// 			continue

// 		case kvpb.Error_OK:
// 			return
// 		}
// 	}
// }

// // Put 接口封装
// func (ck *Clerk) Put(key string, value string) {
//     ck.PutAppend(key, value, "Put")
// }

// // Append 接口封装
// func (ck *Clerk) Append(key string, value string) {
//     ck.PutAppend(key, value, "Append")
// }

//	func (ck *Clerk) Delete(key string) {
//	    // 调用 PutAppend，Value 传空字符串即可，Operation 传 "Delete"
//	    ck.PutAppend(key, "", "Delete")
//	}
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type batchGetReq struct {
	key    string
	respCh chan string // 用于把结果传回给对应的 Get 调用者
}
type Clerk struct {
	servers  []kvpb.RaftKVClient // gRPC 客户端连接池
	clientId int64               // 客户端唯一标识
	seqId    int64               // 请求序列号
	leaderId int                 // 当前认为的 Leader 索引
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
		batchCh: make(chan *batchGetReq, 10000),
	}

	var maxMsgSize = 1024 * 1024 * 64
	for i, addr := range servers {
		// 建立长连接
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxMsgSize), // 接收限制调大
				grpc.MaxCallSendMsgSize(maxMsgSize), // 发送限制调大
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

func (ck *Clerk) Get(key string) string {
    // 创建一个接收结果的通道
    respCh := make(chan string, 1)
    
    // 1. 将请求扔进批处理队列
    ck.batchCh <- &batchGetReq{
        key:    key,
        respCh: respCh,
    }

    // 2. 阻塞等待，直到后台协程处理完并返回结果
    // 这里实现了“看似同步，实则异步合并”的效果
    val := <-respCh
    return val
}

func (ck *Clerk) PutAppend(key string, value string, op string) {

	ck.seqId++

	args := &kvpb.PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	timeoutCount := 0

	for {
		client := ck.servers[ck.leaderId]
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		reply, err := client.PutAppend(ctx, args)
		cancel()

		// --- RPC 错误处理 ---
		if err != nil {
			st, ok := status.FromError(err)
			// 超时重试逻辑
			if ok && st.Code() == codes.DeadlineExceeded {
				if timeoutCount < 1 {
					timeoutCount++
					tool.Log.Warn("请求超时，原地重试", "leaderId", ck.leaderId)
					time.Sleep(50 * time.Millisecond)
					continue // 👈 原地重试
				}
			}

			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			timeoutCount = 0
			time.Sleep(10 * time.Millisecond)
			continue
		}

		timeoutCount = 0

		switch reply.Err {
		case kvpb.Error_ERR_WRONG_LEADER:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue

		case kvpb.Error_ERR_TIMEOUT:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue

		case kvpb.Error_OK:
			return
		}
	}
}

// Put 接口封装
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append 接口封装
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// Delete 接口封装
func (ck *Clerk) Delete(key string) {
	ck.PutAppend(key, "", "Delete")
}
func (ck *Clerk) batcher() {
    const MaxBatchSize = 100
    const BatchTimeout = 2 * time.Millisecond

    buffer := make([]*batchGetReq, 0, MaxBatchSize)
    keys := make([]string, 0, MaxBatchSize)
    
    ticker := time.NewTicker(BatchTimeout)
    defer ticker.Stop()

    for {
        select {
        case req := <-ck.batchCh:
            buffer = append(buffer, req)
            keys = append(keys, req.key)

            if len(buffer) >= MaxBatchSize {
                // 🔥 关键修改：复制一份数据，启动协程去发送！
                // 必须 Copy，因为 buffer 切片马上要被清空重用了
                
                // 1. 浅拷贝 slice Header (reqs 和 keys 指向底层数组)
                // 但因为下面马上 make 了新的，所以这一步其实可以直接传 buffer
                // 为了安全起见，我们把变量传给闭包
                
                reqsToSend := buffer
                keysToSend := keys
                
                go ck.flushBatch(reqsToSend, keysToSend) // 🚀 异步发车！不要等！

                // 2. 换个新袋子装下一波
                buffer = make([]*batchGetReq, 0, MaxBatchSize)
                keys = make([]string, 0, MaxBatchSize)
            }

        case <-ticker.C:
            if len(buffer) > 0 {
                reqsToSend := buffer
                keysToSend := keys
                
                go ck.flushBatch(reqsToSend, keysToSend) // 🚀 异步发车！

                buffer = make([]*batchGetReq, 0, MaxBatchSize)
                keys = make([]string, 0, MaxBatchSize)
            }
        }
    }
}

// 真正发送 RPC 的函数
func (ck *Clerk) flushBatch(reqs []*batchGetReq, keys []string) {
    // 构建 RPC 参数
    args := &kvpb.BatchGetArgs{
        Keys:     keys,
        ClientId: ck.clientId,
        SeqId:    atomic.AddInt64(&ck.seqId, 1),
    }

    // 重试逻辑 (和普通 Get 类似，但针对的是一批)
    for {
        client := ck.servers[ck.leaderId]
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // 批处理稍微给长点时间
        reply, err := client.BatchGet(ctx, args)
        cancel()

        // --- 错误处理 (简化版) ---
        if err != nil || reply.Err == kvpb.Error_ERR_WRONG_LEADER || reply.Err == kvpb.Error_ERR_TIMEOUT {
            ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
            time.Sleep(10 * time.Millisecond)
            continue
        }

        // --- 成功！分发结果 ---
        if reply.Err == kvpb.Error_OK {
            // 遍历原始请求，从 reply.Values 里找对应的结果
            for _, req := range reqs {
                val, ok := reply.Values[req.key]
                if ok {
                    req.respCh <- val // 发回给阻塞的 Get()
                } else {
                    req.respCh <- ""  // 没找到，返回空
                }
            }
            return
        }
    }
}