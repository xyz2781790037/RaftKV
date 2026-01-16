package kvraft

import (
	kvpb "RaftKV/proto/kvpb"
	"RaftKV/tool"
	"context"
	"crypto/rand"
	"math/big"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Clerk struct {
	servers  []kvpb.RaftKVClient // gRPC 客户端连接池
	clientId int64               // 客户端唯一标识
	seqId    int64               // 请求序列号
	leaderId int                 // 当前认为的 Leader 索引
}
func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    return bigx.Int64()
}
func MakeClerk( servers []string) *Clerk {
    ck := &Clerk{
		servers:  make([]kvpb.RaftKVClient, len(servers)),
		clientId: nrand(),
		seqId:    1,
		leaderId: 0,
	}
	for i,addr := range servers{
		conn,err := grpc.NewClient(addr,grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			tool.Log.Warn("Newclient error", "err", err)
			continue
		}
		ck.servers[i] = kvpb.NewRaftKVClient(conn)
	}
    return ck
}
func (ck *Clerk) Get(key string) string {
	args := &kvpb.GetArgs{
		Key: key,
		ClientId: ck.clientId,
		SeqId: ck.seqId,
	}
	for {
		client := ck.servers[ck.leaderId]
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		reply, err := client.Get(ctx, args)
		cancel()
		if err != nil {
			// 连不上，说明挂了或网络分区，换下一个
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			tool.Log.Warn("说明挂了或网络分区，换下一个","err",err)
			continue
		}
		switch reply.Err {
		case kvpb.Error_ERR_WRONG_LEADER, kvpb.Error_ERR_TIMEOUT:
			// 找错人了 -> 换
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue

		case kvpb.Error_ERR_NO_KEY:
			return ""

		case kvpb.Error_OK:
			return reply.Value
		}
	}
}

// PutAppend shared logic
func (ck *Clerk) PutAppend(key string, value string, op string) {
    ck.seqId++

	args := &kvpb.PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	for {
		client := ck.servers[ck.leaderId]
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		
		reply, err := client.PutAppend(ctx, args)
		cancel()

		if err != nil {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			tool.Log.Warn("说明挂了或网络分区，换下一个","err",err)
			continue
		}

		switch reply.Err {
		case kvpb.Error_ERR_WRONG_LEADER, kvpb.Error_ERR_TIMEOUT:
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			tool.Log.Info("换下一个进行","err",reply.Err)
			continue

		case kvpb.Error_OK:
			tool.Log.Info("succeed to return")
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

func (ck *Clerk) Delete(key string) {
    // 调用 PutAppend，Value 传空字符串即可，Operation 传 "Delete"
    ck.PutAppend(key, "", "Delete")
}