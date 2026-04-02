package shardmaster

import (
	"RaftKV/proto/shardpb"
	"RaftKV/tool"
	"context"
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ShardClerk struct {
	servers  []shardpb.ShardMasterClient
	clientId int64
	seqId    int64
	leaderId uint32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}
func MakeClerk(servers []string) *ShardClerk {
	ck := &ShardClerk{
		servers:  make([]shardpb.ShardMasterClient, len(servers)),
		clientId: nrand(),
		seqId:    1,
		leaderId: 0,
	}
	var maxMsgSize = 1024 * 1024 * 1024
	for i, addr := range servers {
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxMsgSize),
				grpc.MaxCallRecvMsgSize(maxMsgSize),
			),
		)
		if err != nil {
			tool.Log.Warn("ShardMaster client connect error", "err", err)
			continue
		}
		ck.servers[i] = shardpb.NewShardMasterClient(conn)
	}
	return ck
}
func (ck *ShardClerk) changeLeader(oldLeader uint32) {
	newLeader := (oldLeader + 1) % uint32(len(ck.servers))
	atomic.CompareAndSwapUint32(&ck.leaderId, oldLeader, newLeader)
}
func (ck *ShardClerk) Query(num int64) shardpb.Config {
	args := &shardpb.QueryArgs{
		Num: num,
	}
	for {
		leader := atomic.LoadUint32(&ck.leaderId)
		client := ck.servers[leader]

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		reply, err := client.Query(ctx, args)
		cancel()

		if err != nil || reply.WrongLeader {
			ck.changeLeader(leader)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if reply.Err == "" {
			return *reply.Config
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *ShardClerk) Join(servers map[int64]*shardpb.GroupServers) {
	args := &shardpb.JoinArgs{
		Servers:  servers,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	for {
		leader := atomic.LoadUint32(&ck.leaderId)
		client := ck.servers[leader]

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		reply, err := client.Join(ctx, args)
		cancel()

		if err != nil || reply.WrongLeader {
			ck.changeLeader(leader)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if reply.Err == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *ShardClerk) Leave(gids []int64) {
	args := &shardpb.LeaveArgs{
		GIDs:     gids,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	for {
		leader := atomic.LoadUint32(&ck.leaderId)
		client := ck.servers[leader]

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		reply, err := client.Leave(ctx, args)
		cancel()

		if err != nil || reply.WrongLeader {
			ck.changeLeader(leader)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if reply.Err == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *ShardClerk) Move(shard int64, gid int64) {
	args := &shardpb.MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	for {
		leader := atomic.LoadUint32(&ck.leaderId)
		client := ck.servers[leader]

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		reply, err := client.Move(ctx, args)
		cancel()

		if err != nil || reply.WrongLeader {
			ck.changeLeader(leader)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if reply.Err == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}
