package raft

import (
	pb "RaftKV/proto/raftpb"
	"RaftKV/tool"
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const rpcTimeout = 200 * time.Millisecond

type RaftPeer struct {
	mu   sync.Mutex // 保护连接字段
	addr string     // 远端节点的网络地址 (e.g., "localhost:50052")

	// gRPC 客户端连接资源
	conn *grpc.ClientConn // 保持持久连接，以避免每次 RPC 都重新建立连接
	stub pb.RaftClient    // Protobuf 生成的客户端接口 (Stub)
	id   int64
}
func NewRaftPeer(id int64, addr string) *RaftPeer {
	rp := &RaftPeer{
		id:   id,
		addr: addr,
	}
	rp.Connect()
	return rp
}
func (rp *RaftPeer) Connect() {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.conn != nil {
		rp.conn.Close()
	}
	conn, err := grpc.NewClient(rp.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tool.Log.Error("Connect failed", "err", err)
		rp.conn = nil
		rp.stub = nil
		return
	}
	rp.conn = conn
	rp.stub = pb.NewRaftClient(conn)
	tool.Log.Info("connected successful", "id", rp.id, "addr", rp.addr)
}
func (rp *RaftPeer) Close() {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.conn != nil {
		rp.conn.Close()
		rp.conn = nil
		rp.stub = nil
	}
}
func (rp *RaftPeer) CallAppendEntries(in *pb.AppendEntriesArgs)(*pb.AppendEntriesReply,bool) {
	rp.mu.Lock()
	stub := rp.stub
	rp.mu.Unlock()
	if stub == nil{
		tool.Log.Error("stub in peer is nil")
		return nil,false
	}
	ctx,cancel := context.WithTimeout(context.Background(),rpcTimeout)
	defer cancel()

	reply,err := stub.AppendEntries(ctx,in)
	if err != nil{
		tool.Log.Error("AppendEntries failed","err",err)
		return nil,false
	}
	return reply,true
}
func (rp *RaftPeer) CallRequestVote(in *pb.RequestVoteArgs)(*pb.RequestVoteReply,bool) {
	rp.mu.Lock()
	stub := rp.stub
	rp.mu.Unlock()
	if stub == nil{
		tool.Log.Error("stub in peer is nil")
		return nil,false
	}
	ctx,cancel := context.WithTimeout(context.Background(),rpcTimeout)
	defer cancel()

	reply,err := stub.RequestVote(ctx,in)
	if err != nil{
		tool.Log.Error("RequestVote failed","err",err)
		return nil,false
	}
	return reply,true
}
func (rp *RaftPeer) CallInstallSnapshot(in *pb.InstallSnapshotArgs)(*pb.InstallSnapshotReply,bool) {
	rp.mu.Lock()
	stub := rp.stub
	rp.mu.Unlock()
	if stub == nil{
		tool.Log.Error("stub in peer is nil")
		return nil,false
	}
	ctx,cancel := context.WithTimeout(context.Background(),5 *time.Second)
	defer cancel()

	reply,err := stub.InstallSnapshot(ctx,in)
	if err != nil{
		tool.Log.Error("InstallSnapshot failed","err",err)
		return nil,false
	}
	return reply,true
}
