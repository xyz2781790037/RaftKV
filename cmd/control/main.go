package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	pb "RaftKV/proto/raftpb"
	"RaftKV/proto/shardpb"
	"RaftKV/service/raft"
	"RaftKV/service/raftapi"
	"RaftKV/service/shardmaster"
	"RaftKV/service/storage"
	"RaftKV/tool"

	"google.golang.org/grpc"
)

func main() {
	nodeId := flag.Int64("id", 1, "控制面节点 ID")
	port := flag.String("port", ":9001", "控制面监听端口")
	peersStr := flag.String("peers", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "控制面节点地址(逗号分隔)")
	dataDir := flag.String("dir", "data/master", "数据存储目录")
	flag.Parse()

	peerAddrs := strings.Split(*peersStr, ",")
	initialPeers := make(map[int64]string)
	for i, addr := range peerAddrs {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			initialPeers[int64(i+1)] = addr
		}
	}
	peerManager := raft.NewPeerManager(initialPeers, *nodeId)

	nodeDir := fmt.Sprintf("%s/node_%d", *dataDir, *nodeId)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		tool.Log.Fatal("创建目录失败", "err", err)
	}
	persister := storage.NewRaftStorage(nodeDir, *nodeId)

	applyCh := make(chan raftapi.ApplyMsg, 10000)
	sm := shardmaster.Make(peerManager, *nodeId, persister, applyCh)

	lis, err := net.Listen("tcp", *port)
	if err != nil {
		tool.Log.Fatal("监听失败", "err", err)
	}
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024*1024*1024),
		grpc.MaxSendMsgSize(1024*1024*1024),
	)
	shardpb.RegisterShardMasterServer(s, sm)
	pb.RegisterRaftServer(s, sm.GetRaft())

	tool.Log.Info("ShardMaster 控制节点已启动", "ID", *nodeId, "Port", *port)
	go func() {
		if err := s.Serve(lis); err != nil {
			tool.Log.Fatal("gRPC 运行出错", "err", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	tool.Log.Info("收到退出信号，正在关闭控制节点...")
	sm.Kill()
	s.GracefulStop()
}