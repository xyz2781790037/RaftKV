package server

import (
	"RaftKV/proto/kvpb"
	pb "RaftKV/proto/raftpb"
	"RaftKV/service/kvraft"
	"RaftKV/service/raft"
	"RaftKV/service/storage"
	"RaftKV/tool"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"google.golang.org/grpc"
)

type StartOptions struct {
	NodeID       int64             // ID 必须在这里，因为启动前就得指定
	Port         string            // ":8001"
	Peers        *raft.PeerManager // 邻居列表
	DataDir      string            // "data"
	MaxRaftState int64
}

// Server: 包含“运行时组件” + “配置信息”
type Server struct {
	// 运行时组件 (Runtime)
	GrpcServer *grpc.Server
	KVServer   *kvraft.KVServer
	Listener   net.Listener

	// 配置信息 (Config) - 直接把 Options 存一份在这里
	Options StartOptions
}

func ServerStart(opts StartOptions) (*Server, error) {
	nodeDir := fmt.Sprintf("%s/node_%d", opts.DataDir, opts.NodeID)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create dir %s: %v", nodeDir, err)
	}
	persister := storage.NewRaftStorage(nodeDir, opts.NodeID)
	kv := kvraft.StartKVServer(opts.Peers, opts.NodeID, persister, opts.MaxRaftState)
	lis, err := net.Listen("tcp", opts.Port)
	if err != nil {
		tool.Log.Error("Listen failed", "err", err)
		return nil, err
	}
	s := grpc.NewServer()
	// 注册 KV 服务 (供客户端调用)
	kvpb.RegisterRaftKVServer(s, kv)
	// 注册 Raft 服务 (供其他节点调用 CallAppendEntries 等)
	pb.RegisterRaftServer(s, kv.GetRaft())
	tool.Log.Info("Server started", "id", opts.NodeID, "port", opts.Port)
	go func() {
		s.Serve(lis)
	}()
	return &Server{
		GrpcServer: s,
		KVServer:   kv,
		Listener:   lis,
		Options:    opts,
	}, nil
}
func (s *Server) Stop() {
	tool.Log.Info("Stopping Server...", "ID", s.Options.NodeID)

	// 1. 停止 gRPC 监听 (不再接收新请求)
	if s.GrpcServer != nil {
		s.GrpcServer.Stop()
	}
	// 2. 停止 Raft 和 KV 逻辑
	if s.KVServer != nil {
		s.KVServer.Kill()
	}

	tool.Log.Info("Server stopped.")
}
func (s *Server) WaitForExit() {
	sigCh := make(chan os.Signal, 1)
	// 监听 Ctrl+C (Interrupt) 和 kill (SIGTERM)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	
	// 阻塞在这里，直到收到信号
	<-sigCh
	
	// 收到信号后，自动调用 Stop
	s.Stop()
}
func ParseFlags() (StartOptions,bool) {
	// 定义 flag
	nodeID := flag.Int64("id", 1, "ID of this node")
	port := flag.String("port", ":8001", "Port to listen on")
	peersRaw := flag.String("peers", "", "Peers list (e.g. 1=localhost:8001,2=localhost:8002)")
	dataDir := flag.String("dir", "data", "Data directory")
	maxState := flag.Int64("maxstate", 1000000, "Max Raft State size before snapshot")
	// 解析
	flag.Parse()

	// 验证必填项
	if *peersRaw == "" {
		fmt.Println("Error: -peers argument is required")
		flag.Usage()
		return StartOptions{},false
	}

	// 处理 peers 字符串转 map
	peersMap := make(map[int64]string)

	parts := strings.Split(*peersRaw, ",")
	for _, p := range parts {
		kv := strings.Split(p, "=")
		if len(kv) == 2 {
			pid, _ := strconv.ParseInt(kv[0], 10, 64)
			peersMap[pid] = kv[1]
		}
	}
	peers := raft.NewPeerManager(peersMap, *nodeID)
	return StartOptions{
		NodeID:  *nodeID,
		Port:    *port,
		Peers:   peers,
		DataDir: *dataDir,
		MaxRaftState: *maxState,
	},true
}
