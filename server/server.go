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
	GID          int64    
	MasterAddrs  []string
}

// Server: 包含“运行时组件” + “配置信息”
type Server struct {
	GrpcServer *grpc.Server
	KVServer   *kvraft.KVServer
	Listener   net.Listener
	Options    StartOptions
}

var maxMsgSize = 1024 * 1024 * 1024

func ServerStart(opts StartOptions) (*Server, error) {
	nodeDir := fmt.Sprintf("%s/node_%d", opts.DataDir, opts.NodeID)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create dir %s: %v", nodeDir, err)
	}
	persister := storage.NewRaftStorage(nodeDir, opts.NodeID)
	kv := kvraft.StartKVServer(opts.Peers, opts.NodeID, persister, opts.MaxRaftState, opts.GID, opts.MasterAddrs)
	
	lis, err := net.Listen("tcp", opts.Port)
	if err != nil {
		tool.Log.Error("Listen failed", "err", err)
		return nil, err
	}
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)
	
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

	// 1. 停止 gRPC 监听
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
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	s.Stop()
}
func ParseFlags() (StartOptions, bool) {
	nodeID := flag.Int64("id", 1, "ID of this node")
	port := flag.String("port", ":8001", "Port to listen on")
	gid := flag.Int64("gid", 100, "Replica Group ID") // 新增 GID
	peersRaw := flag.String("peers", "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003", "Peers list (comma separated)")
	mastersRaw := flag.String("masters", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "ShardMaster list") // 新增 Masters
	dataDir := flag.String("dir", "data", "Data directory")
	maxState := flag.Int64("maxstate", 100*1024*1024, "Max Raft State size")
	flag.Parse()
	peersMap := make(map[int64]string)
	parts := strings.Split(*peersRaw, ",")
	for i, p := range parts {
		addr := strings.TrimSpace(p)
		if addr != "" {
			peersMap[int64(i+1)] = addr
		}
	}
	peers := raft.NewPeerManager(peersMap, *nodeID)

	// 2. 解析大脑的机器列表
	var masterAddrs []string
	for _, m := range strings.Split(*mastersRaw, ",") {
		addr := strings.TrimSpace(m)
		if addr != "" {
			masterAddrs = append(masterAddrs, addr)
		}
	}

	return StartOptions{
		NodeID:       *nodeID,
		Port:         *port,
		Peers:        peers,
		DataDir:      *dataDir,
		MaxRaftState: *maxState,
		GID:          *gid,
		MasterAddrs:  masterAddrs,
	}, true
}