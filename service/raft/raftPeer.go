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

const rpcTimeout = 3000 * time.Millisecond

type RaftPeer struct {
	mu   sync.Mutex // 保护连接字段
	addr string     // 远端节点的网络地址 (e.g., "localhost:50052")

	// gRPC 客户端连接资源
	conn *grpc.ClientConn // 保持持久连接，以避免每次 RPC 都重新建立连接
	stub pb.RaftClient    // Protobuf 生成的客户端接口 (Stub)
	id   int64
}
type PeerManager struct {
	mu    sync.RWMutex // 专门保护 peers map
	peers map[int64]*RaftPeer
	myID  int64 // 记录自己的 ID，方便遍历时跳过自己
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
func (rp *RaftPeer) CallAppendEntries(in *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, bool) {
	rp.mu.Lock()
	stub := rp.stub
	rp.mu.Unlock()
	if stub == nil {
		tool.Log.Error("stub in peer is nil")
		return nil, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	reply, err := stub.AppendEntries(ctx, in)
	if err != nil {
		// tool.Log.Error("AppendEntries failed", "err", err)
		return nil, false
	}
	return reply, true
}
func (rp *RaftPeer) CallRequestVote(in *pb.RequestVoteArgs) (*pb.RequestVoteReply, bool) {
	rp.mu.Lock()
	stub := rp.stub
	rp.mu.Unlock()
	if stub == nil {
		tool.Log.Error("stub in peer is nil")
		return nil, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	reply, err := stub.RequestVote(ctx, in)
	if err != nil {
		tool.Log.Error("RequestVote failed", "err", err)
		return nil, false
	}
	return reply, true
}
func (rp *RaftPeer) CallInstallSnapshot(in *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, bool) {
	rp.mu.Lock()
	stub := rp.stub
	rp.mu.Unlock()
	if stub == nil {
		tool.Log.Error("stub in peer is nil")
		return nil, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := stub.InstallSnapshot(ctx, in)
	if err != nil {
		tool.Log.Error("InstallSnapshot failed", "err", err)
		return nil, false
	}
	return reply, true
}
func NewPeerManager(initialPeers map[int64]string, myID int64) *PeerManager {
	pm := &PeerManager{
		mu:   sync.RWMutex{},
		myID: myID,
	}
	pm.peers = make(map[int64]*RaftPeer, len(initialPeers))
	for id, addr := range initialPeers {
		pm.peers[id] = NewRaftPeer(id, addr)
	}
	return pm
}
func (pm *PeerManager) AddPeer(id int64, addr string) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.peers[id]; exists {
		return false
	}
	pm.peers[id] = NewRaftPeer(id, addr)
	tool.Log.Info("PeerManager: Added node", "id", id, "addr", addr)
	return true
}
func (pm *PeerManager) RemovePeer(id int64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if rp, exist := pm.peers[id]; exist {
		rp.Close()
		delete(pm.peers, id)
		tool.Log.Info("PeerManager: Removed node", "id", id)
	}

}
func (pm *PeerManager) GetPeer(id int64) (*RaftPeer, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if rp, exist := pm.peers[id]; exist {
		return rp, true
	}
	return nil, false
}
func (pm *PeerManager) CloneList() []*RaftPeer {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	list := make([]*RaftPeer, 0, len(pm.peers))
	for id, peer := range pm.peers {
		if id == pm.myID {
			continue
		}
		list = append(list, peer)
	}
	return list
}
func (pm *PeerManager) ListIDs() []int64 {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	// 0个有效数据会优先使用这些数据
	ids := make([]int64, 0, len(pm.peers))
	for id,_ := range pm.peers{
		ids = append(ids, id)
	}
	return ids
}
func (pm *PeerManager) PeerCount() int{
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.peers)
}
func (pm *PeerManager) QuorumSize() int{
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.peers)/2 + 1
}
func (pm *PeerManager) CloseAll(){
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	for _,peer := range pm.peers{
		peer.Close()
	}
}
