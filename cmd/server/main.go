package main

import (
	"RaftKV/server"
	"RaftKV/tool"
	"fmt"
	"os"

	"net/http"
	_ "net/http/pprof"
)
func main() {
	go func() {
        http.ListenAndServe("localhost:6060", nil)
    }()
	opts,ok := server.ParseFlags()
	if !ok{
		os.Exit(1)
	}
	fmt.Printf("📌 進程 PID: %d\n", os.Getpid())
	server,err := server.ServerStart(opts)
	if err != nil{
		tool.Log.Error("server start failed!","err",err)
		os.Exit(1)
	}
	server.WaitForExit()
}
// package main

// import (
// 	"context"
// 	"fmt"
// 	"math/rand"
// 	"net"
// 	"os"
// 	"runtime"
// 	"strings"
// 	"time"

// 	kvpb "RaftKV/proto/kvpb"
// 	pb "RaftKV/proto/raftpb"
// 	"RaftKV/service/kvraft"
// 	"RaftKV/service/raft"
// 	"RaftKV/service/storage"

// 	"google.golang.org/grpc"
// 	"google.golang.org/grpc/credentials/insecure"
// )

// const (
// 	TEST_NODE_COUNT = 3
// 	TEST_DATA_DIR   = "perf_test_data"
// )

// var (
// 	testNodeAddrs = []string{
// 		"localhost:19101",
// 		"localhost:19102",
// 		"localhost:19103",
// 	}
// )

// type TestServer struct {
// 	id     int64
// 	addr   string
// 	server interface{}
// 	peers  *raft.PeerManager
// }

// type TestCluster struct {
// 	servers []*TestServer
// 	clerks  []*ClerkPerf
// 	dataDir string
// }

// type ClerkPerf struct {
// 	servers  []kvpb.RaftKVClient
// 	clientId int64
// 	seqId    int64
// 	leaderId int
// }

// type PerfMetrics struct {
// 	Name       string
// 	TotalOps   int64
// 	SuccessOps int64
// 	FailedOps  int64
// 	Duration   time.Duration
// 	Throughput float64
// 	AvgLat     time.Duration
// }

// func NewTestCluster(numNodes int) (*TestCluster, error) {
// 	tc := &TestCluster{
// 		servers: make([]*TestServer, numNodes),
// 		clerks:  make([]*ClerkPerf, 0),
// 		dataDir: fmt.Sprintf("%s_%d", TEST_DATA_DIR, time.Now().UnixNano()),
// 	}

// 	os.RemoveAll(TEST_DATA_DIR)
// 	os.MkdirAll(tc.dataDir, 0755)

// 	for i := 0; i < numNodes; i++ {
// 		peersMap := make(map[int64]string)
// 		for j := 0; j < numNodes; j++ {
// 			peersMap[int64(j)] = testNodeAddrs[j]
// 		}
// 		tc.servers[i] = &TestServer{
// 			id:    int64(i),
// 			addr:  testNodeAddrs[i],
// 			peers: raft.NewPeerManager(peersMap, int64(i)),
// 		}
// 	}

// 	for i := 0; i < numNodes; i++ {
// 		nodeDir := fmt.Sprintf("%s/node_%d", tc.dataDir, i)
// 		os.MkdirAll(nodeDir, 0755)

// 		persister := storage.NewRaftStorage(nodeDir, int64(i))
// 		kv := kvraft.StartKVServer(tc.servers[i].peers, int64(i), persister, -1)

// 		lis, err := net.Listen("tcp", fmt.Sprintf(":191%02d", i+1))
// 		if err != nil {
// 			tc.Shutdown()
// 			return nil, fmt.Errorf("节点%d监听失败: %v", i, err)
// 		}

// 		grpcServer := grpc.NewServer()
// 		kvpb.RegisterRaftKVServer(grpcServer, kv)
// 		pb.RegisterRaftServer(grpcServer, kv.GetRaft())

// 		go grpcServer.Serve(lis)

// 		tc.servers[i].server = map[string]interface{}{
// 			"GrpcServer": grpcServer,
// 			"KVServer":   kv,
// 			"Listener":   lis,
// 		}
// 	}

// 	fmt.Println("等待Raft集群选举Leader...")
// 	time.Sleep(3 * time.Second)

// 	for i := 0; i < numNodes; i++ {
// 		clerks := make([]string, numNodes)
// 		for j := 0; j < numNodes; j++ {
// 			clerks[j] = testNodeAddrs[j]
// 		}
// 		tc.clerks = append(tc.clerks, NewClerkPerf(clerks))
// 	}

// 	return tc, nil
// }

// func (tc *TestCluster) Shutdown() {
// 	for _, s := range tc.servers {
// 		if s != nil && s.server != nil {
// 			serverMap := s.server.(map[string]interface{})
// 			if grpcServer, ok := serverMap["GrpcServer"].(*grpc.Server); ok {
// 				grpcServer.Stop()
// 			}
// 			if kv, ok := serverMap["KVServer"].(*kvraft.KVServer); ok {
// 				kv.Kill()
// 			}
// 		}
// 	}
// 	time.Sleep(500 * time.Millisecond)
// 	os.RemoveAll(TEST_DATA_DIR)
// }

// func NewClerkPerf(servers []string) *ClerkPerf {
// 	ck := &ClerkPerf{
// 		servers:  make([]kvpb.RaftKVClient, len(servers)),
// 		clientId: rand.Int63(),
// 		seqId:    1,
// 		leaderId: 0,
// 	}
// 	for i, addr := range servers {
// 		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
// 		if err != nil {
// 			continue
// 		}
// 		ck.servers[i] = kvpb.NewRaftKVClient(conn)
// 	}
// 	return ck
// }

// func (ck *ClerkPerf) Get(key string) string {
// 	args := &kvpb.GetArgs{
// 		Key:     key,
// 		ClientId: ck.clientId,
// 		SeqId:   ck.seqId,
// 	}
// 	for {
// 		if ck.leaderId >= len(ck.servers) {
// 			ck.leaderId = 0
// 		}
// 		client := ck.servers[ck.leaderId]
// 		if client == nil {
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			continue
// 		}
// 		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
// 		reply, err := client.Get(ctx, args)
// 		cancel()
// 		if err != nil {
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			continue
// 		}
// 		switch reply.Err {
// 		case kvpb.Error_ERR_WRONG_LEADER, kvpb.Error_ERR_TIMEOUT:
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			continue
// 		case kvpb.Error_ERR_NO_KEY:
// 			return ""
// 		case kvpb.Error_OK:
// 			return reply.Value
// 		}
// 		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 	}
// }

// func (ck *ClerkPerf) Put(key, value string) {
// 	ck.seqId++
// 	args := &kvpb.PutAppendArgs{
// 		Key:      key,
// 		Value:    value,
// 		Op:       "Put",
// 		ClientId: ck.clientId,
// 		SeqId:    ck.seqId,
// 	}
// 	for {
// 		if ck.leaderId >= len(ck.servers) {
// 			ck.leaderId = 0
// 		}
// 		client := ck.servers[ck.leaderId]
// 		if client == nil {
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			continue
// 		}
// 		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
// 		reply, err := client.PutAppend(ctx, args)
// 		cancel()
// 		if err != nil {
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			continue
// 		}
// 		if reply.Err == kvpb.Error_ERR_WRONG_LEADER || reply.Err == kvpb.Error_ERR_TIMEOUT {
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			continue
// 		}
// 		return
// 	}
// }

// func (ck *ClerkPerf) Append(key, value string) {
// 	ck.seqId++
// 	args := &kvpb.PutAppendArgs{
// 		Key:      key,
// 		Value:    value,
// 		Op:       "Append",
// 		ClientId: ck.clientId,
// 		SeqId:    ck.seqId,
// 	}
// 	for {
// 		if ck.leaderId >= len(ck.servers) {
// 			ck.leaderId = 0
// 		}
// 		client := ck.servers[ck.leaderId]
// 		if client == nil {
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			continue
// 		}
// 		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
// 		reply, err := client.PutAppend(ctx, args)
// 		cancel()
// 		if err != nil {
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			continue
// 		}
// 		if reply.Err == kvpb.Error_ERR_WRONG_LEADER || reply.Err == kvpb.Error_ERR_TIMEOUT {
// 			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
// 			continue
// 		}
// 		return
// 	}
// }

// func (tc *TestCluster) GetClerk() *ClerkPerf {
// 	if len(tc.clerks) == 0 {
// 		return NewClerkPerf(testNodeAddrs)
// 	}
// 	return tc.clerks[rand.Intn(len(tc.clerks))]
// }

// func randString(n int) string {
// 	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
// 	b := make([]byte, n)
// 	for i := range b {
// 		b[i] = letters[rand.Intn(len(letters))]
// 	}
// 	return string(b)
// }

// func printMetrics(m *PerfMetrics) {
// 	fmt.Printf("\n")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Printf("  %s\n", m.Name)
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Printf("  总操作数: %d\n", m.TotalOps)
// 	fmt.Printf("  成功操作: %d\n", m.SuccessOps)
// 	fmt.Printf("  失败操作: %d\n", m.FailedOps)
// 	fmt.Printf("  总耗时: %v\n", m.Duration)
// 	fmt.Printf("  吞吐量: %.2f ops/s\n", m.Throughput)
// 	fmt.Printf("  平均延迟: %v\n", m.AvgLat)
// 	successRate := float64(100)
// 	if m.TotalOps > 0 {
// 		successRate = float64(m.SuccessOps) * 100 / float64(m.TotalOps)
// 	}
// 	fmt.Printf("  成功率: %.2f%%\n", successRate)
// 	fmt.Println(strings.Repeat("=", 50))
// }

// func testPutPerformance(cluster *TestCluster, numOps int) *PerfMetrics {
// 	fmt.Println("\n")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Println("  测试1: 基础Put性能测试")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Printf("  配置: %d次Put操作\n\n", numOps)

// 	start := time.Now()
// 	success := 0
// 	failed := 0

// 	for i := 0; i < numOps; i++ {
// 		ck := cluster.GetClerk()
// 		key := fmt.Sprintf("put_test_key_%d", i)
// 		ck.Put(key, randString(100))
// 		success++
// 		if (i+1)%1000 == 0 {
// 			fmt.Printf("  进度: %d/%d\n", i+1, numOps)
// 		}
// 	}

// 	elapsed := time.Since(start)
// 	avgLat := time.Duration(0)
// 	if success > 0 {
// 		avgLat = elapsed / time.Duration(success)
// 	}

// 	return &PerfMetrics{
// 		Name:       "Put性能测试",
// 		TotalOps:   int64(numOps),
// 		SuccessOps: int64(success),
// 		FailedOps:  int64(failed),
// 		Duration:   elapsed,
// 		Throughput: float64(success) / elapsed.Seconds(),
// 		AvgLat:     avgLat,
// 	}
// }

// func testGetPerformance(cluster *TestCluster, numOps int) *PerfMetrics {
// 	fmt.Println("\n")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Println("  测试2: 基础Get性能测试")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Printf("  配置: %d次Get操作\n\n", numOps)

// 	for i := 0; i < numOps; i++ {
// 		ck := cluster.GetClerk()
// 		key := fmt.Sprintf("get_test_key_%d", i)
// 		ck.Put(key, randString(100))
// 	}
// 	fmt.Printf("  预写入 %d 条数据完成\n", numOps)

// 	time.Sleep(500 * time.Millisecond)

// 	start := time.Now()
// 	success := 0
// 	failed := 0

// 	for i := 0; i < numOps; i++ {
// 		ck := cluster.GetClerk()
// 		key := fmt.Sprintf("get_test_key_%d", i%numOps)
// 		_ = ck.Get(key)
// 		success++
// 		if (i+1)%1000 == 0 {
// 			fmt.Printf("  进度: %d/%d\n", i+1, numOps)
// 		}
// 	}

// 	elapsed := time.Since(start)
// 	avgLat := time.Duration(0)
// 	if success > 0 {
// 		avgLat = elapsed / time.Duration(success)
// 	}

// 	return &PerfMetrics{
// 		Name:       "Get性能测试",
// 		TotalOps:   int64(numOps),
// 		SuccessOps: int64(success),
// 		FailedOps:  int64(failed),
// 		Duration:   elapsed,
// 		Throughput: float64(success) / elapsed.Seconds(),
// 		AvgLat:     avgLat,
// 	}
// }

// func testMixedPerformance(cluster *TestCluster, numOps int) *PerfMetrics {
// 	fmt.Println("\n")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Println("  测试3: 混合操作性能测试")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Printf("  配置: %d次混合操作\n\n", numOps)

// 	start := time.Now()
// 	success := 0
// 	failed := 0

// 	for i := 0; i < numOps; i++ {
// 		ck := cluster.GetClerk()
// 		key := fmt.Sprintf("mixed_key_%d", i)

// 		op := rand.Float64()
// 		if op < 0.4 {
// 			ck.Put(key, "value")
// 		} else if op < 0.8 {
// 			_ = ck.Get(key)
// 		} else {
// 			ck.Append(key, "_appended")
// 		}
// 		success++
// 		if (i+1)%1000 == 0 {
// 			fmt.Printf("  进度: %d/%d\n", i+1, numOps)
// 		}
// 	}

// 	elapsed := time.Since(start)
// 	avgLat := time.Duration(0)
// 	if success > 0 {
// 		avgLat = elapsed / time.Duration(success)
// 	}

// 	return &PerfMetrics{
// 		Name:       "混合操作测试",
// 		TotalOps:   int64(numOps),
// 		SuccessOps: int64(success),
// 		FailedOps:  int64(failed),
// 		Duration:   elapsed,
// 		Throughput: float64(success) / elapsed.Seconds(),
// 		AvgLat:     avgLat,
// 	}
// }

// func testConcurrentPerformance(cluster *TestCluster, numWorkers, numOpsPerWorker int) *PerfMetrics {
// 	fmt.Println("\n")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Println("  测试4: 并发性能测试")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Printf("  配置: %d个并发worker, 每个%d次操作\n\n", numWorkers, numOpsPerWorker)

// 	totalOps := numWorkers * numOpsPerWorker
// 	done := make(chan bool, numWorkers)
// 	success := int64(0)
// 	failed := int64(0)
// 	var start time.Time

// 	for w := 0; w < numWorkers; w++ {
// 		go func(workerId int) {
// 			for i := 0; i < numOpsPerWorker; i++ {
// 				ck := cluster.GetClerk()
// 				key := fmt.Sprintf("concurrent_key_%d_%d", workerId, i)
// 				ck.Put(key, randString(100))
// 			}
// 			done <- true
// 		}(w)
// 	}

// 	start = time.Now()
// 	for w := 0; w < numWorkers; w++ {
// 		<-done
// 	}

// 	elapsed := time.Since(start)
// 	success = int64(totalOps)
// 	avgLat := time.Duration(0)
// 	if success > 0 {
// 		avgLat = elapsed / time.Duration(success)
// 	}

// 	return &PerfMetrics{
// 		Name:       fmt.Sprintf("并发测试 (%d workers)", numWorkers),
// 		TotalOps:   int64(totalOps),
// 		SuccessOps: success,
// 		FailedOps:  failed,
// 		Duration:   elapsed,
// 		Throughput: float64(success) / elapsed.Seconds(),
// 		AvgLat:     avgLat,
// 	}
// }

// func testLeaderFailover(cluster *TestCluster, numOps int) *PerfMetrics {
// 	fmt.Println("\n")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Println("  测试5: Leader故障转移测试")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Printf("  配置: 先正常写入%d次, 然后模拟故障\n\n", numOps)

// 	fmt.Println("  阶段1: 正常写入...")
// 	start := time.Now()
// 	success := 0
// 	for i := 0; i < numOps; i++ {
// 		ck := cluster.GetClerk()
// 		key := fmt.Sprintf("failover_key_%d", i)
// 		ck.Put(key, "value")
// 		success++
// 	}
// 	elapsed1 := time.Since(start)
// 	fmt.Printf("  正常阶段完成: %d 操作 in %v\n", success, elapsed1)

// 	fmt.Println("\n  阶段2: 模拟Leader故障（停止节点0）...")
// 	if len(cluster.servers) > 0 && cluster.servers[0] != nil && cluster.servers[0].server != nil {
// 		serverMap := cluster.servers[0].server.(map[string]interface{})
// 		if grpcServer, ok := serverMap["GrpcServer"].(*grpc.Server); ok {
// 			grpcServer.Stop()
// 		}
// 		if kv, ok := serverMap["KVServer"].(*kvraft.KVServer); ok {
// 			kv.Kill()
// 		}
// 		cluster.servers[0].server = nil
// 		fmt.Println("  节点0已停止，等待新Leader选举...")
// 		time.Sleep(3 * time.Second)
// 	}

// 	fmt.Println("\n  阶段3: 故障后写入...")
// 	start = time.Now()
// 	for i := 0; i < numOps; i++ {
// 		ck := cluster.GetClerk()
// 		key := fmt.Sprintf("after_failover_%d", i)
// 		ck.Put(key, "value")
// 		success++
// 	}
// 	elapsed2 := time.Since(start)
// 	fmt.Printf("  故障后阶段完成: %d 操作 in %v\n", numOps, elapsed2)

// 	totalElapsed := elapsed1 + elapsed2
// 	avgLat := time.Duration(0)
// 	if success > 0 {
// 		avgLat = totalElapsed / time.Duration(success)
// 	}

// 	return &PerfMetrics{
// 		Name:       "Leader故障转移测试",
// 		TotalOps:   int64(success),
// 		SuccessOps: int64(success),
// 		FailedOps:  0,
// 		Duration:   totalElapsed,
// 		Throughput: float64(success) / totalElapsed.Seconds(),
// 		AvgLat:     avgLat,
// 	}
// }

// func testLongRunning(cluster *TestCluster, duration time.Duration) *PerfMetrics {
// 	fmt.Println("\n")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Println("  测试6: 长时间稳定性测试")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Printf("  配置: 持续写入 %v\n\n", duration)

// 	success := 0
// 	failed := 0
// 	start := time.Now()
// 	interval := 100 * time.Millisecond
// 	lastPrint := start

// 	for time.Since(start) < duration {
// 		ck := cluster.GetClerk()
// 		key := fmt.Sprintf("long_running_%d", success)
// 		ck.Put(key, randString(100))
// 		ck.Get(key)
// 		success += 2

// 		if time.Since(lastPrint) >= time.Second {
// 			elapsed := time.Since(start)
// 			fmt.Printf("  进度: %d 操作, 耗时: %v, 吞吐量: %.2f ops/s\n",
// 				success, elapsed, float64(success)/elapsed.Seconds())
// 			lastPrint = time.Now()
// 		}
// 		time.Sleep(interval)
// 	}

// 	elapsed := time.Since(start)
// 	avgLat := time.Duration(0)
// 	if success > 0 {
// 		avgLat = elapsed / time.Duration(success)
// 	}

// 	fmt.Println()

// 	return &PerfMetrics{
// 		Name:       "长时间稳定性测试",
// 		TotalOps:   int64(success),
// 		SuccessOps: int64(success),
// 		FailedOps:  int64(failed),
// 		Duration:   elapsed,
// 		Throughput: float64(success) / elapsed.Seconds(),
// 		AvgLat:     avgLat,
// 	}
// }

// func testValueSizeImpact(cluster *TestCluster, numOps int, valueSize int) *PerfMetrics {
// 	fmt.Println("\n")
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Printf("  测试7: 值大小影响测试 (值大小: %d bytes)\n", valueSize)
// 	fmt.Println(strings.Repeat("=", 50))
// 	fmt.Printf("  配置: %d次Put操作\n\n", numOps)

// 	start := time.Now()
// 	success := 0

// 	for i := 0; i < numOps; i++ {
// 		ck := cluster.GetClerk()
// 		key := fmt.Sprintf("size_test_%d_%d", valueSize, i)
// 		ck.Put(key, randString(valueSize))
// 		success++
// 		if (i+1)%500 == 0 {
// 			fmt.Printf("  进度: %d/%d\n", i+1, numOps)
// 		}
// 	}

// 	elapsed := time.Since(start)
// 	avgLat := time.Duration(0)
// 	if success > 0 {
// 		avgLat = elapsed / time.Duration(success)
// 	}

// 	return &PerfMetrics{
// 		Name:       fmt.Sprintf("值大小测试 (%d bytes)", valueSize),
// 		TotalOps:   int64(numOps),
// 		SuccessOps: int64(success),
// 		FailedOps:  0,
// 		Duration:   elapsed,
// 		Throughput: float64(success) / elapsed.Seconds(),
// 		AvgLat:     avgLat,
// 	}
// }

// func main() {
// 	fmt.Println("\n")
// 	fmt.Println(strings.Repeat("=", 60))
// 	fmt.Println("           KVraft 性能测试 - 开始执行")
// 	fmt.Println(strings.Repeat("=", 60))

// 	fmt.Printf("\n系统信息:\n")
// 	fmt.Printf("  - Go版本: %s\n", runtime.Version())
// 	fmt.Printf("  - CPU核心数: %d\n", runtime.NumCPU())

// 	fmt.Println("\n")
// 	fmt.Println(strings.Repeat("-", 50))
// 	fmt.Println("  步骤1: 创建测试集群")
// 	fmt.Println(strings.Repeat("-", 50))

// 	cluster, err := NewTestCluster(TEST_NODE_COUNT)
// 	if err != nil {
// 		fmt.Printf("\n错误: 创建测试集群失败: %v\n", err)
// 		os.Exit(1)
// 	}
// 	fmt.Printf("  成功创建 %d 节点 Raft 集群!\n\n", TEST_NODE_COUNT)

// 	var allMetrics []*PerfMetrics

// 	m1 := testPutPerformance(cluster, 5000)
// 	allMetrics = append(allMetrics, m1)
// 	printMetrics(m1)

// 	m2 := testGetPerformance(cluster, 5000)
// 	allMetrics = append(allMetrics, m2)
// 	printMetrics(m2)

// 	m3 := testMixedPerformance(cluster, 5000)
// 	allMetrics = append(allMetrics, m3)
// 	printMetrics(m3)

// 	for _, workers := range []int{2, 4, 8} {
// 		m := testConcurrentPerformance(cluster, workers, 1000)
// 		allMetrics = append(allMetrics, m)
// 		printMetrics(m)
// 	}

// 	m6 := testLeaderFailover(cluster, 2000)
// 	allMetrics = append(allMetrics, m6)
// 	printMetrics(m6)

// 	m7 := testLongRunning(cluster, 10*time.Second)
// 	allMetrics = append(allMetrics, m7)
// 	printMetrics(m7)

// 	for _, size := range []int{10, 100, 1000, 5000} {
// 		m := testValueSizeImpact(cluster, 2000, size)
// 		allMetrics = append(allMetrics, m)
// 		printMetrics(m)
// 	}

// 	cluster.Shutdown()

// 	fmt.Println("\n")
// 	fmt.Println(strings.Repeat("=", 60))
// 	fmt.Println("  性能测试汇总")
// 	fmt.Println(strings.Repeat("=", 60))

// 	var totalOps, totalSuccess int64
// 	var totalTime time.Duration

// 	for _, m := range allMetrics {
// 		totalOps += m.TotalOps
// 		totalSuccess += m.SuccessOps
// 		totalTime += m.Duration
// 	}

// 	avgThroughput := float64(totalSuccess) / totalTime.Seconds()

// 	fmt.Printf("\n  总测试数: %d\n", len(allMetrics))
// 	fmt.Printf("  总操作数: %d\n", totalOps)
// 	fmt.Printf("  总耗时: %v\n", totalTime)
// 	fmt.Printf("  平均吞吐量: %.2f ops/s\n", avgThroughput)
// 	if totalOps > 0 {
// 		rate := float64(totalSuccess) * 100 / float64(totalOps)
// 		fmt.Printf("  总成功率: %.2f%%\n", rate)
// 	}

// 	fmt.Println("\n")
// 	fmt.Println(strings.Repeat("=", 60))
// 	fmt.Println("           KVraft 性能测试 - 完成")
// 	fmt.Println(strings.Repeat("=", 60))
// 	fmt.Println("\n")
// }
