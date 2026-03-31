package main

import (
	"RaftKV/service/kvraft" // 引用你的 SDK 包
	"bufio"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const concurrency = 1000

func randStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func main() {
	mastersRaw := flag.String("masters", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "ShardMaster 集群地址列表，用逗号分隔")
	flag.Parse()

	masterAddrs := strings.Split(*mastersRaw, ",")
	fmt.Println("正在连接 ShardMaster 控制中心:", masterAddrs, "...")

	// 🚨 修改点：把大脑地址传给 MakeClerk
	ck := kvraft.MakeClerk(masterAddrs)
	fmt.Println("✅ 客户端初始化完成！它已拉取到最新路由表。")
	
	var watchMutex sync.Mutex
	activeWatches := make(map[string]context.CancelFunc)
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("\n------------------------------------------------")
		fmt.Println("请选择操作类型 (输入 q 退出):")
		fmt.Println(" 1 [put]      写入数据")
		fmt.Println(" 2 [append]   追加数据")
		fmt.Println(" 3 [get]      查询数据")
		fmt.Println(" 4 [delete]   删除数据")
		fmt.Println(" 5 [auto]     顺序写入数据")
		fmt.Println(" 6 [auto]     并发写入数据 (不同Key)")
		fmt.Println(" 7 [auto]     并发读取数据 (不同Key)")
		fmt.Println(" 8 [mvcc]     极高并发同键读写压测 (MVCC)")
		fmt.Println(" 9 [ttl]      带过期时间的写入测试")
		fmt.Println(" 10 [ttl-batch] 批量并发写入带 TTL 的数据 (压测 Compaction)")
		fmt.Println(" 11 [mvcc get] mvcc查询数据")
		fmt.Println(" 12 [watch] watch数据")
		fmt.Println(" 13 [watch] 关闭watch")
		fmt.Println(" 14 [Node] 新增节点 (微观扩容)")
		fmt.Println(" 15 [Node] 删除节点 (微观缩容)")
		fmt.Println(" 16 [CAS] 乐观锁秒杀")
		fmt.Println(" 17 [SDK] 分布式锁")
		fmt.Print("请输入指令 > ")

		var amount, index int
		op, _ := reader.ReadString('\n')
		op = strings.TrimSpace(strings.ToLower(op))

		if op == "q" || op == "quit" || op == "exit" {
			fmt.Println("正在退出客户端，再见！")
			break
		} else if op == "5" || op == "6" || op == "7" || op == "62" {
			amount = readInt(reader, "输入测试数量 > ")
			index = readInt(reader, "输入测试开始位置 > ")
		}

		switch op {
		case "1", "2":
			fmt.Print("请输入 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			if key == "" {
				fmt.Println("错误: Key 不能为空")
				continue
			}

			fmt.Print("请输入 Value > ")
			value, _ := reader.ReadString('\n')
			value = strings.TrimSpace(value)

			fmt.Printf("正在请求 %s(%s, %s)...\n", op, key, value)
			if op == "1" {
				ck.Put(key, value)
				fmt.Println("Put 成功！")
			} else {
				ck.Append(key, value)
				fmt.Println("Append 成功！")
			}

		case "4":
			fmt.Print("请输入要删除的 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			fmt.Printf("正在请求 Delete(%s)...\n", key)
			ck.Delete(key)
			fmt.Println("Delete 成功！(如果 Key 不存在则无事发生)")

		case "3":
			fmt.Print("请输入要查询的 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			fmt.Printf("正在查询 Get(%s)...\n", key)
			val := ck.Get(key)
			if val == "" {
				fmt.Println("查询结果: <空> (Key 不存在或已过期)")
			} else {
				fmt.Printf("查询结果: %s\n", val)
			}

		case "":
			continue

		case "5":
			fmt.Println("开始顺序写入", amount, "条数据...")
			startTime := time.Now()
			for i := index; i <= index+amount; i++ {
				key := strconv.Itoa(i)
				value := key + "-" + key + "-" + key + "-" + key
				ck.Put(key, value)
				if i%10 == 0 {
					fmt.Printf("\r进度: %d / %d (耗时: %v)", i-index+1, amount, time.Since(startTime))
				}
			}
			fmt.Printf("\n压测完成！总耗时: %v\n", time.Since(startTime))

		case "6", "62":
			startTime := time.Now()
			var wg sync.WaitGroup
			wg.Add(concurrency)

			var finishedOps int64
			totalOps := int64(concurrency * amount)

			go func() {
				for {
					completed := atomic.LoadInt64(&finishedOps)
					if completed >= totalOps {
						break
					}
					fmt.Printf("\r进度: %d / %d (TPS: %.0f) 耗时: %v",
						completed, totalOps,
						float64(completed)/time.Since(startTime).Seconds(),
						time.Since(startTime))
					time.Sleep(1 * time.Second)
				}
			}()

			for i := 0; i < concurrency; i++ {
				i1 := i
				ckk := kvraft.MakeClerk(masterAddrs)
				go func(x int, c *kvraft.Clerk) {
					defer wg.Done()
					for j := index; j < amount+index; j++ {
						key := strconv.Itoa(x) + ":" + strconv.Itoa(j)
						var value string
						if op == "6" {
							value = "val-" + key + "-" + time.Now().String()
						} else {
							value = "val-" + key + "-" + time.Now().String() + time.Now().String() + time.Now().String() + time.Now().String() + time.Now().String() + time.Now().String()
						}

						c.Put(key, value)
						atomic.AddInt64(&finishedOps, 1)
					}
				}(i1, ckk)
			}
			wg.Wait()
			fmt.Printf("\n全部完成！总耗时: %v\n", time.Since(startTime))

		case "7":
			var totalOps int64 = int64(amount) * concurrency
			fmt.Println("\n------------------------------------------------")
			fmt.Println("准备开始并发读取测试 (Get Benchmark)...")
			time.Sleep(1 * time.Second)

			startReadTime := time.Now()
			var wgRead sync.WaitGroup
			wgRead.Add(concurrency)

			var finishedReadOps int64
			var successReadCount int64

			go func() {
				for {
					completed := atomic.LoadInt64(&finishedReadOps)
					if completed >= totalOps {
						break
					}
					elapsed := time.Since(startReadTime).Seconds()
					ops := float64(completed) / elapsed
					fmt.Printf("\r🔍 读取进度: %d / %d (TPS: %.0f) 耗时: %.1fs",
						completed, totalOps, ops, elapsed)
					time.Sleep(1 * time.Second)
				}
			}()

			for i := 0; i < concurrency; i++ {
				i := i
				go func() {
					defer wgRead.Done()
					for j := index; j < amount+index; j++ {
						key := strconv.Itoa(i) + ":" + strconv.Itoa(j)
						val := ck.Get(key)
						atomic.AddInt64(&finishedReadOps, 1)
						if val != "" {
							atomic.AddInt64(&successReadCount, 1)
						} else if val == "" {
							fmt.Println(key)
						}
					}
				}()
			}
			wgRead.Wait()
			readDuration := time.Since(startReadTime)
			totalReads := atomic.LoadInt64(&finishedReadOps)
			successReads := atomic.LoadInt64(&successReadCount)
			ops := float64(totalReads) / readDuration.Seconds()
			successRate := float64(successReads) / float64(totalReads) * 100.0

			fmt.Printf("\n\n============ 读取测试报告 ============\n")
			fmt.Printf("总耗时:        %v\n", readDuration)
			fmt.Printf("总请求次数:    %d\n", totalReads)
			fmt.Printf("成功读取数量:  %d\n", successReads)
			fmt.Printf("平均 OPS/s:    %.2f\n", ops)
			fmt.Printf("读取成功率:    %.2f%%\n", successRate)
			fmt.Println("========================================")

		case "8":
			fmt.Println("准备启动 MVCC 并发压测...")
			fmt.Print("按回车键开始压测... ")
			reader.ReadString('\n')

			testKey := "mvcc_hot_key"
			var writeCount int64
			var readCount int64
			var wg sync.WaitGroup

			timeout := time.After(5 * time.Second)
			stopFlag := int32(0)

			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func(writerId int) {
					defer wg.Done()
					for atomic.LoadInt32(&stopFlag) == 0 {
						val := fmt.Sprintf("val-%d-%d", writerId, time.Now().UnixNano())
						ck.Put(testKey, val)
						atomic.AddInt64(&writeCount, 1)
					}
				}(i)
			}

			for i := 0; i < 500; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for atomic.LoadInt32(&stopFlag) == 0 {
						ck.Get(testKey)
						atomic.AddInt64(&readCount, 1)
					}
				}()
			}

			<-timeout
			atomic.StoreInt32(&stopFlag, 1)
			wg.Wait()

			wCount := atomic.LoadInt64(&writeCount)
			rCount := atomic.LoadInt64(&readCount)

			fmt.Printf("\n============ MVCC 压测报告 ============\n")
			fmt.Printf("完成写操作: %d 次\n", wCount)
			fmt.Printf("完成读操作: %d 次\n", rCount)

		case "9":
			fmt.Print("请输入要测试的 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}

			fmt.Print("请输入 Value > ")
			value, _ := reader.ReadString('\n')
			value = strings.TrimSpace(value)

			ttlSec := readInt(reader, "⏳ 请输入过期时间 (秒) > ")

			fmt.Printf("正在请求带过期时间的写入 (%s, %s, %d秒)...\n", key, value, ttlSec)
			ck.PutWithTTL(key, value, int64(ttlSec))

			fmt.Printf("写入成功！\n")
			
		case "10":
			amount = readInt(reader, "输入每个线程写入的数量 > ")
			index = readInt(reader, "输入测试起始编号 > ")
			ttlSec := readInt(reader, "请输入统一的过期时间 (秒) > ")

			startTime := time.Now()
			var wg sync.WaitGroup
			wg.Add(concurrency)

			var finishedOps int64
			totalOps := int64(concurrency * amount)

			go func() {
				for {
					completed := atomic.LoadInt64(&finishedOps)
					if completed >= totalOps {
						break
					}
					fmt.Printf("\r进度: %d / %d (TPS: %.0f) 耗时: %v",
						completed, totalOps,
						float64(completed)/time.Since(startTime).Seconds(),
						time.Since(startTime))
					time.Sleep(1 * time.Second)
				}
			}()

			for i := 0; i < concurrency; i++ {
				i1 := i
				ckk := kvraft.MakeClerk(masterAddrs)
				go func(x int, c *kvraft.Clerk) {
					defer wg.Done()
					for j := index; j < amount+index; j++ {
						key := "ttl-key-" + strconv.Itoa(x) + ":" + strconv.Itoa(j)
						value := "ttl-val-" + key + "-" + time.Now().String()
						c.PutWithTTL(key, value, int64(ttlSec))
						atomic.AddInt64(&finishedOps, 1)
					}
				}(i1, ckk)
			}
			wg.Wait()
			fmt.Printf("\nTTL 批量写入完成！总耗时: %v\n", time.Since(startTime))
			
		case "11":
			fmt.Print("请输入 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			ts := readUint(reader, "输入ts的时刻 > ")
			val := ck.GetAt(key, ts)
			if val == "" {
				fmt.Println("查询结果: <空> ")
			} else {
				fmt.Printf("查询结果: %s\n", val)
			}
			
		case "12":
			fmt.Print("请输入要 Watch 的 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			watchMutex.Lock()
			if _, exists := activeWatches[key]; exists {
				fmt.Printf("警告：Key [%s] 已经在监听中了！\n", key)
				watchMutex.Unlock()
				continue
			}
			watchMutex.Unlock()
			fmt.Print("是否为前缀监听？[Y/n] (默认Y) > ")
			Prefix, _ := reader.ReadString('\n')
			Prefix = strings.TrimSpace(Prefix)
			isPrefix := (Prefix == "Y" || Prefix == "y" || Prefix == "")

			ctx, cancel := context.WithCancel(context.Background())

			watchMutex.Lock()
			activeWatches[key] = cancel
			watchMutex.Unlock()

			fmt.Printf("正在后台看着 Key: [%s]\n", key)

			go ck.Watch(ctx, key, isPrefix, func(op string, watchKey string, val string, ts uint64) {
				fmt.Printf("\n你选择的 Key 发生变化了!\n动作: %s\n键: %s\n值: %s\n版本(Ts): %d\n> ", op, watchKey, val, ts)
			})
			
		case "13":
			watchMutex.Lock()
			if len(activeWatches) == 0 {
				fmt.Println("当前没有任何正在运行的 Watch 任务。")
				watchMutex.Unlock()
				continue
			}
			for id := range activeWatches {
				fmt.Printf(" - 任务 Key: %s\n", id)
			}
			watchMutex.Unlock()
			fmt.Print("请输入要关闭的 Watch 的 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			watchMutex.Lock()
			if cancelFn, exists := activeWatches[key]; exists {
				cancelFn()
				delete(activeWatches, key)
				fmt.Printf("%s 已成功取消监听！\n", key)
			} else {
				fmt.Println("找不到该任务。")
			}
			watchMutex.Unlock()
			
		case "14":
			gid := int64(readInt(reader, "请输入该节点所属的机器组 GID (例如 100) > "))
			nodeId := int64(readInt(reader, "请输入要新增的节点 ID (例如 4) > "))
			if nodeId <= 0 {
				fmt.Println("节点 ID 必须大于 0")
				continue
			}
			fmt.Print("请输入该节点的地址 (例如 localhost:8004) > ")
			addr, _ := reader.ReadString('\n')
			addr = strings.TrimSpace(addr)
			if addr == "" {
				fmt.Println("地址不能为空")
				continue
			}

			fmt.Printf("正在向集群发送扩容请求: AddNode(GID:%d, Node:%d, Addr:%s)...\n", gid, nodeId, addr)
			ck.AddNode(gid, nodeId, addr)
			
		case "15":
			gid := int64(readInt(reader, "请输入该节点所属的机器组 GID (例如 100) > "))
			nodeId := int64(readInt(reader, "请输入要踢出的节点 ID (例如 4) > "))
			if nodeId <= 0 {
				fmt.Println("节点 ID 必须大于 0")
				continue
			}

			fmt.Printf("正在向集群发送缩容请求: RemoveNode(GID:%d, Node:%d)...\n", gid, nodeId)
			ck.RemoveNode(gid, nodeId)
			
		case "16":
			fmt.Println("开始测试 CAS 乐观锁并发扣库存...")
			testKey := "iphone_stock"
			ck.Put(testKey, "100") 
			fmt.Println("初始库存已设置为 100，启动 1000 个并发线程抢购！")
			successMan := make([]int, 0, 100)
			var wg sync.WaitGroup
			var mu sync.Mutex
			wg.Add(1000)

			for i := 0; i < 1000; i++ {
				ckk := kvraft.MakeClerk(masterAddrs)
				go func(threadId int) {
					defer wg.Done()
					for {
						oldStr := ckk.Get(testKey)
						if oldStr == "0" {
							break
						}

						oldInt, _ := strconv.Atoi(oldStr)
						newVal := strconv.Itoa(oldInt - 1)
						success := ckk.CAS(testKey, oldStr, newVal)
						if success {
							mu.Lock()
							successMan = append(successMan, threadId)
							mu.Unlock()
							fmt.Println(threadId,"获得")
							break
						} else {
							time.Sleep(10 * time.Millisecond)
						}
					}
				}(i)
			}
			wg.Wait()
			fmt.Printf("最终数据库库存余量: %s\n", ck.Get(testKey))
			fmt.Println("最终成功协程数:", successMan)
			
		case "17":
			fmt.Println("开始测试 [分布式锁 SDK] (CAS + Watch 唤醒机制)...")

			var wg sync.WaitGroup
			wg.Add(50)

			for i := 0; i < 50; i++ {
				go func(threadId int) {
					defer wg.Done()

					localCk := kvraft.MakeClerk(masterAddrs)
					mu := localCk.NewMutex("print_task_lock")

					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					if mu.Lock(ctx) {
						fmt.Printf("[协程 %d] 成功拿到锁！正在独占执行关键业务...\n", threadId)
						time.Sleep(1 * time.Second)
						fmt.Printf("[协程 %d] 业务执行完毕，释放锁。\n", threadId)
						mu.Unlock()
					} else {
						fmt.Printf("[协程 %d] 获取锁超时，放弃任务。\n", threadId)
					}
				}(i)
			}
			wg.Wait()
			fmt.Println("分布式锁测试完毕！")
		default:
			fmt.Printf("未知指令: [%s]，请输入菜单对应的数字\n", op)
		}
	}
}

func readInt(reader *bufio.Reader, prompt string) int {
	for {
		fmt.Print(prompt)
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if v, err := strconv.Atoi(line); err == nil {
			return v
		}
		fmt.Println("请输入合法整数")
	}
}
func readUint(reader *bufio.Reader, prompt string) uint64 {
	for {
		fmt.Print(prompt)
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if v, err := strconv.ParseUint(line, 10, 64); err == nil {
			return v
		}
		fmt.Println("请输入合法整数")
	}
}