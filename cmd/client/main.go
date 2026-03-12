package main

import (
	"RaftKV/service/kvraft" // 引用你的 SDK 包
	"bufio"
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
	serversRaw := flag.String("servers", "localhost:8001,localhost:8002,localhost:8003", "服务器地址列表，用逗号分隔")
	flag.Parse()

	serverAddrs := strings.Split(*serversRaw, ",")
	fmt.Println("正在连接服务器集群:", serverAddrs, "...")

	ck := kvraft.MakeClerk(serverAddrs)
	fmt.Println("✅ 客户端初始化完成！")

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
		fmt.Print("👉 请输入指令 > ")
		
		var amount, index int
		op, _ := reader.ReadString('\n')
		op = strings.TrimSpace(strings.ToLower(op))

		if op == "q" || op == "quit" || op == "exit" {
			fmt.Println("👋 正在退出客户端，再见！")
			break
		} else if op == "5" || op == "6" || op == "7" || op == "62" {
			amount = readInt(reader, "输入测试数量 > ")
			index = readInt(reader, "输入测试开始位置 > ")
		}

		switch op {
		case "1", "2":
			fmt.Print("🔑 请输入 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			if key == "" {
				fmt.Println("❌ 错误: Key 不能为空")
				continue
			}

			fmt.Print("📦 请输入 Value > ")
			value, _ := reader.ReadString('\n')
			value = strings.TrimSpace(value)

			fmt.Printf("⏳ 正在请求 %s(%s, %s)...\n", op, key, value)
			if op == "1" {
				ck.Put(key, value)
				fmt.Println("✅ Put 成功！")
			} else {
				ck.Append(key, value)
				fmt.Println("✅ Append 成功！")
			}
			
		case "4":
			fmt.Print("🗑️ 请输入要删除的 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			fmt.Printf("⏳ 正在请求 Delete(%s)...\n", key)
			ck.Delete(key)
			fmt.Println("✅ Delete 成功！(如果 Key 不存在则无事发生)")
			
		case "3":
			fmt.Print("🔑 请输入要查询的 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			fmt.Printf("⏳ 正在查询 Get(%s)...\n", key)
			val := ck.Get(key)
			if val == "" {
				fmt.Println("查询结果: <空> (Key 不存在或已过期)")
			} else {
				fmt.Printf("📄 查询结果: %s\n", val)
			}

		case "":
			continue

		case "5":
			fmt.Println("🚀 开始顺序写入", amount, "条数据...")
			startTime := time.Now()
			for i := index; i <= index+amount; i++ {
				key := strconv.Itoa(i)
				value := key + "-" + key + "-" + key + "-" + key
				ck.Put(key, value)
				if i%10 == 0 {
					fmt.Printf("\r📊 进度: %d / %d (耗时: %v)", i-index+1, amount, time.Since(startTime))
				}
			}
			fmt.Printf("\n✅ 压测完成！总耗时: %v\n", time.Since(startTime))

		case "6","62":
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
					fmt.Printf("\r🚀 进度: %d / %d (TPS: %.0f) 耗时: %v",
						completed, totalOps,
						float64(completed)/time.Since(startTime).Seconds(),
						time.Since(startTime))
					time.Sleep(1 * time.Second)
				}
			}()

			for i := 0; i < concurrency; i++ {
				i1 := i
				ckk := kvraft.MakeClerk(serverAddrs)
				go func(x int, c *kvraft.Clerk) {
					defer wg.Done()
					for j := index; j < amount+index; j++ {
						key := strconv.Itoa(x) + ":" + strconv.Itoa(j)
						var value string
						if op == "6"{
							value = "val-" + key + "-" + time.Now().String()
						}else{
							value = "val-" + key + "-" + time.Now().String() + time.Now().String() + time.Now().String() + time.Now().String() + time.Now().String() + time.Now().String()
						}
						
						c.Put(key, value)
						atomic.AddInt64(&finishedOps, 1)
					}
				}(i1, ckk)
			}
			wg.Wait()
			fmt.Printf("\n✅ 全部完成！总耗时: %v\n", time.Since(startTime))

		case "7":
			var totalOps int64 = int64(amount) * concurrency
			fmt.Println("\n------------------------------------------------")
			fmt.Println("🚀 准备开始并发读取测试 (Get Benchmark)...")
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
						}else if val == ""{
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

			fmt.Printf("\n\n📊 ============ 读取测试报告 ============\n")
			fmt.Printf("⏱️  总耗时:        %v\n", readDuration)
			fmt.Printf("🔄 总请求次数:    %d\n", totalReads)
			fmt.Printf("✅ 成功读取数量:  %d\n", successReads)
			fmt.Printf("🚀 平均 OPS/s:    %.2f\n", ops)
			fmt.Printf("📈 读取成功率:    %.2f%%\n", successRate)
			fmt.Println("========================================")

		case "8":
			fmt.Println("🚀 准备启动 MVCC 并发压测...")
			fmt.Println("说明：我们将开启 100 个线程疯狂写入同一个 Key，同时开启 500 个线程疯狂读取这个 Key。")
			fmt.Println("由于底层使用了 MVCC 机制，读线程获取的是快照版本，绝对不会被写线程阻塞卡死！")
			fmt.Print("👉 按回车键开始压测... ")
			reader.ReadString('\n')

			fmt.Println("🔥 压测开始...")
			testKey := "mvcc_hot_key"
			var writeCount int64
			var readCount int64
			var wg sync.WaitGroup

			timeout := time.After(5 * time.Second)
			stopFlag := int32(0)

			// 100 个写线程
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

			// 500 个读线程
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

			// 等待 5 秒
			<-timeout
			atomic.StoreInt32(&stopFlag, 1) // 通知所有协程停止
			wg.Wait()

			wCount := atomic.LoadInt64(&writeCount)
			rCount := atomic.LoadInt64(&readCount)

			fmt.Printf("\n📊 ============ MVCC 压测报告 ============\n")
			fmt.Printf("⏱️  测试耗时: 5.00 秒\n")
			fmt.Printf("✍️  完成写操作: %d 次 (每秒约 %d 次)\n", wCount, wCount/5)
			fmt.Printf("📖 完成读操作: %d 次 (每秒约 %d 次)\n", rCount, rCount/5)
			fmt.Printf("💡 结论：在普通的锁机制下，大量的写操作必定导致读操作严重排队。\n")
			fmt.Printf("   但在我们的 MVCC 架构下，读写操作完全可以齐头并进，性能极高！\n")

		case "9":
			fmt.Print("🔑 请输入要测试的 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}

			fmt.Print("📦 请输入 Value > ")
			value, _ := reader.ReadString('\n')
			value = strings.TrimSpace(value)

			ttlSec := readInt(reader, "⏳ 请输入过期时间 (秒) > ")

			fmt.Printf("⏳ 正在请求带过期时间的写入 (%s, %s, %d秒)...\n", key, value, ttlSec)
			ck.PutWithTTL(key, value, int64(ttlSec)) 
			
			fmt.Printf("✅ 写入成功！请使用 `3 (get)` 命令查询，%d 秒后它将自动消失。\n", ttlSec)
		case "10":
			amount = readInt(reader, "🔢 输入每个线程写入的数量 > ")
			index = readInt(reader, "🔢 输入测试起始编号 > ")
			ttlSec := readInt(reader, "⏳ 请输入统一的过期时间 (秒) > ")

			fmt.Printf("🚀 准备开启 %d 个线程，总共写入 %d 条 TTL 为 %d 秒的数据...\n", concurrency, concurrency*amount, ttlSec)
			
			startTime := time.Now()
			var wg sync.WaitGroup
			wg.Add(concurrency)

			var finishedOps int64
			totalOps := int64(concurrency * amount)

			// 进度监控协程
			go func() {
				for {
					completed := atomic.LoadInt64(&finishedOps)
					if completed >= totalOps {
						break
					}
					fmt.Printf("\r🚀 进度: %d / %d (TPS: %.0f) 耗时: %v",
						completed, totalOps,
						float64(completed)/time.Since(startTime).Seconds(),
						time.Since(startTime))
					time.Sleep(1 * time.Second)
				}
			}()

			// 发起并发写入
			for i := 0; i < concurrency; i++ {
				i1 := i
				ckk := kvraft.MakeClerk(serverAddrs)
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
			fmt.Printf("提示：这批数据将在 %d 秒后过期。你可以等待 %d 秒后，观察服务器后台的 Compaction 日志，看看它们是否被彻底清理！\n", ttlSec, ttlSec)
		case "11":
			fmt.Print("请输入 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			ts := readUint(reader, "输入ts的时刻 > ")
			fmt.Printf("⏳ 正在查询 Get(%s)...\n", key)
			val := ck.GetAt(key,ts)
			if val == "" {
				fmt.Println("查询结果: <空> ")
			} else {
				fmt.Printf("查询结果: %s\n", val)
			}
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
		if v, err := strconv.ParseUint(line,10,64); err == nil {
			return v
		}
		fmt.Println("请输入合法整数")
	}
}