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
	// 1. 初始化连接
	// 依然保留 flag，允许你在启动时指定服务器地址，例如: -servers="192.168.1.1:5001"
	serversRaw := flag.String("servers", "localhost:8001,localhost:8002,localhost:8003", "服务器地址列表，用逗号分隔")
	flag.Parse()

	serverAddrs := strings.Split(*serversRaw, ",")
	fmt.Println("正在连接服务器集群:", serverAddrs, "...")

	// 初始化 SDK (只做一次)
	ck := kvraft.MakeClerk(serverAddrs)
	fmt.Println("✅ 客户端初始化完成！")

	// 准备读取用户输入
	reader := bufio.NewReader(os.Stdin)

	// 2. 进入交互式循环
	for {
		fmt.Println("\n------------------------------------------------")
		fmt.Println("请选择操作类型 (输入 q 退出):")
		fmt.Println(" 1 [put]    写入数据")
		fmt.Println(" 2 [append] 追加数据")
		fmt.Println(" 3 [get]    查询数据")
		fmt.Println(" 4 [delete] 删除数据")
		fmt.Println(" 5 [auto put] 自动写入数据")
		fmt.Println(" 6 [auto put] 并发写入数据")
		fmt.Println(" 7 [auto put] 并发读取数据")
		fmt.Print("👉 请输入指令 > ")
		var amount, index int
		// 读取指令
		op, _ := reader.ReadString('\n')
		op = strings.TrimSpace(strings.ToLower(op)) // 去除回车和空格

		if op == "q" || op == "quit" || op == "exit" {
			fmt.Println("👋 正在退出客户端，再见！")
			break
		} else if op == "5" || op == "6" || op == "7" {
			amount = readInt(reader, "输入测试数量 > ")
			index = readInt(reader, "输入测试开始位置 > ")
		}

		// 根据指令分别处理
		switch op {
		case "1", "2":
			// 1. 输入 Key
			fmt.Print("🔑 请输入 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)

			if key == "" {
				fmt.Println("❌ 错误: Key 不能为空")
				continue
			}

			// 2. 输入 Value
			fmt.Print("📦 请输入 Value > ")
			value, _ := reader.ReadString('\n')
			value = strings.TrimSpace(value) // 这里的去除空格看你需求，如果value允许空格，可以用 strings.TrimRight(value, "\r\n")

			// 3. 执行请求
			fmt.Printf("⏳ 正在请求 %s(%s, %s)...\n", op, key, value)
			if op == "1" {
				ck.Put(key, value)
				fmt.Println("✅ Put 成功！")
			} else {
				ck.Append(key, value)
				fmt.Println("✅ Append 成功！")
			}
		case "4":
			// 1. 输入 Key
			fmt.Print("🗑️ 请输入要删除的 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)

			if key == "" {
				fmt.Println("❌ 错误: Key 不能为空")
				continue
			}

			// 2. 执行请求
			fmt.Printf("⏳ 正在请求 Delete(%s)...\n", key)
			ck.Delete(key) // 调用刚才写的 Client 方法
			fmt.Println("✅ Delete 成功！(如果 Key 不存在则无事发生)")
		case "3":
			// 1. 输入 Key
			fmt.Print("🔑 请输入要查询的 Key > ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)

			if key == "" {
				fmt.Println("❌ 错误: Key 不能为空")
				continue
			}

			// 2. 执行请求
			fmt.Printf("⏳ 正在查询 Get(%s)...\n", key)
			val := ck.Get(key)

			// 3. 显示结果
			if val == "" {
				fmt.Println("📭 查询结果: <空> (Key 不存在)")
			} else {
				fmt.Printf("📄 查询结果: %s\n", val)
			}

		case "":
			continue // 空回车，不做处理
		case "5":
			fmt.Println("🚀 开始疯狂写入 ", amount, " 条数据...")
			fmt.Println("⚠️ 注意：这可能需要几分钟，请耐心等待...")

			startTime := time.Now()

			for i := index; i <= index+amount; i++ {
				// 1. 生成 Key: "1", "2", ... "100000"
				key := strconv.Itoa(i)

				// 2. 生成 Value: 16位随机字符串
				value := key + "-" + key + "-" + key + "-" + key + "-" + key + "-" + key

				// 3. 发送请求
				// 这里不需要打印每一条日志，否则控制台会刷屏变慢
				ck.Put(key, value)

				// 4. 打印进度条 (每完成 1000 条打印一次)
				if i%10 == 0 {
					// \r 表示回到行首，实现覆盖打印效果
					fmt.Printf("\r📊 进度: %d / %d (耗时: %v)", i-index+1, amount, time.Since(startTime))
				}
			}
			fmt.Printf("\n✅ 压测完成！总耗时: %v\n", time.Since(startTime))
		case "6":
			startTime := time.Now()
			var wg sync.WaitGroup

			// 假设你要开 1000 个并发，每个人写 amount 条数据
			wg.Add(concurrency)

			// 🔥 进度监控计数器 (原子操作)
			var finishedOps int64
			totalOps := int64(concurrency * amount)

			// 单独开一个协程打印进度，防止 1000 个协程同时抢控制台
			go func() {
				for {
					completed := atomic.LoadInt64(&finishedOps)
					if completed >= totalOps {
						break
					}
					// 每 1 秒打印一次进度
					fmt.Printf("\r🚀 进度: %d / %d (TPS: %.0f) 耗时: %v",
						completed, totalOps,
						float64(completed)/time.Since(startTime).Seconds(),
						time.Since(startTime))
					time.Sleep(1 * time.Second)
				}
			}()

			for i := 0; i < concurrency; i++ {
				i1 := i // 闭包捕获
				ck := kvraft.MakeClerk(serverAddrs)
				go func(x int,ckk *kvraft.Clerk) {
					defer wg.Done()

					for j := index; j < amount + index; j++ {
						// 1. 生成唯一 Key (协程ID : 序列号)
						// 这样保证 key 不会冲突
						key := strconv.Itoa(x) + ":" + strconv.Itoa(j)

						// 2. 生成 Value (模拟一点长度)
						value := "val-" + key + "-" + time.Now().String()

						// 3. 发送请求
						ckk.Put(key, value)

						atomic.AddInt64(&finishedOps, 1)
					}
				}(i1,ck)
			}

			wg.Wait() // 等待所有任务完成

			// 最后打印一次最终结果
			fmt.Printf("\n✅ 全部完成！总耗时: %v\n", time.Since(startTime))
		case "7":
			var totalOps int64 = int64(amount) * concurrency
			fmt.Println("\n------------------------------------------------")
			fmt.Println("🚀 准备开始并发读取测试 (Get Benchmark)...")
			// 稍微停顿一下，让数据库喘口气（可选）
			time.Sleep(1 * time.Second)

			startReadTime := time.Now()
			var wgRead sync.WaitGroup
			wgRead.Add(concurrency) // 复用之前的并发数

			// 计数器
			var finishedReadOps int64  // 完成的读取请求数
			var successReadCount int64 // 成功读到数据（非空）的数量

			// ----------------------
			// 1. 启动监控协程 (每秒打印一次读取进度)
			// ----------------------
			go func() {
				for {
					completed := atomic.LoadInt64(&finishedReadOps)
					// 如果全部读完，退出监控
					if completed >= totalOps {
						break
					}

					// 计算瞬时 OPS
					elapsed := time.Since(startReadTime).Seconds()
					ops := float64(completed) / elapsed

					// 打印进度条
					fmt.Printf("\r🔍 读取进度: %d / %d (TPS: %.0f) 耗时: %.1fs",
						completed, totalOps, ops, elapsed)

					time.Sleep(1 * time.Second)
				}
			}()

			// ----------------------
			// 2. 启动并发读取 Worker
			// ----------------------
			for i := 0; i < concurrency; i++ {
				i := i // 闭包捕获
				go func() {
					defer wgRead.Done() // 🔥 别忘了 Done

					for j := index; j < amount + index; j++ {
						// ⚠️ 关键：Key 的生成逻辑必须和 Put 阶段完全一致！
						// 否则你查的就是不存在的 Key，肯定读不到
						key := strconv.Itoa(i) + ":" + strconv.Itoa(j)
						// key := strconv.Itoa(j)

						// 发起 RPC 请求
						val := ck.Get(key)

						// 统计
						atomic.AddInt64(&finishedReadOps, 1)

						// 只要返回的 val 不为空，就算命中 (Hit)
						if val != "" {
							atomic.AddInt64(&successReadCount, 1)
						}
					}
				}()
			}

			// 等待所有读取协程结束
			wgRead.Wait()
			readDuration := time.Since(startReadTime)

			// ----------------------
			// 3. 打印最终报告
			// ----------------------
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

		default:
			fmt.Printf("❌ 未知指令: [%s]，请输入 put, append, get 或 q\n", op)
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
		fmt.Println("❌ 请输入合法整数")
	}
}
