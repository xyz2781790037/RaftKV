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
	"time"
)
const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

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
		fmt.Println(" 5 [auto put] 自动")
		fmt.Print("👉 请输入指令 > ")
		var amount int
		// 读取指令
		op, _ := reader.ReadString('\n')
		op = strings.TrimSpace(strings.ToLower(op)) // 去除回车和空格

		if op == "q" || op == "quit" || op == "exit" {
			fmt.Println("👋 正在退出客户端，再见！")
			break
		}else if op == "5"{
			fmt.Print("输入测试数量 > ")
			fmt.Scanf("%d",&amount)
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
            fmt.Println("🚀 开始疯狂写入 ",amount," 条数据...")
            fmt.Println("⚠️ 注意：这可能需要几分钟，请耐心等待...")
            
            startTime := time.Now()

            for i := 1; i <= amount; i++ {
                // 1. 生成 Key: "1", "2", ... "100000"
                key := strconv.Itoa(i)

                // 2. 生成 Value: 16位随机字符串
                value := randStr(16) 

                // 3. 发送请求
                // 这里不需要打印每一条日志，否则控制台会刷屏变慢
                ck.Put(key, value)

                // 4. 打印进度条 (每完成 1000 条打印一次)
                if i%1000 == 0 {
                    // \r 表示回到行首，实现覆盖打印效果
                    fmt.Printf("\r📊 进度: %d / 100000 (耗时: %v)", i, time.Since(startTime))
                }
            }
            fmt.Printf("\n✅ 压测完成！总耗时: %v\n", time.Since(startTime))

		default:
			fmt.Printf("❌ 未知指令: [%s]，请输入 put, append, get 或 q\n", op)
		}
	}
}
