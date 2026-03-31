package main

import (
	"RaftKV/proto/shardpb"
	"RaftKV/service/shardmaster"
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	mastersRaw := flag.String("masters", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "ShardMaster 集群地址列表，用逗号分隔")
	flag.Parse()

	masterAddrs := strings.Split(*mastersRaw, ",")
	fmt.Println("正在连接 ShardMaster 控制中心:", masterAddrs, "...")

	ck := shardmaster.MakeClerk(masterAddrs)
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("\n=== ShardMaster 集群管理控制台 ===")
		fmt.Println("1 [Query] 查询全局路由表配置")
		fmt.Println("2 [Leave] 剔除机器组 (引发全网重新平衡)")
		fmt.Println("3 [Move]  将指定分片强制分配给指定机器组")
		fmt.Println("4 [Join]  新增机器组 (上线新集群)")
		fmt.Println("q [退出]")
		fmt.Print("请选择操作 > ")

		op, _ := reader.ReadString('\n')
		op = strings.TrimSpace(op)

		if op == "q" || op == "quit" || op == "exit" {
			fmt.Println("正在退出客户端，再见！")
			break
		}

		switch op {
		case "1":
			fmt.Print("输入要查询的配置版本号 (输入 -1 查询最新版本) > ")
			numStr, _ := reader.ReadString('\n')
			num, err := strconv.ParseInt(strings.TrimSpace(numStr), 10, 64)
			if err != nil {
				fmt.Println("输入错误，请输入合法数字。")
				continue
			}
			cfg := ck.Query(num)

			fmt.Printf("\n当前配置版本: %d\n", cfg.Num)
			fmt.Printf("分片映射 (Shard -> GroupID): %v\n", cfg.Shards)
			for gid, servers := range cfg.Groups {
				fmt.Printf("💻 机器组 %d 物理节点: %v\n", gid, servers.Servers)
			}

		case "2":
			fmt.Print("输入要剔除的 GroupID (如需同时剔除多个用逗号分割) > ")
			gidsStr, _ := reader.ReadString('\n')
			var gids []int64
			for _, s := range strings.Split(strings.TrimSpace(gidsStr), ",") {
				if id, err := strconv.ParseInt(s, 10, 64); err == nil {
					gids = append(gids, id)
				}
			}
			if len(gids) > 0 {
				ck.Leave(gids)
				fmt.Printf("Leave 指令已发送: %v。请用 Query 查看分片是否已重新分配。\n", gids)
			} else {
				fmt.Println("无效的 GroupID。")
			}

		case "3":
			fmt.Print("输入参数 [分片ID,目标GroupID] (例如 3,200) > ")
			argsStr, _ := reader.ReadString('\n')
			parts := strings.Split(strings.TrimSpace(argsStr), ",")
			if len(parts) == 2 {
				shard, err1 := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
				gid, err2 := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
				if err1 == nil && err2 == nil {
					ck.Move(shard, gid)
					fmt.Printf("Move 指令已发送: Shard %d 已强制指派给 Group %d。\n", shard, gid)
				} else {
					fmt.Println("解析数字失败，请检查输入格式。")
				}
			} else {
				fmt.Println("输入格式错误，需要用逗号分隔。")
			}
		case "4":
			fmt.Print("输入要新增的 GroupID (100 / 200 / 300) > ")
			gidStr, _ := reader.ReadString('\n')
			gidStr = strings.TrimSpace(gidStr)

			gid, err := strconv.ParseInt(gidStr, 10, 64)
			if err != nil {
				fmt.Println("GroupID 必须是数字。")
				continue
			}

			predefined := map[int64][]string{
				100: {"127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"},
				200: {"127.0.0.1:8011", "127.0.0.1:8012", "127.0.0.1:8013"},
				300: {"127.0.0.1:8021", "127.0.0.1:8022", "127.0.0.1:8023"},
			}

			servers, ok := predefined[gid]
			if !ok {
				fmt.Println("不支持的 GroupID，仅支持 100 / 200 / 300")
				continue
			}

			joinData := make(map[int64]*shardpb.GroupServers)
			joinData[gid] = &shardpb.GroupServers{
				Servers: servers,
			}

			ck.Join(joinData)

			fmt.Printf("Join 完成: Group %d -> %v\n", gid, servers)
		default:
			fmt.Println("未知指令。")
		}
	}
}
