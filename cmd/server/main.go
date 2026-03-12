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
