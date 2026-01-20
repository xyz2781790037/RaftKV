package main

import (
	"RaftKV/server"
	"RaftKV/tool"
	"os"
)

func main() {
	opts,ok := server.ParseFlags()
	if !ok{
		os.Exit(1)
	}
	server,err := server.ServerStart(opts)
	if err != nil{
		tool.Log.Error("server start failed!","err",err)
		os.Exit(1)
	}
	server.WaitForExit()
}