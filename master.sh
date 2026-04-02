
mkdir -p data/master

echo "正在启动 ShardMaster (大脑集群)..."

go run cmd/control/main.go -id 1 -port :9001 -peers "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" -dir data/master &
go run cmd/control/main.go -id 2 -port :9002 -peers "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" -dir data/master &
go run cmd/control/main.go -id 3 -port :9003 -peers "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" -dir data/master &

wait