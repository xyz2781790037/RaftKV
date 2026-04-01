
mkdir -p data/kv100

echo "正在启动 Replica Group 100..."

go run cmd/server/main.go -id 1 -gid 100 -port :8001 -peers "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003" -masters "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" -dir data/kv100 &
go run cmd/server/main.go -id 2 -gid 100 -port :8002 -peers "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003" -masters "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" -dir data/kv100 &
go run cmd/server/main.go -id 3 -gid 100 -port :8003 -peers "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003" -masters "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" -dir data/kv100 &

wait