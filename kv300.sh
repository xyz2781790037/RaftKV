
mkdir -p data/kv300

echo "正在启动 Replica Group 300..."

go run cmd/server/main.go -id 1 -gid 300 -port :8021 -peers "127.0.0.1:8021,127.0.0.1:8022,127.0.0.1:8023" -masters "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" -dir data/kv300 &
go run cmd/server/main.go -id 2 -gid 300 -port :8022 -peers "127.0.0.1:8021,127.0.0.1:8022,127.0.0.1:8023" -masters "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" -dir data/kv300 &
go run cmd/server/main.go -id 3 -gid 300 -port :8023 -peers "127.0.0.1:8021,127.0.0.1:8022,127.0.0.1:8023" -masters "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" -dir data/kv300 &

wait