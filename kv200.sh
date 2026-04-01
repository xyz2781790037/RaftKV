
mkdir -p data/kv200

echo "正在启动 Replica Group 200..."

go run cmd/server/main.go -id 1 -gid 200 -port :8011 -peers "127.0.0.1:8011,127.0.0.1:8012,127.0.0.1:8013" -masters "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" -dir data/kv200 &
go run cmd/server/main.go -id 2 -gid 200 -port :8012 -peers "127.0.0.1:8011,127.0.0.1:8012,127.0.0.1:8013" -masters "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" -dir data/kv200 &
go run cmd/server/main.go -id 3 -gid 200 -port :8013 -peers "127.0.0.1:8011,127.0.0.1:8012,127.0.0.1:8013" -masters "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003" -dir data/kv200 &

wait