package bgdb

import (
	"RaftKV/badger" // 替换为你实际的路径
	"RaftKV/badger/skl"
	"RaftKV/tool"
	"encoding/binary"
	"fmt"
	"sync/atomic"
)

type KVEngine struct {
	db         *badger.DB
	isFlushing int32
}

func NewKVEngine(dir string) (*KVEngine, error) {
	db, err := badger.Open(dir)
	if err != nil {
		return nil, err
	}
	return &KVEngine{db: db}, nil
}

func (k *KVEngine) ApplyCommand(op string, key, value []byte, clientId int64, seqId int64) error {
	var entries []*skl.Entry
	metaEntry := k.NewOperations(clientId, seqId)
	// 我们甚至可以给 metaEntry 专门定一个 Meta 码，虽然现在用 0 也可以
	entries = append(entries, metaEntry)
	switch op {
	case "Put":
		entries = append(entries, skl.NewEntry(key, value))
	case "Append":
		oldVal, err := k.db.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		newVal := append(oldVal, value...)
		entries = append(entries, skl.NewEntry(key, newVal))
	case "Delete":
		ent := skl.NewEntry(key, nil)
		ent.Meta = skl.BitDelete // 假设 1 是 MetaDelete
		entries = append(entries, ent)

	}
	if err := k.db.BatchSet(entries); err != nil {
		tool.Log.Warn("写入失败了")
		return err
	}
	if k.db.NeedFlush() {
		if atomic.CompareAndSwapInt32(&k.isFlushing, 0, 1) {
            go func() {
                defer atomic.StoreInt32(&k.isFlushing, 0)

                if err := k.db.Flush(); err != nil {
                    if err.Error() != "flush already in progress" {
                         tool.Log.Warn("BgDB Flush error", "err", err)
                    }
                } else {
                    tool.Log.Debug("✅ 自动 Flush 成功")
                }
            }()
        }
	}
	return nil
}

func (k *KVEngine) Get(key string) (string, error) {
	val, err := k.db.Get([]byte(key))
	return string(val), err
}

func (k *KVEngine) IsDuplicate(clientId int64, seqId int64) bool {
	metaKey := []byte(fmt.Sprintf("m:%d", clientId))
	val, err := k.db.Get(metaKey)
	if err != nil {
		return false
	}
	if len(val) < 8 {
		return false
	}
	lastSeq := int64(binary.BigEndian.Uint64(val))
	return seqId <= lastSeq
}
func (k *KVEngine) NewOperations(clientId int64, seqId int64) *skl.Entry {
	metaKey := []byte(fmt.Sprintf("m:%d", clientId))
	metaVal := make([]byte, 8)
	binary.BigEndian.PutUint64(metaVal, uint64(seqId))
	return skl.NewEntry(metaKey, metaVal)
}
func (k *KVEngine) GetSnapshot() ([]byte, error) {
	return k.db.Backup()
}

func (k *KVEngine) RestoreSnapshot(data []byte) error {
	return k.db.LoadFrom(data)
}

func (k *KVEngine) Close() error {
	return k.db.Close()
}

func (k *KVEngine) Sync() error {
	return k.db.Sync()
}
