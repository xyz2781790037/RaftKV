package bgdb

import (
	"RaftKV/badger" // 替换为你实际的路径
	"RaftKV/badger/skl"
	"RaftKV/tool"
	"encoding/binary"
	"fmt"
)

type KVEngine struct {
	db *badger.DB
}

func NewKVEngine(dir string) (*KVEngine, error) {
	db, err := badger.Open(dir)
	if err != nil {
		return nil, err
	}
	return &KVEngine{db: db}, nil
}

func (k *KVEngine) ApplyCommand(op string, key, value []byte,clientId int64, seqId int64) error{
	var entries []*skl.Entry
	metaEntry := k.NewOperations(clientId,seqId)
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
		newVal := append(oldVal,value...)
		entries = append(entries, skl.NewEntry(key, newVal))
	case "Delete":
		ent := skl.NewEntry(key, nil)
		ent.Meta = skl.BitDelete // 假设 1 是 MetaDelete
		entries = append(entries, ent)
	
	}
	if err := k.db.BatchSet(entries); err != nil {
		return err
	}
	if k.db.NeedFlush() {
		// 必须异步执行！否则会阻塞 Raft 的 Apply 循环，导致吞吐量暴跌
		go func() {
			// Flush 内部有锁，并发安全
			if err := k.db.Flush(); err != nil {
				tool.Log.Info("BgDB Flush failed: %v", err)
			}
		}()
	}
	return nil
}

func (k *KVEngine) Get(key string) (string, error) {
	val, err := k.db.Get([]byte(key))
	if err == badger.ErrKeyNotFound {
		return "", nil
	}
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
func (k *KVEngine)NewOperations(clientId int64, seqId int64)*skl.Entry{
	metaKey := []byte(fmt.Sprintf("m:%d", clientId))
	metaVal := make([]byte, 8)
	binary.BigEndian.PutUint64(metaVal, uint64(seqId))
	return skl.NewEntry(metaKey,metaVal)
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
