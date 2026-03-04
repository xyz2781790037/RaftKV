package bgdb

import (
	"RaftKV/badger"
	"RaftKV/tool"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type KVEngine struct {
	db         *badger.DB
	isFlushing int32
	seqCache   sync.Map
}

func NewKVEngine(dir string) (*KVEngine, error) {
	db, err := badger.Open(dir)
	if err != nil {
		return nil, err
	}
	return &KVEngine{db: db}, nil
}

func (k *KVEngine) ApplyCommand(op string, key, value []byte, ttl int64, clientId int64, seqId int64) (uint64, error) {
	err := k.db.Update(func(txn *badger.Txn) error {
		metaKey := []byte(fmt.Sprintf("m:%d", clientId))
		metaVal := make([]byte, 8)
		binary.BigEndian.PutUint64(metaVal, uint64(seqId))

		if err := txn.Set(metaKey, metaVal); err != nil {
			return err
		}

		var opErr error
		switch op {
		case "Put":
			if ttl > 0 {
				opErr = txn.SetWithTTL(key, value, time.Duration(ttl)*time.Second)
			} else {
				opErr = txn.Set(key, value)
			}
		case "Append":
			var oldVal []byte
			item, err := txn.Get(key)
			if err == nil {
				oldVal, _ = item.ValueCopy(nil)
			} else if err != badger.ErrKeyNotFound {
				return err
			}
			newVal := append(oldVal, value...)
			opErr = txn.Set(key, newVal)
		case "Delete":
			opErr = txn.Delete(key)
		}

		if opErr != nil {
			return opErr
		}
		return nil
	})

	if err != nil {
		tool.Log.Warn("write failed", "err", err)
		return 0, err
	} else {
		k.seqCache.Store(clientId, seqId)
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
					tool.Log.Debug("Auto Flush succeed")
				}
			}()
		}
	}
	currentTs := k.db.CurrentTs()
	return currentTs, nil
}

func (k *KVEngine) Get(key string) (string, error) {
	val, err := k.db.Get([]byte(key))
	if err != nil {
		tool.Log.Warn("[GET-DEBUG] KVEngine.Get 查无此键", "key", key, "err", err)
	}
	return string(val), err
}
func (k *KVEngine) GetAt(key string, ts uint64) (string, error) {
	val, err := k.db.GetAt([]byte(key), ts)
	return string(val), err
}
func (k *KVEngine) IsDuplicate(clientId int64, seqId int64) bool {
	// 优先查内存缓存，O(1) 复杂度，无锁/无磁盘 IO
	if val, ok := k.seqCache.Load(clientId); ok {
		lastSeq := val.(int64)
		return seqId <= lastSeq
	}

	// 内存没有命中（通常只有重启后第一次请求才会走到这里），再去查底层的 DB
	metaKey := []byte(fmt.Sprintf("m:%d", clientId))
	var lastSeq int64
	err := k.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey)
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if len(val) >= 8 {
			lastSeq = int64(binary.BigEndian.Uint64(val))
		}
		return nil
	})

	if err == nil {
		k.seqCache.Store(clientId, lastSeq) // 回填缓存
	}
	return seqId <= lastSeq
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
