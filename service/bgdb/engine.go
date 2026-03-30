package bgdb

import (
	"RaftKV/badger"
	"RaftKV/tool"
	"encoding/binary"
	"fmt"
	"sync"

	"time"
)

type KVEngine struct {
	db         *badger.DB
	isFlushing int32
	seqCache   sync.Map
}

func NewKVEngine(dir string) (*KVEngine, error) {
	opt := badger.DefaultOptions(dir)
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}
	return &KVEngine{db: db}, nil
}

func (k *KVEngine) ApplyCommand(op string, key, value []byte, ttl int64, clientId int64, seqId int64,expectedValue []byte) (uint64, error) {
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
		case "CAS":
			var currentVal []byte
			item, err := txn.Get(key)
			if err == nil {
				currentVal, _ = item.ValueCopy(nil)
			} else if err != badger.ErrKeyNotFound {
				return err
			}
			if string(currentVal) != string(expectedValue) {
				return fmt.Errorf("CAS_FAILED: expected [%s], got [%s]", string(expectedValue), string(currentVal))
			}
			if ttl > 0 {
				opErr = txn.SetWithTTL(key, value, time.Duration(ttl)*time.Second)
			} else {
				opErr = txn.Set(key, value)
			}
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
	currentTs := k.db.CurrentTs()
	return currentTs, nil
}

func (k *KVEngine) Get(key string) (string, error) {
	val, err := k.db.Get([]byte(key))
	if err != nil {
		tool.Log.Debug("[GET-DEBUG] KVEngine.Get 查无此键", "key", key, "err", err)
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
func (k *KVEngine) GetShardData(shard int, shardFn func(string) int) map[string] string {
	data := make(map[string]string)
	k.db.View(func(txn *badger.Txn)error{
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind();it.Valid();it.Next(){
			item := it.Item()
			keyBytes := item.Key()
			if len(keyBytes) >= 2 && keyBytes[0] == 'm' &&  keyBytes[1] == ':'{
				continue
			}
			keyStr := string(keyBytes)
			if shardFn(keyStr) == shard {
				valBytes, _ := item.ValueCopy(nil)
				data[keyStr] = string(valBytes)
			}
		}
		return nil
	})
	return data
}

func (k *KVEngine) GetSeqCache() map[int64]int64 {
	m := make(map[int64]int64)
	k.seqCache.Range(func(key, value interface{}) bool {
		m[key.(int64)] = value.(int64)
		return true
	})
	return m
}
func (k *KVEngine) GetSnapshot(ts uint64) ([]byte, error) {
	return k.db.BackupAt(ts)
}
func (db *KVEngine) CurrentTs() uint64 {
	return db.db.CurrentTs()
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

func (k *KVEngine) InsertShardData(data map[string]string, seqCache map[int64]int64) error {
	const batchSize = 1000

	var keys []string
	for key := range data {
		keys = append(keys, key)
	}

	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		chunkKeys := keys[i:end]

		err := k.db.Update(func(txn *badger.Txn) error {
			for _, key := range chunkKeys {
				if err := txn.Set([]byte(key), []byte(data[key])); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("分批插入 KV 數據失敗: %v", err)
		}
	}

	var cIds []int64
	for cid := range seqCache {
		cIds = append(cIds, cid)
	}

	for i := 0; i < len(cIds); i += batchSize {
		end := i + batchSize
		if end > len(cIds) {
			end = len(cIds)
		}
		chunkCIds := cIds[i:end]

		err := k.db.Update(func(txn *badger.Txn) error {
			for _, cid := range chunkCIds {
				metaKey := []byte(fmt.Sprintf("m:%d", cid))
				metaVal := make([]byte, 8)
				binary.BigEndian.PutUint64(metaVal, uint64(seqCache[cid]))
				if err := txn.Set(metaKey, metaVal); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("分批插入防重放緩存失敗: %v", err)
		}
	}
	for clientId, seqId := range seqCache {
		if val, ok := k.seqCache.Load(clientId); !ok || seqId > val.(int64) {
			k.seqCache.Store(clientId, seqId)
		}
	}

	return k.db.Sync()
}

func (k *KVEngine) DeleteShardData(shard int, shardFn func(string) int) error {
	var keysToDelete [][]byte

	// 1. 在唯讀事務中掃描並收集所有屬於該 Shard 的 Key
	err := k.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			keyBytes := item.Key()
			if len(keyBytes) >= 2 && keyBytes[0] == 'm' && keyBytes[1] == ':' {
				continue
			}
			if shardFn(string(keyBytes)) == shard {
				kb := make([]byte, len(keyBytes))
				copy(kb, keyBytes)
				keysToDelete = append(keysToDelete, kb)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("掃描待刪除數據失敗: %v", err)
	}

	const batchSize = 1000
	for i := 0; i < len(keysToDelete); i += batchSize {
		end := i + batchSize
		if end > len(keysToDelete) {
			end = len(keysToDelete)
		}
		chunk := keysToDelete[i:end]

		err := k.db.Update(func(txn *badger.Txn) error {
			for _, k := range chunk {
				if err := txn.Delete(k); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("分批刪除數據失敗: %v", err)
		}
	}

	return k.db.Sync()
}