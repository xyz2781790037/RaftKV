package badger

import (
	skl "RaftKV/badger/skiplist"
	"bytes"
	"encoding/gob"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)
const (
	MetaNormal = 0 // 正常数据
	MetaDelete = 1 // 删除标记
)
var (
	ErrDBClosed    = errors.New("database is closed")
	ErrKeyNotFound = errors.New("key not found")
	ErrEmptyKey    = errors.New("key cannot be empty")
)
type DB struct{
	mu sync.RWMutex
	skl *skl.SkipList    // 内存数据结构 (金字塔)
	wal *WAL         // 磁盘持久化 (账本)
	isClosed atomic.Uint32
}
func Open(dir string) (*DB, error){
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	walPath := filepath.Join(dir, "minibadger.wal")
	wal,err := OpenWAL(walPath)
	if err != nil{
		return nil,err
	}
	skl := skl.NewSkiplist()
	entries,err := wal.ReadWAL()
	if err != nil{
		return nil,err
	}
	for _, e := range entries {
		skl.Put(e)
	}
	db := &DB{
		skl: skl,
		wal: wal,
	}
	return db, nil
}
func (db *DB) checkClosed() error {
	if db.isClosed.Load() == 1 {
		return ErrDBClosed
	}
	return nil
}

func (db *DB) Put(key, value []byte) error{
	if err := db.checkClosed();err != nil{
		return err
	}
	if len(key) == 0{
		return ErrEmptyKey
	}
	e := skl.NewEntry(key,value)

	return db.BatchSet([]*skl.Entry{e})
}
func (db *DB) BatchSet(entries []*skl.Entry) error{
	if err := db.checkClosed(); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.wal.WriteBatch(entries);err != nil{
		return err
	}
	for _,e := range entries{
		db.skl.Put(e)
	}
	return nil
}
func (db *DB) Get(key []byte) ([]byte, error){
	if err := db.checkClosed(); err != nil {
		return nil, err
	}
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	db.mu.RLock()
	defer db.mu.RUnlock()
	e := db.skl.Get(key)
	if e == nil{
		return nil,ErrKeyNotFound
	}
	if e.IsDeleted(){
		return nil,ErrKeyNotFound
	}
	return e.Value,nil
}
func (db *DB) Delete(key []byte) error{
	if err := db.checkClosed(); err != nil {
		return err
	}
	if len(key) == 0 {
		return ErrEmptyKey
	}
	e := &skl.Entry{
		Key:   key,
		Value: nil,
		Meta:  MetaDelete,
	}
	return db.BatchSet([]*skl.Entry{e})
}
func (db *DB) Close() error {
	if !db.isClosed.CompareAndSwap(0, 1) {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.wal.Close()
}

// Sync 强制刷盘
func (db *DB) Sync() error {
	return db.wal.Sync()
}
func (db *DB) Backup() ([]byte, error) {
	if err := db.checkClosed(); err != nil {
		return nil, err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	var err error
	db.skl.Scan(func(e *skl.Entry) bool {
		if e.IsDeleted() {
			return true // continue
		}
		if encodeErr := enc.Encode(e.Key); encodeErr != nil {
			err = encodeErr
			return false
		}
		if encodeErr := enc.Encode(e.Value); encodeErr != nil {
			err = encodeErr
			return false
		}
		if encodeErr := enc.Encode(e.Meta); encodeErr != nil {
			err = encodeErr
			return false
		}
		return true
	})

	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
func (db *DB) LoadFrom(data []byte) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.skl = skl.NewSkiplist()

	if err := db.wal.Close(); err != nil {
		return err
	}
	if err := os.Truncate(db.wal.path, 0); err != nil {
		return err
	}
	newWal, err := OpenWAL(db.wal.path)
	if err != nil {
		return err
	}
	db.wal = newWal
	if len(data) == 0 {
		return nil
	}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	var batch []*skl.Entry

	for buf.Len() > 0 {
		var key, value []byte
		var meta byte

		// 必须按写入顺序读取
		if err := dec.Decode(&key); err != nil {
			// EOF 处理
			if err.Error() == "EOF" { break }
			return err
		}
		if err := dec.Decode(&value); err != nil {
			return err
		}
		if err := dec.Decode(&meta); err != nil {
			return err
		}

		entry := skl.NewEntry(key, value)
		entry.Meta = meta
		batch = append(batch, entry)

		if len(batch) >= 1000 {
			if err := db.wal.WriteBatch(batch); err != nil {
				return err
			}
			for _, e := range batch {
				db.skl.Put(e)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := db.wal.WriteBatch(batch); err != nil {
			return err
		}
		for _, e := range batch {
			db.skl.Put(e)
		}
	}

	return nil
}