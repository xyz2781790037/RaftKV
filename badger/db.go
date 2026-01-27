package badger

import (
	"RaftKV/badger/skl"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	MetaNormal     = 0
	MetaDelete     = 1
	ValueThreshold = 1024
)

var (
	ErrDBClosed    = errors.New("database is closed")
	ErrKeyNotFound = errors.New("key not found")
	ErrEmptyKey    = errors.New("key cannot be empty")
)

type DB struct {
	mu  sync.RWMutex
	skl *skl.SkipList
	immSkl *skl.SkipList

	wal        *WAL
	vlog       *ValueLog
	tables     []*Table
	isClosed   atomic.Uint32
	nextFileID atomic.Uint32
}

func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	walPath := filepath.Join(dir, "minibadger.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, err
	}
	vlogOpt := Options{ValueLogFileSize: MaxVLogFileSize}
	vlog, err := OpenValueLog(dir, vlogOpt)
	if err != nil {
		return nil, err
	}

	var tables []*Table
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var sstFiles []string
	var maxFileID uint64 = 0

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".sst" {
			filename := file.Name()
			idStr := strings.TrimSuffix(filename, ".sst")
			id, _ := strconv.ParseUint(idStr, 10, 32)
			if id > maxFileID {
				maxFileID = id
			}
			sstFiles = append(sstFiles, filepath.Join(dir, filename))
		}
	}

	sort.Strings(sstFiles)

	for _, sstPath := range sstFiles {
		t, err := OpenTable(sstPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open sst %s: %v", sstPath, err)
		}
		tables = append([]*Table{t}, tables...)
	}

	sl := skl.NewSkiplist()
	entries, err := wal.ReadWAL()
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		sl.Put(e)
	}

	db := &DB{
		skl:    sl,
		immSkl: nil, // 初始为空
		wal:    wal,
		vlog:   vlog,
		tables: tables,
	}

	if len(sstFiles) > 0 {
		db.nextFileID.Store(uint32(maxFileID) + 1)
	} else {
		db.nextFileID.Store(0)
	}

	return db, nil
}

func (db *DB) checkClosed() error {
	if db.isClosed.Load() == 1 {
		return ErrDBClosed
	}
	return nil
}

func (db *DB) Put(key, value []byte) error {
	e := skl.NewEntry(key, value)
	return db.BatchSet([]*skl.Entry{e})
}

func (db *DB) BatchSet(entries []*skl.Entry) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	for _, e := range entries {
		if len(e.Value) > ValueThreshold && e.Meta != skl.BitDelete {
			ptr, err := db.vlog.Write(e.Key, e.Value)
			if err != nil {
				return err
			}
			e.Value = EncodeValuePointer(ptr)
			e.Meta |= skl.BitValuePointer
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.WriteBatch(entries); err != nil {
		return err
	}
	for _, e := range entries {
		db.skl.Put(e)
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if err := db.checkClosed(); err != nil {
		return nil, err
	}
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	db.mu.RLock()
	if e := db.skl.Get(key); e != nil {
		val, err := db.valueStructToBytes(e.Value, e.Meta)
		db.mu.RUnlock()
		return val, err
	}

	if db.immSkl != nil {
		if e := db.immSkl.Get(key); e != nil {
			val, err := db.valueStructToBytes(e.Value, e.Meta)
			db.mu.RUnlock()
			return val, err
		}
	}
	currentTables := db.tables
	db.mu.RUnlock()

	for _, table := range currentTables {
		val, meta, err := table.Get(key)
		if err == nil {
			return db.valueStructToBytes(val, meta)
		}
	}
	return nil, ErrKeyNotFound
}

func (db *DB) valueStructToBytes(val []byte, meta byte) ([]byte, error) {
	if meta&skl.BitDelete != 0 {
		return nil, ErrKeyNotFound
	}
	if meta&skl.BitValuePointer != 0 {
		valPtr := DecodeValuePointer(val)
		return db.vlog.Read(valPtr)
	}
	return val, nil
}

func (db *DB) Delete(key []byte) error {
	e := &skl.Entry{Key: key, Value: nil, Meta: skl.BitDelete}
	return db.BatchSet([]*skl.Entry{e})
}

func (db *DB) Close() error {
	if !db.isClosed.CompareAndSwap(0, 1) {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.wal.Close()
	db.vlog.Close()
	for _, t := range db.tables {
		t.Close()
	}
	return nil
}

func (db *DB) Sync() error {
	return db.wal.Sync()
}

func (db *DB) Backup() ([]byte, error) {
	if err := db.checkClosed(); err != nil {
		return nil, err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()

	buf := bytes.NewBuffer(make([]byte, 0, 4*db.skl.SklSize()))
	headerBuf := make([]byte, maxHeaderSize)

	writeEntry := func(key, value []byte, meta byte) bool {
		h := skl.EntryHeader{
			Meta:     meta,
			KeyLen:   uint32(len(key)),
			ValueLen: uint32(len(value)),
		}
		n := h.Encode(headerBuf)
		buf.Write(headerBuf[:n])
		buf.Write(key)
		buf.Write(value)
		return true
	}

	for i := len(db.tables) - 1; i >= 0; i-- {
		if err := db.tables[i].Scan(writeEntry); err != nil {
			return nil, err
		}
	}

	if db.immSkl != nil {
		db.immSkl.Scan(func(e *skl.Entry) bool {
			return writeEntry(e.Key, e.Value, e.Meta)
		})
	}

	db.skl.Scan(func(e *skl.Entry) bool {
		return writeEntry(e.Key, e.Value, e.Meta)
	})

	return buf.Bytes(), nil
}

func (db *DB) LoadFrom(data []byte) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, t := range db.tables {
		t.Close()
		os.Remove(t.fd.Name())
	}
	db.tables = nil
	db.nextFileID.Store(0)

	db.wal.Close()
	os.Truncate(db.wal.path, 0)
	newWal, err := OpenWAL(db.wal.path)
	if err != nil {
		return err
	}
	db.wal = newWal

	db.skl = skl.NewSkiplist()
	db.immSkl = nil

	if len(data) == 0 {
		return nil
	}

	offset := 0
	var batch []*skl.Entry

	for offset < len(data) {
		header, n := skl.DecodeHeader(data[offset:])
		if header == nil || n == 0 {
			return fmt.Errorf("corrupted snapshot")
		}
		offset += n

		key := make([]byte, header.KeyLen)
		copy(key, data[offset:offset+int(header.KeyLen)])
		offset += int(header.KeyLen)

		val := make([]byte, header.ValueLen)
		copy(val, data[offset:offset+int(header.ValueLen)])
		offset += int(header.ValueLen)

		entry := skl.NewEntry(key, val)
		entry.Meta = header.Meta
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
		db.wal.WriteBatch(batch)
		for _, e := range batch {
			db.skl.Put(e)
		}
	}
	return nil
}

func (db *DB) Flush() error {
	db.mu.Lock()
	if db.skl.SklSize() == 0 {
		db.mu.Unlock()
		return nil
	}

	if db.immSkl != nil {
		db.mu.Unlock()
		return fmt.Errorf("flush already in progress")
	}

	db.immSkl = db.skl
	db.skl = skl.NewSkiplist()

	fid := db.nextFileID.Add(1) - 1
	db.mu.Unlock()

	sstName := filepath.Join(db.vlog.dirPath, fmt.Sprintf("%06d.sst", fid))
	builder, err := NewBuilder(sstName)
	if err != nil {
		return err
	}

	db.immSkl.Scan(func(e *skl.Entry) bool {
		builder.Add(e.Key, e.Value, e.Meta)
		return true
	})

	if err := builder.Finish(); err != nil {
		return err
	}

	t, err := OpenTable(sstName)
	if err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.tables = append([]*Table{t}, db.tables...)
	db.immSkl = nil

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

	return nil
}
const MemTableEntryLimit = 64 * 1024 * 1024
func (db *DB) NeedFlush() bool {
	return db.skl.SklSize() > MemTableEntryLimit
}
