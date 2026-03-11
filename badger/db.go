package badger

import (
	"RaftKV/badger/skl"
	"RaftKV/badger/table"
	"RaftKV/badger/y"
	"RaftKV/tool"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
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
	mu     sync.RWMutex
	skl    *skl.SkipList
	immSkl *skl.SkipList

	wal         *WAL
	vlog        *ValueLog
	levelTables [][]*table.Table
	isClosed    atomic.Uint32
	nextFileID  atomic.Uint32
	manifest    *Manifest
	orc         *oracle
	lc          *LevelsController
}

// func Open(dir string) (*DB, error) {
// 	if err := os.MkdirAll(dir, 0755); err != nil {
// 		return nil, err
// 	}
// 	manifest, err := OpenManifest(dir)
// 	if err != nil {
// 		return nil, err
// 	}

// 	nextFid, levelFIDs := manifest.GetState()

// 	walPath := filepath.Join(dir, "minibadger.wal")
// 	wal, err := OpenWAL(walPath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	vlogOpt := Options{ValueLogFileSize: MaxVLogFileSize}
// 	vlog, err := OpenValueLog(dir, vlogOpt)
// 	if err != nil {
// 		return nil, err
// 	}
// 	levels := make([][]*table.Table, MaxLevels)
// 	for levelNum, fids := range levelFIDs {
// 		for _, fid := range fids {
// 			sstName := filepath.Join(dir, fmt.Sprintf("%06d.sst", fid))
// 			t, err := table.OpenTable(sstName)
// 			if err != nil {
// 				return nil, err
// 			}
// 			levels[levelNum] = append(levels[levelNum], t)
// 		}
// 	}
// 	sl := skl.NewSkiplist()
// 	bakPath := filepath.Join(dir, "minibadger.wal.bak")
// 	if _, err := os.Stat(bakPath); err == nil {
// 		tool.Log.Info("Found .bak WAL, recovering data...", "path", bakPath)
// 		bakWal, err := OpenWAL(bakPath)
// 		if err == nil {
// 			entries, err := bakWal.ReadWAL()
// 			if err == nil {
// 				for _, e := range entries {
// 					sl.Put(e)
// 				}
// 			}
// 			bakWal.Close()
// 			// 恢复后不做删除，留给下次 Flush 处理，或者你可以选择现在 os.Remove(bakPath)
// 		}
// 	}
// 	entries, err := wal.ReadWAL()
// 	if err != nil {
// 		return nil, err
// 	}
// 	// for _, e := range entries {
// 	// 	sl.Put(e)
// 	// }
// 	var maxWalTs uint64 = 0
// 	for _, e := range entries {
// 		ts := y.ParseTs(e.Key)
// 		if ts > maxWalTs {
// 			maxWalTs = ts
// 		}
// 		sl.Put(e)
// 	}
// 	db := &DB{
// 		skl:         sl,
// 		immSkl:      nil, // 初始为空
// 		wal:         wal,
// 		vlog:        vlog,
// 		levelTables: levels,
// 		manifest:    manifest,
// 		orc:         newOracle(),
// 	}
// 	db.lc = NewLevelsController(db)
// 	maxTs := manifest.GetMaxTs()
// 	finalTs := maxTs
// 	if maxWalTs > finalTs {
// 		finalTs = maxWalTs
// 	}
// 	if finalTs > 0 {
// 		db.orc.nextTxnTs = finalTs + 1
// 		tool.Log.Info("成功从 Manifest 恢复时间戳", "nextTxnTs", db.orc.nextTxnTs)
// 	}
// 	db.nextFileID.Store(nextFid)
// 	go func() {
// 		for {
// 			if db.isClosed.Load() == 1 {
// 				return
// 			}
// 			cd := db.lc.PickCompactionTask()

// 			if cd != nil {
// 				if err := db.lc.RunCompaction(cd); err != nil {
// 					tool.Log.Error("后台合并任务执行失败", "err", err)
// 				}
// 			} else {
// 				time.Sleep(100 * time.Millisecond)
// 			}
// 		}
// 	}()
// 	return db, nil
// }
func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	manifest, err := OpenManifest(dir)
	if err != nil {
		return nil, err
	}

	nextFid, levelFIDs := manifest.GetState()

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
	levels := make([][]*table.Table, MaxLevels)
	for levelNum, fids := range levelFIDs {
		for _, fid := range fids {
			sstName := filepath.Join(dir, fmt.Sprintf("%06d.sst", fid))
			t, err := table.OpenTable(sstName)
			if err != nil {
				return nil, err
			}
			levels[levelNum] = append(levels[levelNum], t)
		}
	}

	// 🚨 核心修复 2：将 DB 实例的构建提前，使其在恢复期间就能调用 db.Flush()
	db := &DB{
		skl:         skl.NewSkiplist(),
		immSkl:      nil,
		wal:         wal,
		vlog:        vlog,
		levelTables: levels,
		manifest:    manifest,
		orc:         newOracle(),
	}
	db.lc = NewLevelsController(db)
	db.nextFileID.Store(nextFid)

	bakPath := filepath.Join(dir, "minibadger.wal.bak")
	if _, err := os.Stat(bakPath); err == nil {
		tool.Log.Info("Found .bak WAL, recovering data...", "path", bakPath)
		bakWal, err := OpenWAL(bakPath)
		if err == nil {
			entries, err := bakWal.ReadWAL()
			if err == nil {
				for _, e := range entries {
					db.skl.Put(e)
					// 如果恢复期内存满了，强制落盘
					if db.skl.SklSize() >= MemTableEntryLimit {
						_ = db.Flush()
					}
				}
			}
			bakWal.Close()
		}
	}

	entries, err := wal.ReadWAL()
	if err != nil {
		return nil, err
	}
	var maxWalTs uint64 = 0
	for _, e := range entries {
		ts := y.ParseTs(e.Key)
		if ts > maxWalTs {
			maxWalTs = ts
		}
		db.skl.Put(e)
		// 🚨 核心修复 3：重放 WAL 时实行水位控制，满则落盘！
		if db.skl.SklSize() >= MemTableEntryLimit {
			_ = db.Flush()
		}
	}

	maxTs := manifest.GetMaxTs()
	finalTs := maxTs
	if maxWalTs > finalTs {
		finalTs = maxWalTs
	}
	if finalTs > 0 {
		db.orc.nextTxnTs = finalTs + 1
		tool.Log.Info("成功从 Manifest 恢复时间戳", "nextTxnTs", db.orc.nextTxnTs)
	}

	go func() {
		for {
			if db.isClosed.Load() == 1 {
				return
			}
			cd := db.lc.PickCompactionTask()

			if cd != nil {
				if err := db.lc.RunCompaction(cd); err != nil {
					tool.Log.Error("后台合并任务执行失败", "err", err)
				}
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	return db, nil
}

func (db *DB) checkClosed() error {
	if db.isClosed.Load() == 1 {
		return ErrDBClosed
	}
	return nil
}
func (db *DB) View(fn func(txn *Txn) error) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	txn := db.NewTransaction(false)
	defer txn.Discard()

	return fn(txn)
}
func (db *DB) Update(fn func(txn *Txn) error) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	txn := db.NewTransaction(true)
	defer txn.Discard()

	if err := fn(txn); err != nil {
		return err
	}

	return txn.Commit()
}
func (db *DB) Put(key, value []byte) (uint64, error) {
	err := db.Update(func(txn *Txn) error {
		return txn.Set(key, value)
	})
	commitTs := db.orc.readTs()
	return commitTs, err
}
func (db *DB) Delete(key []byte) error {
	return db.Update(func(txn *Txn) error {
		return txn.Delete(key)
	})
}
func (db *DB) BatchSet(entries []*skl.Entry) error {
	if err := db.checkClosed(); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	tool.Log.Debug("[Trace-3] 引擎开始落盘", "entries_count", len(entries))
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
	for {
		db.mu.RLock()
		needFlush := db.skl.SklSize() >= MemTableEntryLimit
		hasImm := db.immSkl != nil
		db.mu.RUnlock()

		if needFlush && hasImm {
			time.Sleep(10 * time.Millisecond) // 等待后台 Flush 腾出空间
			continue
		}
		break
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.WriteBatch(entries); err != nil {
		tool.Log.Debug("BatchSet fail")
		return err
	}
	for _, e := range entries {
		db.skl.Put(e)
	}
	tool.Log.Debug("BatchSet 完成", "Count", len(entries), "CurrentSklSize", db.skl.SklSize())
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	tool.Log.Debug("🔍 [Trace-4] 开始 Get 查询", "key", string(key))
	var val []byte
	err := db.View(
		func(txn *Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			val = v
			return nil
		})
	return val, err
}
func (db *DB) GetAt(key []byte, ts uint64) ([]byte, error) {
	var val []byte
	err := db.View(
		func(txn *Txn) error {
			txn.readTs = ts
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			val = v
			return nil
		})
	return val, err
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

func (db *DB) Close() error {
	if !db.isClosed.CompareAndSwap(0, 1) {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	currentTs := db.orc.readTs()
	_ = db.manifest.SetMaxTs(currentTs)
	db.wal.Close()
	db.vlog.Close()
	for _, tt := range db.levelTables {
		for _, t := range tt {
			t.Close()
		}
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

	writeEntry := func(key, value []byte, meta byte, expiresAt uint64) bool {
		vs := y.ValueStruct{
			Meta:      meta, 
			UserValue: value, 
			ExpiresAt: expiresAt,
		}
		encodedVal := make([]byte, vs.EncodedSize())
		vs.Encode(encodedVal)
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
	for i := MaxLevels - 1; i >= 0; i-- {
		for j := len(db.levelTables[i]) - 1; j >= 0; j-- {
			if err := db.levelTables[i][j].Scan(writeEntry); err != nil {
				return nil, err
			}
		}
	}

	if db.immSkl != nil {
		db.immSkl.Scan(func(e *skl.Entry) bool {
			return writeEntry(e.Key, e.Value, e.Meta,e.ExpiresAt)
		})
	}

	db.skl.Scan(func(e *skl.Entry) bool {
		return writeEntry(e.Key, e.Value, e.Meta,e.ExpiresAt)
	})

	return buf.Bytes(), nil
}

// func (db *DB) LoadFrom(data []byte) error {
// 	if err := db.checkClosed(); err != nil {
// 		return err
// 	}
// 	db.mu.Lock()
// 	defer db.mu.Unlock()
// 	if err := db.manifest.RevertToSnapshot(); err != nil {
// 		return err
// 	}
// 	for _, tt := range db.levelTables {
// 		for _, t := range tt {
// 			t.Close()
// 			os.Remove(t.Fd().Name())
// 		}
// 	}
// 	db.levelTables = make([][]*table.Table, MaxLevels)
// 	db.nextFileID.Store(0)
// 	db.wal.Close()
// 	os.Truncate(db.wal.path, 0)
// 	newWal, err := OpenWAL(db.wal.path)
// 	if err != nil {
// 		return err
// 	}
// 	db.wal = newWal

// 	db.skl = skl.NewSkiplist()
// 	db.immSkl = nil

// 	if len(data) == 0 {
// 		return nil
// 	}

// 	offset := 0
// 	var batch []*skl.Entry

// 	for offset < len(data) {
// 		header, n := skl.DecodeHeader(data[offset:])
// 		if header == nil || n == 0 {
// 			return fmt.Errorf("corrupted snapshot")
// 		}
// 		offset += n

// 		key := make([]byte, header.KeyLen)
// 		copy(key, data[offset:offset+int(header.KeyLen)])
// 		offset += int(header.KeyLen)

// 		val := make([]byte, header.ValueLen)
// 		copy(val, data[offset:offset+int(header.ValueLen)])
// 		offset += int(header.ValueLen)

// 		entry := skl.NewEntry(key, val)
// 		entry.Meta = header.Meta
// 		batch = append(batch, entry)

//			if len(batch) >= 1000 {
//				if err := db.wal.WriteBatch(batch); err != nil {
//					return err
//				}
//				for _, e := range batch {
//					db.skl.Put(e)
//				}
//				batch = batch[:0]
//			}
//		}
//		if len(batch) > 0 {
//			db.wal.WriteBatch(batch)
//			for _, e := range batch {
//				db.skl.Put(e)
//			}
//		}
//		return nil
//	}
func (db *DB) LoadFrom(data []byte) error {
	if err := db.checkClosed(); err != nil {
		return err
	}

	// 1. 初始环境清理（只需在重置元数据时加锁）
	db.mu.Lock()
	if err := db.manifest.RevertToSnapshot(); err != nil {
		db.mu.Unlock()
		return err
	}
	for _, tt := range db.levelTables {
		for _, t := range tt {
			t.Close()
			os.Remove(t.Fd().Name())
		}
	}
	db.levelTables = make([][]*table.Table, MaxLevels)
	db.nextFileID.Store(0)
	db.wal.Close()
	os.Truncate(db.wal.path, 0)
	newWal, err := OpenWAL(db.wal.path)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	db.wal = newWal
	db.skl = skl.NewSkiplist()
	db.immSkl = nil
	db.mu.Unlock()

	if len(data) == 0 {
		return nil
	}

	offset := 0
	var batch []*skl.Entry

	// 2. 批量回放与写入（按批次加锁）
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
		var vs y.ValueStruct
		vs.Decode(val)

		entry := skl.NewEntry(key,vs.UserValue)
		entry.Meta = header.Meta
		entry.ExpiresAt = vs.ExpiresAt 
		batch = append(batch, entry)

		if len(batch) >= 1000 {
			db.mu.Lock() // 仅在写入内存表和 WAL 时加锁
			if err := db.wal.WriteBatch(batch); err != nil {
				db.mu.Unlock()
				return err
			}
			for _, e := range batch {
				db.skl.Put(e)
			}
			needFlush := db.skl.SklSize() > MemTableEntryLimit
			db.mu.Unlock()

			// 🚨 核心修复：如果内存表满了，由于当前没有持有大锁，可以安全地触发同步 Flush 防止 OOM
			if needFlush {
				if err := db.Flush(); err != nil && err.Error() != "flush already in progress" {
					tool.Log.Error("LoadFrom 期间 Flush 失败", "err", err)
				}
			}
			batch = batch[:0]
		}
	}

	// 3. 处理尾部残余数据
	if len(batch) > 0 {
		db.mu.Lock()
		db.wal.WriteBatch(batch)
		for _, e := range batch {
			db.skl.Put(e)
		}
		db.mu.Unlock()
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

	if err := db.wal.Close(); err != nil {
		db.mu.Unlock()
		return err
	}

	walPath := db.wal.path
	bakPath := walPath + ".bak"

	if err := os.Rename(walPath, bakPath); err != nil {
		db.mu.Unlock()
		return err
	}

	newWal, err := OpenWAL(walPath)
	if err != nil {
		db.mu.Unlock()
		os.Rename(bakPath, walPath) // 失敗回滾
		return err
	}
	db.wal = newWal

	db.immSkl = db.skl
	db.skl = skl.NewSkiplist()
	db.mu.Unlock()
	fid := db.manifest.AllocFileID()
	sstName := filepath.Join(db.manifest.dirPath, fmt.Sprintf("%06d.sst", fid))

	builder, err := table.NewBuilder(sstName)
	if err != nil {
		return err
	}

	db.immSkl.Scan(func(e *skl.Entry) bool {
		vs := y.ValueStruct{
			Meta:      e.Meta,
			UserValue: e.Value,
			ExpiresAt: e.ExpiresAt,
		}
		builder.Add(e.Key, vs)
		return true
	})

	if err := builder.Finish(); err != nil {
		return err
	}

	t, err := table.OpenTable(sstName)
	if err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	newL0 := make([]*table.Table, 0, len(db.levelTables[0])+1)
	newL0 = append(newL0, t)
	newL0 = append(newL0, db.levelTables[0]...)
	db.levelTables[0] = newL0
	db.immSkl = nil
	if err := db.manifest.AddTableToL0(uint32(fid)); err != nil {
		t.Close()
		return fmt.Errorf("failed to update manifest: %v", err)
	}
	_ = db.manifest.SetMaxTs(db.orc.readTs())

	os.Remove(bakPath)
	return nil
}

const MemTableEntryLimit = 40000

func (db *DB) NeedFlush() bool {
	return db.skl.SklSize() > MemTableEntryLimit
}
func (db *DB) NewIterators(reverse bool) []y.Iterator {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iters := make([]y.Iterator, 0)
	iters = append(iters, db.skl.NewIterator(reverse))
	if db.immSkl != nil {
		iters = append(iters, db.immSkl.NewIterator(reverse))
	}
	opt := table.NONE
	if reverse {
		opt |= table.REVERSED
	}
	for _, tt := range db.levelTables {
		for _, t := range tt {
			iters = append(iters, t.NewIterator(opt))
		}
	}
	return iters
}
func (db *DB) CurrentTs() uint64 {
	return db.orc.readTs()
}
