package badger

import (
	"RaftKV/badger/skl"
	"RaftKV/badger/table"
	"RaftKV/badger/y"
	"RaftKV/tool"
	"bytes"
	"errors"
	"fmt"
	"io"
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
	opt         Options
	flushCond   *sync.Cond
}

func Open(opt Options) (*DB, error) {
	dir := opt.Dir
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
	vlogOpt := DefaultOptions(dir)
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
	db.flushCond = sync.NewCond(&db.mu)
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
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if db.isClosed.Load() == 1 {
					return
				}
				for {
					if err := db.RunValueLogGC(); err != nil {
						break
					}
				}
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
	var wroteToVlog bool
	for _, e := range entries {
		if len(e.Value) > ValueThreshold && e.Meta != skl.BitDelete {
			ptr, err := db.vlog.Write(e.Key, e.Value)
			if err != nil {
				return err
			}
			e.Value = EncodeValuePointer(ptr)
			e.Meta |= y.BitValuePointer
			wroteToVlog = true
		}
	}
	if wroteToVlog {
		if db.opt.SyncWrites {
			if err := db.vlog.Sync(); err != nil {
				return err
			}
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	for db.skl.SklSize() >= MemTableEntryLimit {
		if db.immSkl != nil {
			db.flushCond.Wait()
		} else {
			if err := db.wal.Close(); err != nil {
				return err
			}
			walPath := db.wal.path
			bakPath := walPath + ".bak"
			os.Rename(walPath, bakPath)

			newWal, err := OpenWAL(walPath)
			if err != nil {
				os.Rename(bakPath, walPath)
				return err
			}
			db.wal = newWal

			db.immSkl = db.skl
			db.skl = skl.NewSkiplist()

			go db.doFlushTask(db.immSkl, bakPath)
		}
	}

	if err := db.wal.WriteBatch(entries); err != nil {
		return err
	}
	for _, e := range entries {
		db.skl.Put(e)
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	tool.Log.Debug("4开始 Get 查询", "key", string(key))
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
	if db.vlog != nil {
		db.vlog.Close()
	}
	for _, tt := range db.levelTables {
		for _, t := range tt {
			t.DecrRef()
		}
	}
	return nil
}

func (db *DB) Sync() error {
	return db.wal.Sync()
}

func (db *DB) BackupAt(ts uint64) ([]byte, error) {
	if err := db.checkClosed(); err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(make([]byte, 0, 10*1024*1024))
	headerBuf := make([]byte, maxHeaderSize)
	err := db.View(func(txn *Txn) error {
		txn.readTs = ts

		it := txn.NewIterator(DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}
			cleanMeta := item.Meta() &^ y.BitValuePointer

			vs := y.ValueStruct{
				Meta:      cleanMeta,
				UserValue: val,
				ExpiresAt: item.ExpiresAt(),
			}

			encodedVal := make([]byte, vs.EncodedSize())
			vs.Encode(encodedVal)

			h := skl.EntryHeader{
				Meta:     cleanMeta,
				KeyLen:   uint32(len(item.Key())),
				ValueLen: uint32(len(encodedVal)),
			}
			n := h.Encode(headerBuf)
			buf.Write(headerBuf[:n])
			buf.Write(item.Key())
			buf.Write(encodedVal)
		}
		return nil
	})

	return buf.Bytes(), err
}

func (db *DB) LoadFrom(data []byte) error {
	if err := db.checkClosed(); err != nil {
		return err
	}

	db.mu.Lock()
	if err := db.manifest.RevertToSnapshot(); err != nil {
		db.mu.Unlock()
		return err
	}

	for _, tt := range db.levelTables {
		for _, t := range tt {
			t.MarkDelete()
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

		entry := skl.NewEntry(key, vs.UserValue)
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
		os.Rename(bakPath, walPath) // 失败回滚
		return err
	}
	db.wal = newWal

	db.immSkl = db.skl
	db.skl = skl.NewSkiplist()
	db.mu.Unlock()

	// 🚨 核心防死锁机制：确保退出前必定释放 immSkl 并唤醒前台
	defer func() {
		db.mu.Lock()
		db.immSkl = nil
		db.flushCond.Broadcast()
		db.mu.Unlock()
	}()

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
	newL0 := make([]*table.Table, 0, len(db.levelTables[0])+1)
	newL0 = append(newL0, t)
	newL0 = append(newL0, db.levelTables[0]...)
	db.levelTables[0] = newL0
	db.mu.Unlock()

	if err := db.manifest.AddTableToL0(uint32(fid)); err != nil {
		t.Close()
		return fmt.Errorf("failed to update manifest: %v", err)
	}
	_ = db.manifest.SetMaxTs(db.orc.readTs())
	os.Remove(bakPath)

	return nil
}

func (db *DB) doFlushTask(imm *skl.SkipList, bakWal string) {
	defer func() {
		db.mu.Lock()
		db.immSkl = nil
		db.flushCond.Broadcast()
		db.mu.Unlock()
	}()

	fid := db.manifest.AllocFileID()
	sstName := filepath.Join(db.manifest.dirPath, fmt.Sprintf("%06d.sst", fid))

	builder, err := table.NewBuilder(sstName)
	if err != nil {
		tool.Log.Error("Builder failed", "err", err)
		return
	}

	imm.Scan(func(e *skl.Entry) bool {
		vs := y.ValueStruct{
			Meta:      e.Meta,
			UserValue: e.Value,
			ExpiresAt: e.ExpiresAt,
		}
		builder.Add(e.Key, vs)
		return true
	})

	if err := builder.Finish(); err != nil {
		tool.Log.Error("Builder Finish failed", "err", err)
		return
	}

	t, err := table.OpenTable(sstName)
	if err != nil {
		tool.Log.Error("OpenTable failed", "err", err)
		return
	}

	db.mu.Lock()
	newL0 := make([]*table.Table, 0, len(db.levelTables[0])+1)
	newL0 = append(newL0, t)
	newL0 = append(newL0, db.levelTables[0]...)
	db.levelTables[0] = newL0
	db.mu.Unlock()

	if err := db.manifest.AddTableToL0(uint32(fid)); err != nil {
		t.Close()
		return
	}
	_ = db.manifest.SetMaxTs(db.orc.readTs())
	os.Remove(bakWal)
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

// RunValueLogGC 执行垃圾回收
func (db *DB) RunValueLogGC() error {
	if err := db.checkClosed(); err != nil {
		return err
	}

	fid, lf, err := db.vlog.pickLogToGC()
	if err != nil {
		return err
	}

	var wb []*skl.Entry
	var offset int64 = 0
	var reuseBuf []byte

	for {
		e, vp, nextOffset, buf, err := db.vlog.readEntryFrom(lf, offset, reuseBuf)
		reuseBuf = buf
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		vp.Fid = fid

		userKey := y.ParseKey(e.Key)
		readTs := y.ParseTs(e.Key)

		_ = db.View(func(txn *Txn) error {
			item, err := txn.Get(userKey)
			if err != nil {
				return nil
			}
			if item.version != readTs {
				return nil
			}
			if item.meta&y.BitDelete > 0 || (item.expiresAt > 0 && uint64(time.Now().Unix()) > item.expiresAt) {
				return nil
			}
			if item.meta&y.BitValuePointer == 0 {
				return nil
			}

			currentVp := DecodeValuePointer(item.vptr)
			if currentVp != nil && currentVp.Fid == fid && currentVp.Offset == vp.Offset {
				cleanMeta := e.Meta &^ y.BitValuePointer
				ne := &skl.Entry{
					Key:       userKey,
					Value:     e.Value,
					Meta:      cleanMeta,
					ExpiresAt: item.expiresAt,
				}
				wb = append(wb, ne)
			}
			return nil
		})

		if len(wb) >= 1000 {
			_ = db.Update(func(txn *Txn) error {
				for _, entry := range wb {
					txn.pendingWrites[string(entry.Key)] = entry
				}
				return nil
			})
			wb = wb[:0]
		}
		offset = nextOffset
	}

	if len(wb) > 0 {
		_ = db.Update(func(txn *Txn) error {
			for _, entry := range wb {
				txn.pendingWrites[string(entry.Key)] = entry
			}
			return nil
		})
	}
	return db.vlog.deleteLogFile(fid)
}
