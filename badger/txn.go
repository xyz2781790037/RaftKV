package badger

import (
	"RaftKV/badger/skl"
	"RaftKV/badger/y"
	"RaftKV/tool"
	"bytes"
	"errors"
	"hash/crc64"
	"sort"
	"sync"
	"time"
)

var crcTable = crc64.MakeTable(crc64.ISO)
var (
	ErrDiscardedTxn = errors.New("Transaction is discarded")
	ErrReadOnlyTxn  = errors.New("Transaction is read-only")
	ErrConflict     = errors.New("Transaction Conflict. Please retry") // 新增错误
)

func MemHash(data []byte) uint64 {
	return crc64.Checksum(data, crcTable)
}

type oracle struct {
	nextTxnTs uint64 // 下一个可用的时间戳
	sync.Mutex
	commitLock    sync.Mutex
	committedTxns []committedTxn
}
type committedTxn struct {
	ts           uint64
	conflictKeys map[uint64]struct{}
}

func newOracle() *oracle {
	return &oracle{
		nextTxnTs:     1, // 从 1 开始
		committedTxns: make([]committedTxn, 0),
	}
}
func (o *oracle) readTs() uint64 {
	o.Lock()
	defer o.Unlock()
	return o.nextTxnTs - 1
}
func (o *oracle) hasConflict(txn *Txn) bool {
	if len(txn.reads) == 0 {
		return false
	}
	for _, committedTxn := range o.committedTxns {
		if committedTxn.ts <= txn.readTs {
			continue
		}
		for _, ro := range txn.reads {
			if _, has := committedTxn.conflictKeys[ro]; has {
				return true
			}
		}
	}
	return false
}
func (o *oracle) newCommitTs(txn *Txn) (uint64, error) {
	o.Lock()
	defer o.Unlock()

	if o.hasConflict(txn) {
		return 0, ErrConflict
	}

	ts := o.nextTxnTs
	o.nextTxnTs++
	o.committedTxns = append(o.committedTxns, committedTxn{
		ts:           ts,
		conflictKeys: txn.conflictKeys,
	})
	return ts, nil
}

type Txn struct {
	readTs       uint64 // 事务开启时的快照时间 (Snapshot TS)
	commitTs     uint64 // 提交时的时间戳
	db           *DB
	readsLock    sync.Mutex
	reads        []uint64            // 记录本事务读过的 Key 的指纹
	conflictKeys map[uint64]struct{} // 记录本事务写过的 Key 的指纹 (用 map 方便查找)

	pendingWrites map[string]*skl.Entry

	discarded bool
	update    bool // 是否是读写事务
}
type pendingWritesIterator struct {
	entries  []*skl.Entry
	nextIdx  int
	readTs   uint64
	reversed bool
}

func (pi *pendingWritesIterator) Next() {
	pi.nextIdx++
}

func (pi *pendingWritesIterator) Rewind() {
	pi.nextIdx = 0
}
func (pi *pendingWritesIterator) Seek(key []byte) {
	key = y.ParseKey(key)
	pi.nextIdx = sort.Search(len(pi.entries), func(idx int) bool {
		cmp := bytes.Compare(pi.entries[idx].Key, key)
		if !pi.reversed {
			return cmp >= 0
		}
		return cmp <= 0
	})
}
func (pi *pendingWritesIterator) Key() []byte {
	entry := pi.entries[pi.nextIdx]
	return y.KeyWithTs(entry.Key, pi.readTs)
}
func (pi *pendingWritesIterator) Value() y.ValueStruct {
	entry := pi.entries[pi.nextIdx]
	return y.ValueStruct{
		UserValue: entry.Value,
		Meta:      entry.Meta,
		UserMeta:  entry.UserMeta,
		ExpiresAt: entry.ExpiresAt,
	}
}
func (pi *pendingWritesIterator) Valid() bool {
	return pi.nextIdx < len(pi.entries)
}

func (pi *pendingWritesIterator) Close() error {
	return nil
}
func (txn *Txn) newPendingWritesIterator(reversed bool) *pendingWritesIterator {
	if !txn.update || len(txn.pendingWrites) == 0 {
		return nil
	}
	entries := make([]*skl.Entry, 0, len(txn.pendingWrites))
	for _, e := range txn.pendingWrites {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		cmp := bytes.Compare(entries[i].Key, entries[j].Key)
		if !reversed {
			return cmp < 0
		}
		return cmp > 0
	})
	return &pendingWritesIterator{
		readTs:   txn.readTs,
		entries:  entries,
		reversed: reversed,
	}
}
func (db *DB) NewTransaction(update bool) *Txn {
	txn := &Txn{
		readTs:        db.orc.readTs(),
		db:            db,
		pendingWrites: make(map[string]*skl.Entry),
		update:        update,
	}
	if update {
		txn.reads = make([]uint64, 0)
		txn.conflictKeys = make(map[uint64]struct{})
	}
	return txn
}
func (txn *Txn) modify(e *skl.Entry) error {
	if txn.discarded {
		return ErrDiscardedTxn
	}
	if !txn.update {
		return ErrReadOnlyTxn
	}
	if len(e.Key) == 0 {
		return ErrEmptyKey
	}

	txn.pendingWrites[string(e.Key)] = e

	// 记录写操作指纹
	fp := MemHash(e.Key)
	txn.conflictKeys[fp] = struct{}{}

	return nil
}
func (txn *Txn) Set(key, value []byte) error {
	e := skl.NewEntry(key, value)
	return txn.modify(e)
}
func (txn *Txn) Delete(key []byte) error {
	e := skl.NewEntry(key, nil)
	e.Meta = skl.BitDelete
	return txn.modify(e)
}

// func (txn *Txn) Get(key []byte) (*Item, error) {
// 	if len(key) == 0 {
// 		return nil, ErrEmptyKey
// 	} else if txn.discarded {
// 		return nil, ErrDiscardedTxn
// 	}

// 	if txn.update {
// 		if e, ok := txn.pendingWrites[string(key)]; ok && bytes.Equal(key, e.Key) {
// 			if isDeletedOrExpired(e.Meta, e.ExpiresAt) {
// 				return nil, ErrKeyNotFound
// 			}
// 			return &Item{
// 				key:       key,
// 				val:       e.Value, // 直接引用内存值
// 				meta:      e.Meta,
// 				userMeta:  0,
// 				version:   txn.readTs,
// 				expiresAt: e.ExpiresAt,
// 				txn:       txn,
// 			}, nil
// 		}
// 		txn.addReadKey(key)
// 	}
// 	opt := DefaultIteratorOptions
// 	opt.Prefix = key
// 	it := txn.NewIterator(opt)
// 	defer it.Close()

// 	it.Seek(key)
// 	if !it.Valid() {
// 		return nil, ErrKeyNotFound
// 	}

// 	itItem := it.Item()
// 	if !bytes.Equal(itItem.Key(), key) {
// 		return nil, ErrKeyNotFound
// 	}

// 	resItem := &Item{
// 		key:       y.SafeCopy(nil, itItem.Key()),
// 		txn:       txn,
// 		meta:      itItem.Meta(),
// 		userMeta:  itItem.UserMeta(),
// 		version:   itItem.Version(),
// 		expiresAt: itItem.ExpiresAt(),
// 	}
// 	if itItem.vptr != nil {
// 		resItem.vptr = y.SafeCopy(nil, itItem.vptr)
// 	} else {
// 		resItem.val = y.SafeCopy(nil, itItem.val)
// 	}

//		return resItem, nil
//	}
func (txn *Txn) Get(key []byte) (*Item, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	txn.readsLock.Lock()
	fp := MemHash(key)
	found := false
	for _, r := range txn.reads {
		if r == fp {
			found = true
			break
		}
	}
	if !found {
		txn.reads = append(txn.reads, fp)
	}
	txn.readsLock.Unlock()

	internalKey := y.KeyWithTs(key, txn.readTs)
	
	checkMem := func(sk *skl.SkipList) (*Item, error) {
		if sk == nil {
			return nil, ErrKeyNotFound
		}
		it := sk.NewIterator(false)
		defer it.Close()
		
		it.Seek(internalKey)
		for it.Valid() {
			userKey := y.ParseKey(it.Key())
			if bytes.Equal(userKey, key) {
				ts := y.ParseTs(it.Key())
				if ts <= txn.readTs {
					vs := it.Value()
					if vs.Meta&y.BitDelete > 0 {
						return nil, ErrKeyNotFound
					}
					res := &Item{
						key:       y.SafeCopy(nil, userKey),
						txn:       txn,
						meta:      vs.Meta,
						userMeta:  vs.UserMeta,
						version:   ts,
						expiresAt: vs.ExpiresAt,
					}
					if vs.Meta&y.BitValuePointer > 0 {
						res.vptr = y.SafeCopy(nil, vs.UserValue)
					} else {
						res.val = y.SafeCopy(nil, vs.UserValue)
					}
					return res, nil
				}
			} else {
				if bytes.Compare(userKey, key) > 0 {
					break
				}
			}
			it.Next()
		}
		return nil, ErrKeyNotFound
	}

	txn.db.mu.RLock()
	defer txn.db.mu.RUnlock()

	if item, err := checkMem(txn.db.skl); err == nil {
		return item, nil
	}
	if txn.db.immSkl != nil {
		if item, err := checkMem(txn.db.immSkl); err == nil {
			return item, nil
		}
	}

	// for _, tables := range txn.db.levelTables {
	// 	for i := 0; i < len(tables); i++ {
	// 		val, meta, ts, err := tables[i].Get(key, txn.readTs)
	// 		if err == nil {
	// 			if meta&y.BitDelete > 0 {
	// 				return nil, ErrKeyNotFound
	// 			}
	// 			return &Item{
	// 				key:     y.SafeCopy(nil, key),
	// 				txn:     txn,
	// 				meta:    meta,
	// 				val:     y.SafeCopy(nil, val),
	// 				version: ts,
	// 			}, nil
	// 		}
	// 	}
	// }
	for _, tables := range txn.db.levelTables {
		for i := 0; i < len(tables); i++ {
			val, meta, ts, err := tables[i].Get(key, txn.readTs)
			if err == nil {
				if meta&y.BitDelete > 0 {
					return nil, ErrKeyNotFound
				}
				
				var vs y.ValueStruct
				vs.Decode(val)

				res := &Item{
					key:       y.SafeCopy(nil, key),
					txn:       txn,
					meta:      vs.Meta,     // 使用解码后的 Meta
					userMeta:  vs.UserMeta,
					version:   ts,
					expiresAt: vs.ExpiresAt, // 恢复 TTL 属性
				}
				
				// 根据解码后的 Meta 正确分发数据或 Vlog 指针
				if vs.Meta&y.BitValuePointer > 0 {
					res.vptr = y.SafeCopy(nil, vs.UserValue)
				} else {
					res.val = y.SafeCopy(nil, vs.UserValue)
				}
				return res, nil
			}
		}
	}
	return nil, ErrKeyNotFound
}
func (txn *Txn) addReadKey(key []byte) {
	if txn.update {
		fp := MemHash(key)
		txn.readsLock.Lock()
		txn.reads = append(txn.reads, fp)
		txn.readsLock.Unlock()
	}
}
func (txn *Txn) commitPrecheck() error {
	if txn.discarded {
		return ErrDiscardedTxn
	}
	// ing
	return nil
}
func (txn *Txn) commitAndSend() (func() error, error) {
	orc := txn.db.orc
	orc.commitLock.Lock()
	defer orc.commitLock.Unlock()
	commitTs, err := orc.newCommitTs(txn)
	if err != nil {
		return nil, err
	}
	txn.commitTs = commitTs
	entries := make([]*skl.Entry, 0, len(txn.pendingWrites))
	for _, e := range txn.pendingWrites {
		encodedKey := y.KeyWithTs(e.Key, commitTs)
		newEntry := &skl.Entry{
			Key:       encodedKey,
			Value:     e.Value,
			Meta:      e.Meta,
			ExpiresAt: e.ExpiresAt,
		}
		entries = append(entries, newEntry)
	}
	writeErr := txn.db.BatchSet(entries)
	cb := func() error {
		//ing
		return writeErr
	}

	return cb, nil
}
func (txn *Txn) Commit() error {
	if len(txn.pendingWrites) == 0 {
		txn.Discard()
		return nil
	}
	if err := txn.commitPrecheck(); err != nil {
		return err
	}
	defer txn.Discard()
	txnCb, err := txn.commitAndSend()
	if err != nil {
		return err
	}
	return txnCb()
}

func (txn *Txn) Discard() {
	if txn.discarded {
		return
	}
	txn.discarded = true

	// 置空大对象，加速 GC 回收
	txn.pendingWrites = nil
	txn.reads = nil
	txn.conflictKeys = nil
}
func (txn *Txn) SetWithTTL(key, value []byte, ttl time.Duration) error {
	e := skl.NewEntry(key, value).WithTTL(ttl)
	tool.Log.Debug("🔍 [Trace-2] 事务暂存写入", "key", string(key), "expiresAt", e.ExpiresAt, "meta", e.Meta)
	txn.pendingWrites[string(key)] = e
	return nil
}
