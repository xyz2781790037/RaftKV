package badger

import (
	"RaftKV/badger/table"
	"RaftKV/badger/y"
	"RaftKV/tool"
	"bytes"
	"fmt"
	"time"
)

type Item struct {
	key       []byte
	val       []byte
	vptr      []byte
	meta      byte
	userMeta  byte
	expiresAt uint64
	version   uint64
	txn       *Txn
}

func (item *Item) String() {
	tool.Log.Info("item", "key", item.Key(), "version", item.Version(), "meta", item.meta)
}
func (item *Item) Key() []byte {
	return item.key
}
func (item *Item) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, item.key)
}

func (item *Item) Version() uint64 {
	return item.version
}
func (item *Item) ExpiresAt() uint64 {
	return item.expiresAt
}
func (item *Item) UserMeta() byte {
	return item.userMeta
}
func (item *Item) Meta() byte {
	return item.meta
}

func (item *Item) Value() ([]byte, error) {
	return item.yieldItemValue()
}
func (item *Item) ValueCopy(dst []byte) ([]byte, error) {
	val, err := item.yieldItemValue()
	if err != nil {
		return nil, err
	}
	return y.SafeCopy(dst, val), nil
}
func (item *Item) yieldItemValue() ([]byte, error) {
	if !item.hasValue() {
		return nil, nil
	}
	if (item.meta & y.BitValuePointer) == 0 {
		return item.val, nil
	}
	if item.txn == nil || item.txn.db == nil {
		return nil, fmt.Errorf("txn or db is nil, cannot read value log")
	}
	var vp ValuePointer
	vpDecode := DecodeValuePointer(item.vptr)
	vp = *vpDecode

	return item.txn.db.vlog.Read(&vp)
}
func (item *Item) hasValue() bool {
	if item.meta == 0 && item.vptr == nil && item.val == nil {
		return false
	}
	return true
}
func (item *Item) KeySize() int64 {
	return int64(len(item.key))
}
func (item *Item) ValueSize() int64 {
	if !item.hasValue() {
		return 0
	}
	if (item.meta & y.BitValuePointer) == 0 {
		return int64(len(item.val))
	}
	vp := DecodeValuePointer(item.vptr)
	return int64(vp.Len)
}
func (item *Item) EstimatedSize() int64 {
	if !item.hasValue() {
		return 0
	}
	if (item.meta & y.BitValuePointer) == 0 {
		return int64(len(item.key) + len(item.vptr))
	}
	vp := DecodeValuePointer(item.vptr)
	return int64(len(item.key)) + int64(vp.Len)
}

type IteratorOptions struct {
	PrefetchSize   int    // 预取数量 (简化版暂未实现异步预取，保留字段)
	PrefetchValues bool   // 是否预取 Value
	Reverse        bool   // 是否反向遍历
	AllVersions    bool   // 是否输出所有版本 (包括删除标记和旧版本)
	Prefix         []byte // 前缀过滤
}

var DefaultIteratorOptions = IteratorOptions{
	PrefetchSize:   100,
	PrefetchValues: true,
	Reverse:        false,
	AllVersions:    false,
}

type Iterator struct {
	iitr   y.Iterator // 底层：MergeIterator
	txn    *Txn
	readTs uint64 // 事务时间戳
	opt    IteratorOptions

	item    *Item  // 当前指向的数据
	lastKey []byte // 用于去重 (MVCC)
}

func (txn *Txn) NewIterator(opt IteratorOptions) *Iterator {
	if txn.discarded {
		panic(ErrDiscardedTxn)
	}
	tables := txn.db.NewIterators(opt.Reverse)
	if pending := txn.newPendingWritesIterator(opt.Reverse); pending != nil {
		tables = append(tables, pending)
	}
	mi := table.NewMergeIterator(tables, opt.Reverse)
	mi.Rewind()
	it := &Iterator{
		iitr:   mi,
		txn:    txn,
		readTs: txn.readTs,
		opt:    opt,
	}
	return it
}
func (txn *Txn) NewKeyIterator(key []byte, opt IteratorOptions) *Iterator {
	if len(opt.Prefix) > 0 {
		tool.Log.Fatal("opt.Prefix should be nil for  NewKeyIterator.")
	}
	opt.Prefix = key
	opt.AllVersions = true
	return txn.NewIterator(opt)
}

func (it *Iterator) Item() *Item {
	tx := it.txn
	tx.addReadKey(it.item.Key())
	return it.item
}

func (it *Iterator) Valid() bool {
	if it.item == nil {
		return false
	}
	return bytes.HasPrefix(it.item.key, it.opt.Prefix)
}

// ValidForPrefix 前缀检查
func (it *Iterator) ValidForPrefix(prefix []byte) bool {
	return it.Valid() && bytes.HasPrefix(it.item.key, prefix)
}

func (it *Iterator) Close() {
	it.iitr.Close()
}

func (it *Iterator) Rewind() {
	it.Seek(nil)
}

func (it *Iterator) Seek(key []byte) {
	if it.iitr == nil {
		return
	}
	if len(key) > 0 {
		it.txn.addReadKey(key)
	}
	it.lastKey = it.lastKey[:0]
	if len(key) == 0 {
		key = it.opt.Prefix
	}
	if len(key) == 0 {
		it.iitr.Rewind()
		it.prefetch()
		return
	}
	if !it.opt.Reverse {
		key = y.KeyWithTs(key, it.txn.readTs)
	} else {
		key = y.KeyWithTs(key, 0)
	}
	it.iitr.Seek(key)
	it.prefetch()
}
func (it *Iterator) Next() {
	if it.iitr == nil {
		return
	}
	if it.Valid() {
		// 记录当前 Key 为“已读”，强制 parseItem 找下一个不同的 Key
		it.lastKey = y.SafeCopy(it.lastKey, it.item.key)
		it.iitr.Next()
	}
	it.prefetch()
}

func (it *Iterator) hasPrefix() bool {
	//我们不应该检查前缀，以防迭代器反向运行因为相反我们期望
	//人们将项目附加到前缀末尾。
	if !it.opt.Reverse && len(it.opt.Prefix) > 0 {
		return bytes.HasPrefix(y.ParseKey(it.iitr.Key()), it.opt.Prefix)
	}
	return true
}
func (it *Iterator) prefetch() {
	it.item = nil
	for it.iitr.Valid() && it.hasPrefix() {
		// parseItem 现在只负责检查当前这一个，如果不合格它会自己 Next
		if !it.parseItem() {
			continue
		}
		break
	}
}

// 在底层流中找到下一个有效的 Item
func (it *Iterator) parseItem() bool {
	key := it.iitr.Key()
	userKey := y.ParseKey(key)
	ts := y.ParseTs(key)
	if ts > it.readTs {
		it.iitr.Next()
		return false
	}
	if len(it.opt.Prefix) > 0{
		if !bytes.HasPrefix(userKey,it.opt.Prefix){
			if !it.opt.Reverse{
				it.iitr.Next() 
				return false
			}
			it.iitr.Next()
			return false
		}

	}
	if it.lastKey != nil && bytes.Equal(userKey, it.lastKey){
		it.iitr.Next()
		return false
	}
	vs := it.iitr.Value()
	if isDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		if !it.opt.Reverse {
			it.lastKey = y.SafeCopy(it.lastKey, userKey)
		}
		it.iitr.Next()
		return false
	}
	it.item = &Item{
		key:       y.SafeCopy(nil, userKey),
		version:   ts,
		meta:      vs.Meta,
		userMeta:  vs.UserMeta,
		expiresAt: vs.ExpiresAt,
		txn:       it.txn,
	}

	if vs.Meta&y.BitValuePointer > 0 {
		it.item.vptr = y.SafeCopy(nil, vs.UserValue)
	} else {
		it.item.val = y.SafeCopy(nil, vs.UserValue)
	}
	return true
}

func isDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if meta&y.BitDelete > 0 {
		return true
	}
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}
