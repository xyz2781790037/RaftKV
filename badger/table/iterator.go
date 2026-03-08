package table

import (
	"RaftKV/badger/skl"
	"RaftKV/badger/y"
	"io"
	"sort"
)

type blockIterator struct {
	data []byte // Block 数据
	idx  int    // 原 offset
	key  []byte
	val  []byte
	err  error
}

func (itr *blockIterator) setBlock(data []byte) {
	itr.data = data
	itr.idx = 0
	itr.err = nil
	itr.parse()
}

func (itr *blockIterator) seekToFirst() {
	itr.idx = 0
	itr.parse()
}

func (itr *blockIterator) seek(key []byte) {
	itr.seekToFirst()
	for itr.Valid() {
		if y.CompareKeys(itr.key, key) >= 0 {
			break
		}
		itr.next()
	}
}

func (itr *blockIterator) next() {
	if itr.idx >= len(itr.data) {
		itr.err = io.EOF
		return
	}

	header, n := skl.DecodeHeader(itr.data[itr.idx:])
	if header == nil {
		itr.err = io.EOF
		return
	}

	itr.idx += n + int(header.KeyLen) + int(header.ValueLen)
	itr.parse()
}

// parse 解析当前 idx 处的 Entry
func (itr *blockIterator) parse() {
	if itr.idx >= len(itr.data) {
		itr.key = nil
		itr.val = nil
		itr.err = io.EOF
		return
	}

	header, n := skl.DecodeHeader(itr.data[itr.idx:])
	if header == nil {
		itr.key = nil
		itr.val = nil
		itr.err = io.EOF
		return
	}

	keyOffset := itr.idx + n
	itr.key = itr.data[keyOffset : keyOffset+int(header.KeyLen)]

	valOffset := keyOffset + int(header.KeyLen)
	itr.val = itr.data[valOffset : valOffset+int(header.ValueLen)]
}

func (itr *blockIterator) Valid() bool {
	return itr.key != nil && itr.err == nil
}

func (itr *blockIterator) Close() {
}

type Iterator struct {
	t    *Table
	bpos int           // 原 blockIdx，官方叫 bpos (Block Position)
	bi   blockIterator // 原 blockIter，官方叫 bi，且是结构体值而非指针
	err  error
	opt  int // 预留选项
}

func (t *Table) NewIterator(opt int) *Iterator {
	return &Iterator{
		t:    t,
		bpos: -1,
		opt:  opt,
	}
}

func (itr *Iterator) Close() error {
	return nil
}

func (itr *Iterator) Valid() bool {
	return itr.err == nil && itr.bi.Valid()
}

func (itr *Iterator) Rewind() {
	itr.seekToFirst()
}

// Seek -> 内部调用 seek
func (itr *Iterator) Seek(key []byte) {
	itr.seek(key)
}

// Next -> 内部调用 next
func (itr *Iterator) Next() {
	itr.next()
}

// Key 返回当前 Key
func (itr *Iterator) Key() []byte {
	return itr.bi.key
}

// Value 返回 ValueStruct
func (itr *Iterator) Value() y.ValueStruct {
	var vs y.ValueStruct
	vs.Decode(itr.bi.val)
	return vs
}

// seekToFirst
func (itr *Iterator) seekToFirst() {
	itr.bpos = 0
	itr.loadBlock()
	if itr.err == nil {
		itr.bi.seekToFirst()
	}
}

func (itr *Iterator) seek(key []byte) {
	idx := sort.Search(len(itr.t.blockIndices), func(i int) bool {
		return y.CompareKeys(itr.t.blockIndices[i].FirstKey, key) > 0
	})

	itr.bpos = idx - 1
	if itr.bpos < 0 {
		itr.bpos = 0
	}

	itr.loadBlock()

	if itr.err == nil {
		itr.bi.seek(key)
		for !itr.bi.Valid() && itr.bpos < len(itr.t.blockIndices)-1 {
			itr.bpos++        // 跨到下一个 Block
			itr.loadBlock()   // 加载新 Block 的数据
			if itr.err == nil {
				itr.bi.seek(key) // 再次在这个新 Block 里寻找
			}
		}
	}
}

func (itr *Iterator) next() {
	if !itr.Valid() {
		return
	}

	itr.bi.next()
	if !itr.bi.Valid() {
		itr.bpos++
		itr.loadBlock()
	}
}

func (itr *Iterator) loadBlock() {
	if itr.bpos < 0 || itr.bpos >= len(itr.t.blockIndices) {
		itr.err = io.EOF
		return
	}

	itr.err = nil
	meta := itr.t.blockIndices[itr.bpos]
	blockData := make([]byte, meta.Size)

	_, err := itr.t.fd.ReadAt(blockData, int64(meta.Offset))
	if err != nil {
		itr.err = err
		return
	}

	itr.bi.setBlock(blockData)
}

var (
	NONE     int = 0
	REVERSED int = 2
	NOCACHE  int = 4
)
