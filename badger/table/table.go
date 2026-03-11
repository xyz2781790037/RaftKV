package table

import (
	"RaftKV/badger/filter"
	"RaftKV/badger/skl"
	"RaftKV/badger/y"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"
)

// Table 代表一个只读的 SSTable 文件
type Table struct {
	fd           *os.File
	fileSize     int64
	blockIndices []*BlockMeta // 内存中的稀疏索引
	filter       *filter.BloomFilter
	smallest     []byte
	biggest      []byte
	blockCache   sync.Map
}

// OpenTable 打开一个 SSTable 文件并加载索引
func OpenTable(filename string) (*Table, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()
	if fileSize < 24 {
		return nil, fmt.Errorf("file too small")
	}

	t := &Table{
		fd:       file,
		fileSize: fileSize,
	}

	if err := t.loadIndexAndFilter(); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Table) loadIndexAndFilter() error {
	footer := make([]byte, 24)
	if _, err := t.fd.ReadAt(footer, t.fileSize-24); err != nil {
		return err
	}
	indexOffset := binary.BigEndian.Uint64(footer[0:8])
	filterOffset := binary.BigEndian.Uint64(footer[8:16])
	filterLen := binary.BigEndian.Uint64(footer[16:24])
	filterData := make([]byte, filterLen)
    if _, err := t.fd.ReadAt(filterData, int64(filterOffset)); err != nil {
        return err
    }
    
    k := filterData[filterLen-1]
    actualFilterData := filterData[:filterLen-1]
    
    t.filter = filter.FromBytes(actualFilterData, k) // 动态加载真实的 K
	// t.filter = filter.FromBytes(filterData, 7)

	indexSize := int64(filterOffset) - int64(indexOffset)
	if indexSize <= 0 {
		return fmt.Errorf("invalid index size")
	}

	indexData := make([]byte, indexSize)
	if _, err := t.fd.ReadAt(indexData, int64(indexOffset)); err != nil {
		return err
	}

	buf := bytes.NewBuffer(indexData)
	for buf.Len() > 0 {
		keyLen, err := binary.ReadUvarint(buf)
		if err != nil {
			return err
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(buf, key); err != nil {
			return err
		}

		offset, err := binary.ReadUvarint(buf)
		if err != nil {
			return err
		}

		size, err := binary.ReadUvarint(buf)
		if err != nil {
			return err
		}

		t.blockIndices = append(t.blockIndices, &BlockMeta{
			FirstKey: key,
			Offset:   offset,
			Size:     uint32(size),
		})
	}

	if len(t.blockIndices) > 0 {
		t.smallest = t.blockIndices[0].FirstKey
		lastMeta := t.blockIndices[len(t.blockIndices)-1]
		blockData := make([]byte, lastMeta.Size)
		if _, err := t.fd.ReadAt(blockData, int64(lastMeta.Offset)); err == nil {
			var lastKey []byte
			offset := 0
			for offset < len(blockData) {
				h := &skl.EntryHeader{}
				headerSize := h.Decode(blockData[offset:])
				if headerSize == 0 {
					break
				}
				keyOffset := offset + headerSize
				lastKey = blockData[keyOffset : keyOffset+int(h.KeyLen)]

				offset += headerSize + int(h.KeyLen) + int(h.ValueLen)
			}
			t.biggest = make([]byte, len(lastKey))
			copy(t.biggest, lastKey)
		}
	}
	return nil
}
// func (t *Table) Get(key []byte) ([]byte, byte, error) {
// 	if !t.filter.MayContain(key) {
// 		return nil, 0, fmt.Errorf("key not found (filtered)")
// 	}
// 	idx := sort.Search(len(t.blockIndices), func(i int) bool {
// 		return bytes.Compare(t.blockIndices[i].FirstKey, key) > 0
// 	})

// 	idx = idx - 1
// 	if idx < 0 {
// 		return nil, 0, fmt.Errorf("key not found (too small)")
// 	}

// 	targetMeta := t.blockIndices[idx]
// 	blockData := make([]byte, targetMeta.Size)
// 	if _, err := t.fd.ReadAt(blockData, int64(targetMeta.Offset)); err != nil {
// 		return nil, 0, err
// 	}

// 	offset := 0
// 	for offset < len(blockData) {
// 		h := &skl.EntryHeader{}
// 		headerSize := h.Decode(blockData[offset:])
// 		if headerSize == 0 {
// 			break
// 		}

// 		keyOffset := offset + headerSize
// 		currentKey := blockData[keyOffset : keyOffset+int(h.KeyLen)]

// 		cmp := bytes.Compare(currentKey, key)
// 		if cmp == 0 {
// 			valOffset := keyOffset + int(h.KeyLen)
// 			val := blockData[valOffset : valOffset+int(h.ValueLen)]
// 			return val, h.Meta, nil
// 		} else if cmp > 0 {
// 			break
// 		}

// 		offset += headerSize + int(h.KeyLen) + int(h.ValueLen)
// 	}

// 	return nil, 0, fmt.Errorf("key not found in block")
// }
// 注意函数签名变了，增加了 readTs，返回了 ts
// Get 在 SSTable 中查询指定 Key 的数据，支持 MVCC 时间戳和跨块扫描
// Get 在 SSTable 中查询指定 Key 的数据，支持 MVCC 时间戳和跨块扫描
func (t *Table) Get(key []byte, readTs uint64) ([]byte, byte, uint64, error) {
	// 1. 布隆过滤器瞬切
	if !t.filter.MayContain(key) {
		return nil, 0, 0, fmt.Errorf("key not found (filtered)")
	}

	// 🚨 终极防御第一重：寻找 Block 时，使用该键的绝对物理最小值！
	// math.MaxUint64 翻转后后缀全是 0x00，保证搜索下界绝对在目标之前。
	searchMin := y.KeyWithTs(key, math.MaxUint64)
	
	idx := sort.Search(len(t.blockIndices), func(i int) bool {
		return y.CompareKeys(t.blockIndices[i].FirstKey, searchMin) > 0
	})

	idx = idx - 1
	if idx < 0 {
		idx = 0
	}

	for ; idx < len(t.blockIndices); idx++ {
		targetMeta := t.blockIndices[idx]

		var blockData []byte
		if cachedBlock, ok := t.blockCache.Load(targetMeta.Offset); ok {
			blockData = cachedBlock.([]byte)
		} else {
			blockData = make([]byte, targetMeta.Size)
			if _, err := t.fd.ReadAt(blockData, int64(targetMeta.Offset)); err != nil {
				return nil, 0, 0, err
			}
			t.blockCache.Store(targetMeta.Offset, blockData)
		}

		offset := 0
		for offset < len(blockData) {
			h := &skl.EntryHeader{}
			headerSize := h.Decode(blockData[offset:])
			if headerSize == 0 {
				break
			}

			keyOffset := offset + headerSize
			internalKey := blockData[keyOffset : keyOffset+int(h.KeyLen)]
			
			userKey := y.ParseKey(internalKey)
			ts := y.ParseTs(internalKey)

			if bytes.Equal(userKey, key) {
				// 匹配当前事务的 MVCC 读版本
				if ts <= readTs {
					valOffset := keyOffset + int(h.KeyLen)
					val := blockData[valOffset : valOffset+int(h.ValueLen)]
					return val, h.Meta, ts, nil
				}
			} else {
				
				if bytes.Compare(userKey, key) > 0 && !bytes.HasPrefix(userKey, key) {
					return nil, 0, 0, fmt.Errorf("key not found")
				}
			}

			offset += headerSize + int(h.KeyLen) + int(h.ValueLen)
		}
	}

	return nil, 0, 0, fmt.Errorf("key not found in table")
}
func (t *Table) Close() error {
	return t.fd.Close()
}

// Scan 遍历 SSTable 中的所有数据
// 这是一个简化版的迭代器，按顺序回调
// func (t *Table) Scan(fn func(key, val []byte, meta byte) bool) error {
// 	// 遍历所有 Block
// 	for _, meta := range t.blockIndices {
// 		// 1. 读取 Block
// 		blockData := make([]byte, meta.Size)
// 		if _, err := t.fd.ReadAt(blockData, int64(meta.Offset)); err != nil {
// 			return err
// 		}

// 		// 2. 解析 Block 内部的 Entry
// 		offset := 0
// 		for offset < len(blockData) {
// 			h := &skl.EntryHeader{}
// 			headerSize := h.Decode(blockData[offset:])
// 			if headerSize == 0 {
// 				break
// 			}

// 			keyOffset := offset + headerSize
// 			key := blockData[keyOffset : keyOffset+int(h.KeyLen)]
// 			valOffset := keyOffset + int(h.KeyLen)
// 			val := blockData[valOffset : valOffset+int(h.ValueLen)]

// 			// 回调
// 			if !fn(key, val, h.Meta) {
// 				return nil // 用户终止
// 			}

// 			offset += headerSize + int(h.KeyLen) + int(h.ValueLen)
// 		}
// 	}
// 	return nil
// }
func (t *Table) Scan(fn func(key, val []byte, meta byte,expiresAt uint64) bool) error {
	for _, meta := range t.blockIndices {
		blockData := make([]byte, meta.Size)
		if _, err := t.fd.ReadAt(blockData, int64(meta.Offset)); err != nil {
			return err
		}

		offset := 0
		for offset < len(blockData) {
			h := &skl.EntryHeader{}
			headerSize := h.Decode(blockData[offset:])
			if headerSize == 0 {
				break
			}

			keyOffset := offset + headerSize
			key := blockData[keyOffset : keyOffset+int(h.KeyLen)]
			valOffset := keyOffset + int(h.KeyLen)
			encodedVal := blockData[valOffset : valOffset+int(h.ValueLen)]

			// 🚨 核心修复：还原出真实的 UserValue，防止快照生成时发生双重编码
			var vs y.ValueStruct
			vs.Decode(encodedVal)
			// 回调函数只接收纯净的 UserValue
			if !fn(key, vs.UserValue, h.Meta,vs.ExpiresAt) {
				return nil 
			}

			offset += headerSize + int(h.KeyLen) + int(h.ValueLen)
		}
	}
	return nil
}
func (t *Table) Fd() *os.File {
	return t.fd
}
func (t *Table) Smallest() []byte {
	return t.smallest
}
func (t *Table) Biggest() []byte {
	return t.biggest
}
func (t *Table) Size() int64 {
	return t.fileSize
}
