package badger

import (
	"RaftKV/badger/skl"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

// Table 代表一个只读的 SSTable 文件
type Table struct {
    fd           *os.File
    fileSize     int64
    blockIndices []*BlockMeta // 内存中的稀疏索引
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
    if fileSize < 8 {
        return nil, fmt.Errorf("file too small")
    }

    t := &Table{
        fd:       file,
        fileSize: fileSize,
    }

    if err := t.loadIndex(); err != nil {
        return nil, err
    }

    return t, nil
}

func (t *Table) loadIndex() error {
    footer := make([]byte, 8)
    if _, err := t.fd.ReadAt(footer, t.fileSize-8); err != nil {
        return err
    }
    indexOffset := binary.BigEndian.Uint64(footer)

    indexSize := t.fileSize - int64(indexOffset) - 8
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
    return nil
}

func (t *Table) Get(key []byte) ([]byte, byte, error) {
    idx := sort.Search(len(t.blockIndices), func(i int) bool {
        return bytes.Compare(t.blockIndices[i].FirstKey, key) > 0
    })

    idx = idx - 1
    if idx < 0 {
        return nil, 0, fmt.Errorf("key not found (too small)")
    }

    targetMeta := t.blockIndices[idx]
    blockData := make([]byte, targetMeta.Size)
    if _, err := t.fd.ReadAt(blockData, int64(targetMeta.Offset)); err != nil {
        return nil, 0, err
    }

    offset := 0
    for offset < len(blockData) {
        h := &skl.EntryHeader{}
        headerSize := h.Decode(blockData[offset:])
        if headerSize == 0 {
            break 
        }
        
        keyOffset := offset + headerSize
        currentKey := blockData[keyOffset : keyOffset+int(h.KeyLen)]
        
        cmp := bytes.Compare(currentKey, key)
        if cmp == 0 {
            valOffset := keyOffset + int(h.KeyLen)
            val := blockData[valOffset : valOffset+int(h.ValueLen)]
            return val, h.Meta, nil
        } else if cmp > 0 {
            break
        }

        offset += headerSize + int(h.KeyLen) + int(h.ValueLen)
    }

    return nil, 0, fmt.Errorf("key not found in block")
}

func (t *Table) Close() error {
    return t.fd.Close()
}
// Scan 遍历 SSTable 中的所有数据
// 这是一个简化版的迭代器，按顺序回调
func (t *Table) Scan(fn func(key, val []byte, meta byte) bool) error {
    // 遍历所有 Block
    for _, meta := range t.blockIndices {
        // 1. 读取 Block
        blockData := make([]byte, meta.Size)
        if _, err := t.fd.ReadAt(blockData, int64(meta.Offset)); err != nil {
            return err
        }

        // 2. 解析 Block 内部的 Entry
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
            val := blockData[valOffset : valOffset+int(h.ValueLen)]
            
            // 回调
            if !fn(key, val, h.Meta) {
                return nil // 用户终止
            }
            
            offset += headerSize + int(h.KeyLen) + int(h.ValueLen)
        }
    }
    return nil
}