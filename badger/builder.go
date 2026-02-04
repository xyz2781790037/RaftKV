package badger

import (
	"RaftKV/badger/filter"
	"RaftKV/badger/skl"
	"bytes"
	"encoding/binary"
	"os"
)

const TargetBlockSize = 4 * 1024

type Builder struct {
	fd           *os.File
	currentBlock *bytes.Buffer // 当前正在构建的数据块
	blockIndices []*BlockMeta  // 内存中缓存的索引信息

	// 统计信息
	fileSize   int64
	entryCount int64

	// 查找时：如果 target < FirstKey，说明肯定不在这个块里
	firstKeyInBlock []byte
	filter *filter.BloomFilter
}

// BlockMeta 索引元数据：记录 block 在文件中的位置和范围
type BlockMeta struct {
	FirstKey []byte
	Offset   uint64
	Size     uint32
}

func NewBuilder(filename string) (*Builder, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	bf := filter.NewBloomFilter(2000, 0.01)
	b := &Builder{
		fd:           file,
		currentBlock: bytes.NewBuffer(make([]byte, 0, TargetBlockSize)),
		blockIndices: make([]*BlockMeta, 0),
		filter: bf,
	}
	return b, err
}
func (b *Builder) Add(key []byte, value []byte, meta byte) error {
	estimatedSize := maxHeaderSize + len(key) + len(value)
	if b.currentBlock.Len() > 0 && b.currentBlock.Len()+estimatedSize > TargetBlockSize {
		if err := b.finishBlock(); err != nil {
			return err
		}
	}
	if b.currentBlock.Len() == 0 {
		b.firstKeyInBlock = append([]byte{}, key...)
	}
	b.filter.Add(key)
	h := skl.EntryHeader{
		Meta:     meta,
		KeyLen:   uint32(len(key)),
		ValueLen: uint32(len(value)),
	}
	var headerBuf [16]byte
	n := h.Encode(headerBuf[:])
	b.currentBlock.Write(headerBuf[:n])
	b.currentBlock.Write(key)
	b.currentBlock.Write(value)
	b.entryCount++
	return nil
}
func (b *Builder) finishBlock() error {
	if b.currentBlock.Len() == 0 {
		return nil
	}
	data := b.currentBlock.Bytes()
	blockSize := len(data)
	meta := &BlockMeta{
		FirstKey: b.firstKeyInBlock,
		Offset:   uint64(b.fileSize),
		Size:     uint32(blockSize),
	}
	b.blockIndices = append(b.blockIndices, meta)
	n, err := b.fd.Write(data)
	if err != nil {
		return err
	}
	b.fileSize += int64(n)
	b.currentBlock.Reset() // 清空 buffer 以重用
	b.firstKeyInBlock = nil
	return nil
}
func (b *Builder) Finish() error {
    if err := b.finishBlock(); err != nil {
        return err
    }

    indexOffset := uint64(b.fileSize)
    indexBuf := new(bytes.Buffer)
    var buf [10]byte
    for _, meta := range b.blockIndices {
        n := binary.PutUvarint(buf[:], uint64(len(meta.FirstKey)))
        indexBuf.Write(buf[:n])
        indexBuf.Write(meta.FirstKey)
        n = binary.PutUvarint(buf[:], uint64(meta.Offset))
        indexBuf.Write(buf[:n])
        n = binary.PutUvarint(buf[:], uint64(meta.Size))
        indexBuf.Write(buf[:n])
    }
    if _, err := b.fd.Write(indexBuf.Bytes()); err != nil {
        return err
    }
    b.fileSize += int64(indexBuf.Len())
    filterOffset := uint64(b.fileSize)
    filterData := b.filter.Bytes()
    filterLen := uint64(len(filterData))

    if _, err := b.fd.Write(filterData); err != nil {
        return err
    }
    b.fileSize += int64(filterLen)
    var footer [24]byte
    binary.BigEndian.PutUint64(footer[0:8], indexOffset)
    binary.BigEndian.PutUint64(footer[8:16], filterOffset)
    binary.BigEndian.PutUint64(footer[16:24], filterLen)

    if _, err := b.fd.Write(footer[:]); err != nil {
        return err
    }

    if err := b.fd.Sync(); err != nil {
        return err
    }
    return b.fd.Close()
}