package skiplist

import (
	"encoding/binary"
	"unsafe"
)

type valuePointer struct {
	Fid    uint32
	Len    uint32
	Offset uint32
}
const vptrSize = unsafe.Sizeof(valuePointer{})
type Entry struct {
	Key       []byte
	Value     []byte
	Meta      byte
	ExpiresAt uint64 // 想支持过期时间
}
type EntryHeader struct {
    Meta     byte
    KeyLen   uint32
    ValueLen uint32
}
const (
	MaxBatchSize = 1 << 20
	EntryHeaderSize = 21
)

func (h EntryHeader)Encode(out []byte) int{
	out[0] = h.Meta
	index := 1
	index += binary.PutUvarint(out[index:],uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:],uint64(h.ValueLen))
	return index
}
func (h *EntryHeader)Decode(buf []byte) int{
	h.Meta = buf[0]
	index := 1
	klen,count := binary.Uvarint(buf[index:])
	if count <= 0 { return 0 } // 错误处理
	h.KeyLen = uint32(klen)
	index += count
	vlen,count := binary.Uvarint(buf[index:])
	if count <= 0 { return 0 } // 错误处理
	h.ValueLen = uint32(vlen)
	return index + count
}
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
		Meta: 0,
	}
}
// 1. 判断是否是删除标记
func (e *Entry) IsDeleted() bool {
    return e.Meta & BitDelete != 0
}

// 2. 判断是否是存放在 Vlog 里的指针
func (e *Entry) IsValuePtr() bool {
    return e.Meta & BitValuePointer != 0
}

// 3. 预估在 WAL 中占用的空间 (用于判断是否需要触发刷盘)
func (e *Entry) EstimateSize() int {
    return EntryHeaderSize + len(e.Key) + len(e.Value) + 4 // 4 是 CRC
}