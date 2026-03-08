package skl

import (
	"RaftKV/badger/y"
	"encoding/binary"
	"time"
)
const (
	MaxBatchSize         = 1 << 20
	EntryHeaderSize      = 21
	BitDelete       byte = 1 << 0 // Set if the key has been deleted.
	BitValuePointer byte = 1 << 1 // Set if the value is NOT stored directly next to key.
)

type Entry struct {
	Key       []byte
	Value     []byte
	UserMeta  byte
	Meta      byte
	ExpiresAt uint64        // 绝对过期时间 (秒级时间戳)
	TTL       time.Duration // 存活时间 (用于计算 ExpiresAt)
	Ts        uint64        // MVCC 版本号
}
type EntryHeader struct {
	Meta     byte
	KeyLen   uint32
	ValueLen uint32
}

const maxHeaderSize = 1 + binary.MaxVarintLen32*2

func (h EntryHeader) Encode(out []byte) int {
	out[0] = h.Meta
	index := 1
	index += binary.PutUvarint(out[index:], uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:], uint64(h.ValueLen))
	return index
}
func (h *EntryHeader) Decode(buf []byte) int {
	h.Meta = buf[0]
	index := 1
	klen, count := binary.Uvarint(buf[index:])
	if count <= 0 {
		return 0
	}
	h.KeyLen = uint32(klen)
	index += count
	vlen, count := binary.Uvarint(buf[index:])
	if count <= 0 {
		return 0
	}
	h.ValueLen = uint32(vlen)
	return index + count
}
func DecodeHeader(buf []byte) (*EntryHeader, int) {
	if len(buf) == 0 {
		return nil, 0
	}
	h := &EntryHeader{}
	h.Meta = buf[0]
	index := 1

	klen, count := binary.Uvarint(buf[index:])
	if count <= 0 {
		return nil, 0
	}
	h.KeyLen = uint32(klen)
	index += count

	vlen, count := binary.Uvarint(buf[index:])
	if count <= 0 {
		return nil, 0
	}
	h.ValueLen = uint32(vlen)
	index += count

	return h, index
}
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
		Meta:  0,
	}
}
func (e *Entry) WithMeta(meta byte) *Entry {
	e.UserMeta = meta
	return e
}
func (e *Entry) WithTTL(dur time.Duration) *Entry {
	if dur > 0{
		e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
		e.Meta |= y.BitHasTTL
	}else{
		e.ExpiresAt = 0
		e.Meta &= ^y.BitHasTTL
	}
	return e
}
// 1. 判断是否是删除标记
func (e *Entry) IsDeleted() bool {
	return e.Meta&BitDelete != 0
}

// 2. 判断是否是存放在 Vlog 里的指针
func (e *Entry) IsValuePtr() bool {
	return e.Meta&BitValuePointer != 0
}

// 3. 预估在 WAL 中占用的空间 (用于判断是否需要触发刷盘)
func (e *Entry) EstimateSize() int {
	return EntryHeaderSize + len(e.Key) + len(e.Value) + 4 // 4 是 CRC
}
