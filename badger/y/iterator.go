package y

import (
	"bytes"
	"encoding/binary"
	"time"
)

type ValueStruct struct {
	Meta      byte
	UserMeta  byte
	UserValue []byte
	ExpiresAt uint64 // 0 表示不过期
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func (v *ValueStruct) Decode(b []byte) {
	if len(b) == 0 {
		return
	}
	v.Meta = b[0]
	offset := 1
	if v.Meta& BitHasTTL > 0 {
		var sz int
		v.ExpiresAt, sz = binary.Uvarint(b[offset:])
		offset += sz
	} else {
		v.ExpiresAt = 0
	}

	v.UserValue = b[offset:]
}
func (v *ValueStruct) EncodedSize() uint32 {
	sz := len(v.UserValue) + 1 // meta
	if v.Meta & BitHasTTL > 0 { // 👈 修改这里
		sz += sizeVarint(v.ExpiresAt)
	}
	return uint32(sz)
}

func (v *ValueStruct) Encode(b []byte) uint32 {
	b[0] = v.Meta
	offset := 1
	if v.Meta & BitHasTTL > 0 { // 👈 修改这里
		n := binary.PutUvarint(b[offset:], v.ExpiresAt)
		offset += n
	}
	n := copy(b[offset:], v.UserValue)
	return uint32(offset + n)
}

func (v *ValueStruct) EncodeTo(buf *bytes.Buffer) {
	buf.WriteByte(v.Meta)
	if v.Meta & BitHasTTL > 0 { // 👈 修改这里
		var enc [binary.MaxVarintLen64]byte
		sz := binary.PutUvarint(enc[:], v.ExpiresAt)
		buf.Write(enc[:sz])
	}
	buf.Write(v.UserValue)
}
func (v *ValueStruct) IsExpired() bool {
	return v.ExpiresAt > 0 && uint64(time.Now().Unix()) > v.ExpiresAt
}
func (v *ValueStruct) IsDeleted() bool {
	return v.Meta & BitDelete > 0
}

type Iterator interface {
	Rewind()         // 回到开头
	Seek(key []byte) // 寻找 >= key 的位置
	Next()           // 下一个
	Valid() bool     // 是否有效
	Key() []byte     // 获取内部 Key (Key+Ts)
	Value() ValueStruct   // 获取 Value (Encoded Value)
	Close() error    // 关闭资源
}