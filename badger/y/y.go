package y

import (
	"bytes"
	"encoding/binary"
	"math"
)
const (
	BitDelete       byte = 1 << 0 // 标记：已删除
	BitValuePointer byte = 1 << 1 // 标记：值指针 (Vlog)
	BitHasTTL       byte = 1 << 2 // 标记：有过期时间
)
func KeyWithTs(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+8)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], math.MaxUint64-ts)
	return out
}
func ParseTs(key []byte) uint64 {
	if len(key) <= 8 {
		return 0
	}
	return math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
}
func ParseKey(key []byte) []byte {
	if len(key) < 8 {
		return nil
	}

	return key[:len(key)-8]
}
func SameKey(src, dst []byte) bool {
	if len(src) < 8 || len(dst) < 8 {
		return false
	}
	if len(src) != len(dst) {
		return false
	}
	return bytes.Equal(ParseKey(src), ParseKey(dst))
}
func SafeCopy(dst []byte, src []byte) []byte {
	if cap(dst) < len(src) {
		dst = make([]byte, len(src))
	}
	dst = dst[:len(src)]
	copy(dst, src)
	return dst
}
func CompareKeys(key1, key2 []byte) int {
    if len(key1) < 8 || len(key2) < 8 {
        return bytes.Compare(key1, key2)
    }
    if cmp := bytes.Compare(key1[:len(key1)-8], key2[:len(key2)-8]); cmp != 0 {
        return cmp
    }
    return bytes.Compare(key1[len(key1)-8:], key2[len(key2)-8:])
}