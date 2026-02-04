package filter

import (
	"hash/fnv"
	"math"
)
type BloomFilter struct {
	bitmap []byte // 位图
	k      uint8  // 哈希函数的个数
}
func NewBloomFilter(n int, p float64) *BloomFilter{
	m := -float64(n) * math.Log(p) / (math.Log(2) * math.Log(2))
	k := (m / float64(n)) * math.Log(2)
	numBytes := int(math.Ceil(m / 8))
	return &BloomFilter{
		bitmap: make([]byte, numBytes),
		k:      uint8(k),
	}
}
func FromBytes(data []byte, k uint8) *BloomFilter {
	return &BloomFilter{
		bitmap: data,
		k:      k,
	}
}
func (bf *BloomFilter) Add(key []byte){
	h1, h2 := getHash(key)

	for i := uint8(0); i < bf.k; i++ {
		ind := (h1 + uint32(i)*h2) % uint32(len(bf.bitmap)*8)
		
		byteInd := ind / 8 // 第几个字节
		bitInd := ind % 8  // 字节里的第几位
		bf.bitmap[byteInd] |= (1 << bitInd)
	}
}
func (bf *BloomFilter) MayContain(key []byte) bool {
	if len(bf.bitmap) == 0 {
		return false
	}

	h1, h2 := getHash(key)

	for i := uint8(0); i < bf.k; i++ {
		ind := (h1 + uint32(i)*h2) % uint32(len(bf.bitmap)*8)
		byteInd := ind / 8
		bitInd := ind % 8

		if bf.bitmap[byteInd]&(1<<bitInd) == 0 {
			return false
		}
	}
	return true
}
func (bf *BloomFilter) Bytes() []byte {
	return bf.bitmap
}
func (bf *BloomFilter) K() uint8 {
	return bf.k
}
func getHash(data []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(data)
	sum := h.Sum64()
	
	// 高 32 位作为 h2，低 32 位作为 h1
	h1 := uint32(sum)
	h2 := uint32(sum >> 32)
	return h1, h2
}