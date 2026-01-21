package badger

import (
	skl "RaftKV/badger/skiplist"
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const maxHeaderSize = 21

var ErrCrcMismatch = errors.New("wal: crc mismatch")

type WAL struct {
	f         *os.File
	path      string
	mu        sync.Mutex // 保证并发写入时的线程安全
	curOffset int64
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	w := &WAL{
		f:         f,
		path:      path,
		curOffset: stat.Size(),
	}
	return w, nil
}
func (w *WAL) Write(e *skl.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	keyLen := len(e.Key)
	valLen := len(e.Value)
	buf := make([]byte, maxHeaderSize+keyLen+valLen+4)
	buf[0] = e.Meta
	index := 1
	index += binary.PutUvarint(buf[index:], uint64(keyLen))
	index += binary.PutUvarint(buf[index:], uint64(valLen))

	copy(buf[index:], e.Key)
	index += keyLen
	copy(buf[index:], e.Value)
	index += valLen
	//crc就是确保数据完整性
	crc := crc32.Checksum(buf[:index], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(buf[index:], crc)
	index += 4

	n, err := w.f.Write(buf[:index])
	if err != nil {
		return err
	}
	w.curOffset += int64(n)
	return nil
}
func (w *WAL) ReadWAL() ([]*skl.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.f.Seek(0, 0); err != nil {
		return nil, err
	}
	r := bufio.NewReader(w.f)
	var entries []*skl.Entry
	var validBytes int64 = 0
	for {
		e, totalSize, err := w.readEntry(r)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("WAL corrupted at offset %d: %v. Truncating...\n", validBytes, err)
			return entries, w.truncate(validBytes)
		}
		entries = append(entries, e)
		validBytes += totalSize
	}
	return entries,nil
}

func (w *WAL) readEntry(r *bufio.Reader) (*skl.Entry, int64, error) {
	meta, err := r.ReadByte()
	if err != nil {
		return nil, 0, err
	}
	kLen, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, 0, err
	}
	vLen, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, 0, err
	}
	var headerBuf [maxHeaderSize]byte
	headerBuf[0] = meta
	n := 1
	n += binary.PutUvarint(headerBuf[n:], kLen)
	n += binary.PutUvarint(headerBuf[n:], vLen)

	kl := int(kLen)
	vl := int(vLen)
	e := &skl.Entry{
		Meta:  meta,
		Key:   make([]byte, kl),
		Value: make([]byte, vl),
	}
	if _, err := io.ReadFull(r, e.Key); err != nil {
		return nil, 0, err
	}
	if _, err := io.ReadFull(r, e.Value); err != nil {
		return nil, 0, err
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return nil, 0, err
	}
	
	expectedCRC := binary.BigEndian.Uint32(crcBuf[:])
	crc := crc32.Update(0, crc32.MakeTable(crc32.Castagnoli), headerBuf[:n])
	crc = crc32.Update(crc, crc32.MakeTable(crc32.Castagnoli), e.Key)
	crc = crc32.Update(crc, crc32.MakeTable(crc32.Castagnoli), e.Value)
	if crc != expectedCRC {
		return nil, 0, ErrCrcMismatch
	}
	totalSize := int64(n + kl + vl + 4)
	return e, totalSize, nil
}
func (w *WAL) WriteBatch(entries []*skl.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 实际工程中这里会复用 buffer池
	var batchBuf []byte
	for _, e := range entries {
		keyLen := len(e.Key)
		valLen := len(e.Value)
		buf := make([]byte, maxHeaderSize+keyLen+valLen+4)
		
		buf[0] = e.Meta
		idx := 1
		idx += binary.PutUvarint(buf[idx:], uint64(keyLen))
		idx += binary.PutUvarint(buf[idx:], uint64(valLen))
		copy(buf[idx:], e.Key)
		idx += keyLen
		copy(buf[idx:], e.Value)
		idx += valLen
		
		crc := crc32.Checksum(buf[:idx], crc32.MakeTable(crc32.Castagnoli))
		binary.BigEndian.PutUint32(buf[idx:], crc)
		idx += 4
		
		batchBuf = append(batchBuf, buf[:idx]...)
	}
	
	n, err := w.f.Write(batchBuf)
	if err != nil {
		return err
	}
	w.curOffset += int64(n)
	return nil
}
func (w *WAL) truncate(size int64) error {
	if err := w.f.Truncate(size); err != nil {
		return err
	}
	// 必须把读写指针移到截断后的末尾，否则下次写入会报错或写出空洞
	if _, err := w.f.Seek(size, 0); err != nil {
		return err
	}
	w.curOffset = size
	return nil
}

// Sync 强制刷盘
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

// Close 关闭文件
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
