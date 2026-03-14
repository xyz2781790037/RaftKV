package badger

import (
	"RaftKV/badger/skl"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

const maxVlogHeaderSize = 21

var headerPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, maxVlogHeaderSize)
		return &b
	},
}

type ValueLog struct {
	sync.RWMutex
	dirPath       string              // Vlog 文件存储目录
	activeFile    *os.File            // 当前正在写入的活跃文件
	activeFid     uint32              // 当前活跃文件的 ID
	filesMap      map[uint32]*os.File // 旧文件的句柄缓存 (Fid -> File)
	currentOffset int64               // 活跃文件的当前写入偏移量
	opt           Options             // 配置项
}
type ValuePointer struct {
	Fid    uint32
	Len    uint32
	Offset uint64
}

const vptrSize = unsafe.Sizeof(ValuePointer{})

func OpenValueLog(dir string, opt Options) (*ValueLog, error) {
	if opt.ValueLogFileSize <= 0 {
		opt.ValueLogFileSize = MaxVLogFileSize
	}
	v := &ValueLog{
		dirPath:  dir,
		filesMap: make(map[uint32]*os.File),
		opt:      opt,
	}
	fids, err := v.getFids()
	if err != nil {
		return nil, err
	}
	var maxFid uint32 = 0
	for _, fid := range fids {
		path := filepath.Join(v.dirPath, fmt.Sprintf("%06d.vlog", fid))
		f, err := os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		v.filesMap[fid] = f
		if fid > maxFid {
			maxFid = fid
		}
	}
	if err := v.createActiveFile(v.activeFid); err != nil {
		return nil, err
	}
	return v, nil
}
func (v *ValueLog) getFids() ([]uint32, error) {
	entries, err := os.ReadDir(v.dirPath)
	if err != nil {
		return nil, err
	}

	var fids []uint32
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".vlog") {
			continue
		}
		// 解析文件名: "000023.vlog" -> 23
		name := strings.TrimSuffix(entry.Name(), ".vlog")
		id, err := strconv.ParseUint(name, 10, 32)
		if err != nil {
			continue
		}
		fids = append(fids, uint32(id))
	}

	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})
	return fids, nil
}
func (v *ValueLog) createActiveFile(fid uint32) error {
	path := filepath.Join(v.dirPath, fmt.Sprintf("%06d.vlog", fid))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	stat, _ := f.Stat()
	v.activeFile = f
	v.activeFid = fid
	v.currentOffset = stat.Size()
	v.filesMap[fid] = f

	return nil
}
func (v *ValueLog) Sync() error {
	v.RLock()
	defer v.RUnlock()
	if v.activeFile != nil {
		return v.activeFile.Sync()
	}
	return nil
}
func (v *ValueLog) Write(key, value []byte) (*ValuePointer, error) {
	v.Lock()
	defer v.Unlock()

	h := skl.EntryHeader{
		Meta:     0,
		KeyLen:   uint32(len(key)),
		ValueLen: uint32(len(value)),
	}
	var headerBuf [maxVlogHeaderSize]byte
	headerLen := h.Encode(headerBuf[:])

	entrySize := int64(headerLen) + int64(len(key)) + int64(len(value))
	if v.currentOffset+entrySize > v.opt.ValueLogFileSize {
		if err := v.rotate(); err != nil {
			return nil, err
		}
	}

	vp := &ValuePointer{
		Fid:    v.activeFid,
		Offset: uint64(v.currentOffset),
		Len:    uint32(len(value)),
	}

	buf := make([]byte, 0, entrySize)
	buf = append(buf, headerBuf[:headerLen]...)
	buf = append(buf, key...)
	buf = append(buf, value...)

	if _, err := v.activeFile.Write(buf); err != nil {
		return nil, err
	}

	v.currentOffset += entrySize
	return vp, nil
}
func (v *ValueLog) rotate() error {
	if err := v.activeFile.Sync(); err != nil {
		return err
	}

	newFid := v.activeFid + 1
	if err := v.createActiveFile(newFid); err != nil {
		return err
	}

	return nil
}
func (v *ValueLog) Read(vp *ValuePointer) ([]byte, error) {
	v.RLock()
	f, ok := v.filesMap[vp.Fid]
	v.RUnlock()
	if !ok {
		return nil, fmt.Errorf("vlog file not found: fid=%d", vp.Fid)
	}
	bufPtr := headerPool.Get().(*[]byte)
	headerBuf := *bufPtr
	defer headerPool.Put(bufPtr)
	n, err := f.ReadAt(headerBuf[:], int64(vp.Offset))
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n == 0 {
		return nil, io.EOF
	}
	h := skl.EntryHeader{}
	headerLen := h.Decode(headerBuf[:])
	valOffset := int64(vp.Offset) + int64(headerLen) + int64(h.KeyLen)
	val := make([]byte, vp.Len)
	if _, err := f.ReadAt(val, valOffset); err != nil {
		return nil, err
	}
	return val, nil
}
func EncodeValuePointer(vp *ValuePointer) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint32(buf[0:4], vp.Fid)
	binary.BigEndian.PutUint32(buf[4:8], vp.Len)
	binary.BigEndian.PutUint64(buf[8:16], vp.Offset)
	return buf
}

// DecodeValuePointer 反序列化指针
func DecodeValuePointer(data []byte) *ValuePointer {
	if len(data) < 16 {
		return nil
	}
	return &ValuePointer{
		Fid:    binary.BigEndian.Uint32(data[0:4]),
		Len:    binary.BigEndian.Uint32(data[4:8]),
		Offset: binary.BigEndian.Uint64(data[8:16]),
	}
}
func (v *ValueLog) Close() error {
	v.Lock()
	defer v.Unlock()
	if v.activeFile != nil {
		_ = v.activeFile.Sync()
	}

	for _, f := range v.filesMap {
		_ = f.Close()
	}
	return nil
}
func (v *ValueLog) readEntryFrom(f *os.File, offset int64, reuseBuf []byte) (*skl.Entry, *ValuePointer, int64, []byte, error) {
	var headerBuf [maxVlogHeaderSize]byte
	n, err := f.ReadAt(headerBuf[:], offset)
	if err != nil && err != io.EOF {
		return nil, nil, 0, reuseBuf, err
	}
	if n == 0 {
		return nil, nil, 0, reuseBuf, io.EOF
	}
	h := skl.EntryHeader{}
	headerLen := h.Decode(headerBuf[:])
	if headerLen == 0 {
		return nil, nil, 0, reuseBuf, io.EOF
	}
	totalLen := int(h.KeyLen + h.ValueLen)
	if cap(reuseBuf) < totalLen {
		reuseBuf = make([]byte, totalLen*2)
	}
	reuseBuf = reuseBuf[:totalLen]
	if _, err := f.ReadAt(reuseBuf, offset+int64(headerLen)); err != nil {
		return nil, nil, 0, reuseBuf, err
	}
	e := &skl.Entry{
		Meta:  h.Meta,
		Key:   reuseBuf[:h.KeyLen],
		Value: reuseBuf[h.KeyLen:],
	}
	vp := &ValuePointer{
		Fid:    0,
		Len:    uint32(len(e.Value)),
		Offset: uint64(offset),
	}
	nextOffset := offset + int64(headerLen+int(h.KeyLen)+int(h.ValueLen))
	return e, vp, nextOffset, reuseBuf, nil
}
func (v *ValueLog) pickLogToGC() (uint32, *os.File, error) {
	v.RLock()
	defer v.RUnlock()

	var targetFid uint32 = math.MaxUint32
	var targetFile *os.File
	for fid, f := range v.filesMap {
		if fid != v.activeFid && fid < targetFid {
			targetFid = fid
			targetFile = f
		}
	}
	if targetFile == nil {
		return 0, nil, fmt.Errorf("no valid vlog file for GC")
	}
	return targetFid, targetFile, nil
}

func (v *ValueLog) deleteLogFile(fid uint32) error {
	v.Lock()
	defer v.Unlock()
	if f, ok := v.filesMap[fid]; ok {
		f.Close()
		delete(v.filesMap, fid)
	}
	path := filepath.Join(v.dirPath, fmt.Sprintf("%06d.vlog", fid))
	return os.Remove(path)
}
