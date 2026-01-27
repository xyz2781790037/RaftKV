package badger

import (
	"RaftKV/badger/skl"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

const MaxVLogFileSize = 64 * 1024 * 1024

type ValueLog struct {
	sync.RWMutex
	dirPath       string              // Vlog 文件存储目录
	activeFile    *os.File            // 当前正在写入的活跃文件
	activeFid     uint32              // 当前活跃文件的 ID
	filesMap      map[uint32]*os.File // 旧文件的句柄缓存 (Fid -> File)
	currentOffset int64               // 活跃文件的当前写入偏移量
	opt           Options             // 配置项
}
type Options struct {
	ValueLogFileSize int64
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
	if len(fids) == 0 {
		if err := v.createActiveFile(0); err != nil {
			return nil, err
		}
		return v, nil
	}
	maxFid := fids[len(fids)-1]
	for _, fid := range fids {
		path := v.fpath(fid)
		f, err := os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}
		v.filesMap[fid] = f
		if fid == maxFid {
			v.activeFile = f
			v.activeFid = fid
			stat, err := f.Stat()
			if err != nil {
				return nil, err
			}
			v.currentOffset = stat.Size()
		}
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
	path := v.fpath(fid)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	v.activeFile = f
	v.activeFid = fid
	v.currentOffset = 0
	v.filesMap[fid] = f

	return nil
}

// Write 写入 KV
func (v *ValueLog) Write(key, value []byte) (*ValuePointer, error) {
	v.Lock()
	defer v.Unlock()

	h := skl.EntryHeader{
		Meta:     0,
		KeyLen:   uint32(len(key)),
		ValueLen: uint32(len(value)),
	}
	entrySize := int64(16 + len(key) + len(value))
	if v.currentOffset+entrySize > v.opt.ValueLogFileSize {
		if err := v.rotate(); err != nil {
			return nil, err
		}
	}
	var headerBuf [16]byte
	headerLen := h.Encode(headerBuf[:])
	ptr := &ValuePointer{
		Fid:    v.activeFid,
		Offset: uint64(v.currentOffset),
		Len:    uint32(len(value)),
	}
	if _, err := v.activeFile.Write(headerBuf[:headerLen]); err != nil {
		return nil, err
	}
	if _, err := v.activeFile.Write(key); err != nil {
		return nil, err
	}
	if _, err := v.activeFile.Write(value); err != nil {
		return nil, err
	}
	v.currentOffset += int64(headerLen + len(key) + len(value))

	return ptr, nil
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
	defer v.RUnlock()
	f,ok := v.filesMap[vp.Fid]
	if !ok{
		return nil, fmt.Errorf("vlog file not found: fid=%d", vp.Fid)
	}
	var headerBuf [16]byte
	if _, err := f.ReadAt(headerBuf[:], int64(vp.Offset)); err != nil {
		return nil, err
	}
	h := skl.EntryHeader{}
	headerLen := h.Decode(headerBuf[:])
	valOffset := int64(vp.Offset) + int64(headerLen) + int64(h.KeyLen)
	val := make([]byte,vp.Len)
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
	return &ValuePointer{
		Fid:    binary.BigEndian.Uint32(data[0:4]),
		Len:    binary.BigEndian.Uint32(data[4:8]),
		Offset: binary.BigEndian.Uint64(data[8:16]),
	}
}
func (v *ValueLog) Close() error {
	v.Lock()
	defer v.Unlock()

	for _, f := range v.filesMap {
		_ = f.Close()
	}
	return nil
}

// fpath 根据 fid 生成文件路径 (000001.vlog)
func (v *ValueLog) fpath(fid uint32) string {
	return filepath.Join(v.dirPath, fmt.Sprintf("%06d.vlog", fid))
}
