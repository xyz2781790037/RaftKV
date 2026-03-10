package badger

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)
const ManifestFilename = "MANIFEST"
const MaxLevels = 7
type ManifestState struct {
	NextFileID uint32   `json:"next_file_id"` // 下一个可用的文件ID
	Levels  [][]uint32 `json:"levels"`
	MaxTs      uint64   `json:"max_ts"`
}
type Manifest struct {
	mu      sync.Mutex
	dirPath string
	state   ManifestState
}
func OpenManifest(dir string) (*Manifest, error){
	m := &Manifest{
		dirPath: dir,
		state: ManifestState{
			NextFileID: 0,
			Levels:  make([][]uint32, MaxLevels),
			MaxTs: 0,
		},
	}
	for i := 0; i < MaxLevels; i++ {
		m.state.Levels[i] = make([]uint32, 0)
	}
	path := filepath.Join(dir, ManifestFilename)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return m, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &m.state); err != nil {
			return nil, fmt.Errorf("corrupted manifest: %v", err)
		}
		if len(m.state.Levels) < MaxLevels {
			newLevels := make([][]uint32, MaxLevels)
			copy(newLevels, m.state.Levels)
			for i := len(m.state.Levels); i < MaxLevels; i++ {
				newLevels[i] = make([]uint32, 0)
			}
			m.state.Levels = newLevels
		}
	}
	return m,nil
}
func (m *Manifest) AddTableToL0(newFid uint32) error{
	m.mu.Lock()
	defer m.mu.Unlock()
	newL0 := make([]uint32,0,len(m.state.Levels[0]) + 1)
	newL0 = append(newL0, newFid)
	newL0 = append(newL0, m.state.Levels[0]...)
	m.state.Levels[0] = newL0
	return m.rewrite()
}
func (m *Manifest) ReplaceLevelFIDs(level int, fids []uint32) error{
	if level < 0 || level >= MaxLevels{
		return fmt.Errorf("invalid level: %d", level)
	}
	m.state.Levels[level] = fids
	return m.rewrite()
}
func (m *Manifest) RevertToSnapshot() error{
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state.Levels = make([][]uint32,MaxLevels)
	for i := 0;i < MaxLevels;i++{
		m.state.Levels[i] = make([]uint32, 0)
	}
	m.state.NextFileID = 0
	return m.rewrite()
}
func (m *Manifest) GetState() (uint32, [][]uint32){
	m.mu.Lock()
	defer m.mu.Unlock()
	levels := make([][]uint32, MaxLevels)
	for i := 0; i < MaxLevels; i++ {
		levels[i] = make([]uint32, len(m.state.Levels[i]))
		copy(levels[i], m.state.Levels[i])
	}
	return m.state.NextFileID,levels
}
func (m *Manifest) rewrite() error{
	data, err := json.Marshal(m.state)
	if err != nil {
		return err
	}
	tmpPath := filepath.Join(m.dirPath, ManifestFilename+".tmp")
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	finalPath := filepath.Join(m.dirPath, ManifestFilename)
	return os.Rename(tmpPath, finalPath)
}
// AllocFileID 安全地分配并推进下一个文件 ID
func (m *Manifest) AllocFileID() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	fid := m.state.NextFileID
	m.state.NextFileID++
	m.rewrite() 
	return fid
}

func (m *Manifest) SetMaxTs(ts uint64) error{
	m.mu.Lock()
    defer m.mu.Unlock()
	if ts > m.state.MaxTs {
        m.state.MaxTs = ts
        return m.rewrite()
    }
	return nil
}
func (m *Manifest) GetMaxTs() uint64 {
	m.mu.Lock()
    defer m.mu.Unlock()
    return m.state.MaxTs
}