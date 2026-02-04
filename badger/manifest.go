package badger

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)
const ManifestFilename = "MANIFEST"
type ManifestState struct {
	NextFileID uint32   `json:"next_file_id"` // 下一个可用的文件ID
	TableFIDs  []uint32 `json:"table_fids"`   // 当前所有有效的 SST 文件 ID
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
			TableFIDs:  make([]uint32, 0),
		},
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
	}
	return m,nil
}
func (m *Manifest) AddTableEntry(newFid uint32, nextFid uint32) error{
	m.mu.Lock()
	defer m.mu.Unlock()
	newTables := make([]uint32, 0, len(m.state.TableFIDs)+1)
	newTables = append(newTables, newFid)
	newTables = append(newTables, m.state.TableFIDs...)

	m.state.TableFIDs = newTables
	m.state.NextFileID = nextFid
	return m.rewrite()
}
func (m *Manifest) RevertToSnapshot() error{
	m.mu.Lock()
	defer m.mu.Unlock()

	m.state.TableFIDs = []uint32{}
	m.state.NextFileID = 0
	return m.rewrite()
}
func (m *Manifest) GetState() (uint32, []uint32){
	m.mu.Lock()
	defer m.mu.Unlock()
	tables := make([]uint32,len(m.state.TableFIDs))
	copy(tables,m.state.TableFIDs)
	
	return m.state.NextFileID,tables
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