package badger

import (
	"RaftKV/badger/table"
	"RaftKV/badger/y"
	"bytes"
	"fmt"
	// "os"
	"path/filepath"
	"sort"
	"time"
)

// RunCompaction 执行多路归并和磁盘文件的原子替换
func (lc *LevelsController) RunCompaction(cd *CompactDef) error {
	defer lc.cstatus.delete(*cd)

	if cd == nil || (len(cd.Top) == 0 && len(cd.Bot) == 0) {
		return nil
	}

	var iters []y.Iterator
	for _, t := range cd.Top {
		iters = append(iters, t.NewIterator(table.NONE))
	}
	for _, t := range cd.Bot {
		iters = append(iters, t.NewIterator(table.NONE))
	}
	if len(iters) == 0 {
		return nil
	}

	mergeIter := table.NewMergeIterator(iters, false)
	defer mergeIter.Close() // 保证退出时释放底层文件句柄

	// 2. 准备构建环境
	targetFileSize := lc.levelTargets().fileSz[cd.NextLevel]
	var newTables []*table.Table
	var currentBuilder *table.Builder
	var currentSstName string
	var err error

	createNewBuilder := func() (*table.Builder, error) {
		newFid := lc.db.manifest.AllocFileID()
		currentSstName = filepath.Join(lc.db.manifest.dirPath, fmt.Sprintf("%06d.sst", newFid))
		return table.NewBuilder(currentSstName)
	}

	// 🚨 修复核心：新增 lastKey 用于防止同 Key 切分，新增 discardTs 用于垃圾回收
	var lastKey []byte
	var hasValidVersionBelowThreshold bool
	discardTs := lc.db.CurrentTs()

	for mergeIter.Rewind(); mergeIter.Valid(); mergeIter.Next() {
		key := mergeIter.Key()
		vs := mergeIter.Value()

		userKey := y.ParseKey(key)
		ts := y.ParseTs(key)

		// 🚨 只有遇到全新的 Key 时，才允许检查是否达到容量并切分新文件！
		if !bytes.Equal(userKey, lastKey) {
			if currentBuilder != nil && currentBuilder.ReachedCapacity(targetFileSize) {
				if err := currentBuilder.Finish(); err != nil {
					return err
				}
				t, err := table.OpenTable(currentSstName)
				if err != nil {
					return err
				}
				newTables = append(newTables, t)
				currentBuilder = nil
			}
			lastKey = append(lastKey[:0], userKey...)
			hasValidVersionBelowThreshold = false
		}

		// MVCC 垃圾回收：丢弃已被覆盖的旧版本
		if ts <= discardTs {
			if hasValidVersionBelowThreshold {
				continue
			}
			hasValidVersionBelowThreshold = true
			if vs.Meta&y.BitDelete > 0 && cd.NextLevel == MaxLevels-1 {
				continue
			}
			if vs.ExpiresAt > 0 && uint64(time.Now().Unix()) > vs.ExpiresAt {
				if cd.NextLevel == MaxLevels-1 {
					continue 
				}
			}
		}

		if currentBuilder == nil {
			currentBuilder, err = createNewBuilder()
			if err != nil {
				return err
			}
		}

		if err := currentBuilder.Add(key, vs); err != nil {
			return err
		}
	}

	// 扫尾：处理残留数据
	if currentBuilder != nil {
		if err := currentBuilder.Finish(); err != nil {
			return err
		}
		t, err := table.OpenTable(currentSstName)
		if err != nil {
			return err
		}
		newTables = append(newTables, t)
	}

	// 4. 原子提交元数据
	return lc.commitCompaction(cd, newTables)
}

func (lc *LevelsController) commitCompaction(cd *CompactDef, newTables []*table.Table) error {
	lc.db.mu.Lock()
	defer lc.db.mu.Unlock()

	lc.db.levelTables[cd.ThisLevel] = removeTables(lc.db.levelTables[cd.ThisLevel], cd.Top)
	lc.db.levelTables[cd.NextLevel] = removeTables(lc.db.levelTables[cd.NextLevel], cd.Bot)

	lc.db.levelTables[cd.NextLevel] = append(lc.db.levelTables[cd.NextLevel], newTables...)

	sort.Slice(lc.db.levelTables[cd.NextLevel], func(i, j int) bool {
		return y.CompareKeys(lc.db.levelTables[cd.NextLevel][i].Smallest(), lc.db.levelTables[cd.NextLevel][j].Smallest()) < 0
	})

	thisLevelFids := getTableFIDs(lc.db.levelTables[cd.ThisLevel])
	if err := lc.db.manifest.ReplaceLevelFIDs(cd.ThisLevel, thisLevelFids); err != nil {
		return err
	}

	nextLevelFids := getTableFIDs(lc.db.levelTables[cd.NextLevel])
	if err := lc.db.manifest.ReplaceLevelFIDs(cd.NextLevel, nextLevelFids); err != nil {
		return err
	}

	for _, t := range cd.Top {
		t.MarkDelete()
	}
	for _, t := range cd.Bot {
		t.MarkDelete()
	}

	return nil
}
// removeTables 根据 FID 集合对源数组进行过滤删除
func removeTables(src []*table.Table, toRemove []*table.Table) []*table.Table {
	removeMap := make(map[uint32]struct{}, len(toRemove))
	for _, t := range toRemove {
		removeMap[getTableID(t)] = struct{}{}
	}
	var res []*table.Table
	for _, t := range src {
		if _, ok := removeMap[getTableID(t)]; !ok {
			res = append(res, t)
		}
	}
	return res
}

// getTableFIDs 提取数组中所有 Table 的 FID
func getTableFIDs(tables []*table.Table) []uint32 {
	fids := make([]uint32, 0, len(tables))
	for _, t := range tables {
		fids = append(fids, getTableID(t))
	}
	return fids
}