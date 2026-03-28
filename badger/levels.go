package badger

import (
	"RaftKV/badger/table"
	"RaftKV/badger/y"
	// "RaftKV/badger/y"
	"bytes"
	"fmt"
	"path/filepath"

	"sync"
)

type keyRange struct {
	left  []byte
	right []byte
	inf   bool
}

func (r keyRange) isEmpty() bool {
	return len(r.left) == 0 && len(r.right) == 0 && !r.inf
}

// extend 将另一个 keyRange 合并到当前范围中
func (r keyRange) extend(kr keyRange) {
	if kr.isEmpty() {
		return
	}
	if r.isEmpty() {
		r = kr
		return
	}
	if y.CompareKeys(r.left, kr.left) > 0 || len(r.left) == 0 {
		r.left = kr.left
	}
	if y.CompareKeys(r.right, kr.right) < 0 || len(r.right) == 0 {
		r.right = kr.right
	}
	if kr.inf {
		r.inf = true
	}
}
func (r keyRange) overlapsWith(dst keyRange) bool {
	if r.isEmpty() || dst.isEmpty() {
		return r.isEmpty() && dst.isEmpty()
	}
	if r.inf || dst.inf {
		return true
	}
	if y.CompareKeys(r.left, dst.right) > 0 {
		return false
	}
	if y.CompareKeys(r.right, dst.left) < 0 {
		return false
	}
	return true
}

func getKeyRange(tables ...*table.Table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	smallest := tables[0].Smallest()
	biggest := tables[0].Biggest()
	for i := 1; i < len(tables); i++ {
		if y.CompareKeys(tables[i].Smallest(), smallest) < 0 {
			smallest = tables[i].Smallest()
		}
		if y.CompareKeys(tables[i].Biggest(), biggest) > 0 {
			biggest = tables[i].Biggest()
		}
	}
	return keyRange{
		left:  smallest,
		right: biggest,
	}
}

type levelCompactStatus struct {
	ranges []keyRange
}

func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}

type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint32]struct{}
}

func newCompactStatus(maxLevels int) *compactStatus {
	cs := &compactStatus{
		levels: make([]*levelCompactStatus, maxLevels),
		tables: make(map[uint32]struct{}),
	}
	for i := 0; i < maxLevels; i++ {
		cs.levels[i] = &levelCompactStatus{}
	}
	return cs
}
func getTableID(t *table.Table) uint32 {
	baseName := filepath.Base(t.Fd().Name())
	var fid uint32
	fmt.Sscanf(baseName, "%06d.sst", &fid)
	return fid
}
func (cs *compactStatus) compareAndAdd(cd CompactDef) bool {
	cs.Lock()
	defer cs.Unlock()
	thisLevel := cs.levels[cd.ThisLevel]
	nextLevel := cs.levels[cd.NextLevel]
	if thisLevel.overlapsWith(cd.ThisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.NextRange) {
		return false
	}
	thisLevel.ranges = append(thisLevel.ranges, cd.ThisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.NextRange)
	for _, t := range append(cd.Top, cd.Bot...) {
		cs.tables[getTableID(t)] = struct{}{}
	}
	return true
}
func (cs *compactStatus) delete(cd CompactDef) {
	cs.Lock()
	defer cs.Unlock()

	removeRange := func(lcs *levelCompactStatus, target keyRange) {
		var final []keyRange
		for _, r := range lcs.ranges {
			if !bytes.Equal(r.left, target.left) || !bytes.Equal(r.right, target.right) {
				final = append(final, r)
			}
		}
		lcs.ranges = final
	}
	removeRange(cs.levels[cd.ThisLevel], cd.ThisRange)
	removeRange(cs.levels[cd.NextLevel], cd.NextRange)

	for _, t := range append(cd.Top, cd.Bot...) {
		delete(cs.tables, getTableID(t))
	}
}

type CompactDef struct {
	ThisLevel int
	NextLevel int
	Top       []*table.Table
	Bot       []*table.Table
	ThisRange keyRange
	NextRange keyRange
}
type LevelsController struct {
	db      *DB
	cstatus *compactStatus
}

func NewLevelsController(db *DB) *LevelsController {
	return &LevelsController{
		db:      db,
		cstatus: newCompactStatus(MaxLevels),
	}
}

type targets struct {
	baseLevel int     // 动态基础层：L0 层的数据应该被压缩到哪一层
	targetSz  []int64 // 容量蓝图
	fileSz    []int64 // 切块蓝图
}

func (lc *LevelsController) levelTargets() targets {
	t := targets{
		targetSz: make([]int64, MaxLevels),
		fileSz:   make([]int64, MaxLevels),
	}
	BaseLevelSize := int64(10 * 1024 * 1024)
	LevelSizeMultiplier := int64(10)
	BaseTableSize := int64(4 * 1024 * 1024)
	dbSize := int64(0)
	lc.db.mu.RLock()
	for _, table := range lc.db.levelTables[MaxLevels-1] {
		dbSize += table.Size()
	}
	lc.db.mu.RUnlock()
	for i := MaxLevels - 1; i > 0; i-- {
		target := dbSize
		if target < BaseLevelSize {
			target = BaseLevelSize
		}
		t.targetSz[i] = target
		if t.baseLevel == 0 && target <= BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= LevelSizeMultiplier
	}
	tsz := BaseTableSize
	for i := 0; i < MaxLevels; i++ {
		if i == 0 {
			t.fileSz[i] = BaseTableSize
		} else if i < t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			tsz *= 2
			t.fileSz[i] = tsz
		}
	}
	for i := t.baseLevel + 1; i < MaxLevels-1; i++ {
		lc.db.mu.RLock()
		size := int64(0)
		for _, tb := range lc.db.levelTables[i] {
			size += tb.Size()
		}
		lc.db.mu.RUnlock()
		if size > 0 {
			break
		}
		t.baseLevel = i
	}
	return t
}
func (lc *LevelsController) PickCompactionTask() *CompactDef {
	targets := lc.levelTargets()
	lc.db.mu.RLock()
	l0Count := len(lc.db.levelTables[0])
	lc.db.mu.RUnlock()

	if l0Count >= 4 {
		cd := lc.fillTablesL0(targets.baseLevel)
		if cd != nil {
			return cd
		}
	}
	for i := 1; i < MaxLevels-1; i++ {
		lc.db.mu.RLock()
		currentSize := int64(0)
		for _, t := range lc.db.levelTables[i] {
			currentSize += t.Size()
		}
		lc.db.mu.RUnlock()
		if currentSize > targets.targetSz[i] {
			cd := lc.fillTablesLi(i, i+1)
			if cd != nil {
				return cd
			}
		}
	}
	return nil
}
func (lc *LevelsController) fillTablesL0(baseLevel int) *CompactDef {
	cd := &CompactDef{
		ThisLevel: 0,
		NextLevel: baseLevel,
	}
	lc.db.mu.RLock()
	if len(lc.db.levelTables[0]) == 0 {
		lc.db.mu.RUnlock()
		return nil
	}
	cd.Top = append(cd.Top, lc.db.levelTables[0]...)
	cd.ThisRange = getKeyRange(cd.Top...)

	cd.Bot = lc.getOverlappingTables(baseLevel, cd.ThisRange)
	if len(cd.Bot) == 0 {
		cd.NextRange = cd.ThisRange
	} else {
		botRange := getKeyRange(cd.Bot...)
		cd.NextRange = keyRange{
			left:  minKey(cd.ThisRange.left, botRange.left),
			right: maxKey(cd.ThisRange.right, botRange.right),
		}
	}
	lc.db.mu.RUnlock()
	if lc.cstatus.compareAndAdd(*cd) {
		return cd
	}
	return nil
}
func (lc *LevelsController) fillTablesLi(thisLevel, nextLevel int) *CompactDef {
	cd := &CompactDef{
		ThisLevel: thisLevel,
		NextLevel: nextLevel,
	}
	lc.db.mu.RLock()
	tables := lc.db.levelTables[thisLevel]
	if len(tables) == 0 {
		lc.db.mu.RUnlock()
		return nil
	}
	for _, t := range tables {
		cd.ThisRange = getKeyRange(t)
		cd.Top = []*table.Table{t}

		cd.Bot = lc.getOverlappingTables(nextLevel, cd.ThisRange)
		if len(cd.Bot) == 0 {
			cd.NextRange = cd.ThisRange
		} else {
			botRange := getKeyRange(cd.Bot...)
			cd.NextRange = keyRange{
				left:  minKey(cd.ThisRange.left, botRange.left),
				right: maxKey(cd.ThisRange.right, botRange.right),
			}
		}
		lc.db.mu.RUnlock()
		if lc.cstatus.compareAndAdd(*cd) {
			return cd
		}
		lc.db.mu.RLock()
	}
	lc.db.mu.RUnlock()
	return nil
}

func (lc *LevelsController) getOverlappingTables(level int, kr keyRange) []*table.Table {
	tables := lc.db.levelTables[level]
	if len(tables) == 0 {
		return nil
	}
	var out []*table.Table
	for _, t := range tables {
		tkr := getKeyRange(t)
		if tkr.overlapsWith(kr) {
			out = append(out, t)
		}
	}
	return out
}

func minKey(a, b []byte) []byte {
	if y.CompareKeys(a, b) <= 0 {
		return a
	}
	return b
}
func maxKey(a, b []byte) []byte {
	if y.CompareKeys(a, b) >= 0 {
		return a
	}
	return b
}