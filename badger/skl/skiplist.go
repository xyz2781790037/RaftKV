package skl

import (
	"bytes"
	"math/rand"
	"sync/atomic"
)

const (
	maxHeight      = 20
	heightIncrease = 0.25
)

type node struct {
	entry *Entry
	next    []atomic.Pointer[node] // 每一層的後繼節點指針
}
type SkipList struct {
	height atomic.Int32
	header *node // 头节点
	length atomic.Int64
	// randSource rand.Source
}
func NewNode(entry *Entry,height int)*node{
	return &node{
		entry: entry,
		next: make([]atomic.Pointer[node], height),
	}
}
func NewSkiplist() *SkipList {
	head := NewNode(nil,maxHeight)
	s := &SkipList{header: head}
	s.height.Store(1)
	// s.length.Store(1)
	return s
}
func (sl *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && rand.Float64() < heightIncrease {
		h++
	}
	return h
}
func (sl *SkipList) getHeight() int32 {
	return sl.height.Load()
}
func (sl *SkipList) findSpliceForLevel(key []byte, curr *node, level int) (*node, *node){
	for{
		next := curr.next[level].Load()
		if next == nil{
			break
		}
		cmp := bytes.Compare(next.entry.Key,key)
		if cmp < 0{
			curr = next
		}else{
			break
		}
	}
	return curr,curr.next[level].Load()
}
func (sl *SkipList) Put(entry *Entry) {
	key := entry.Key
	prev := make([]*node, maxHeight)
	next := make([]*node, maxHeight)
	listHeight := sl.getHeight()
	curr := sl.header
	for i := int(listHeight) - 1;i >= 0;i--{
		curr,next[i] = sl.findSpliceForLevel(key,curr,i)
		prev[i] = curr
	}
	if next[0] != nil && bytes.Equal(next[0].entry.Key,key){
		next[0].entry = entry
		return
	}
	newHeight := sl.randomHeight()
	newNode := NewNode(entry,newHeight)
	for i := 0;i < newHeight;i++{
		if i >= int(listHeight) {
            prev[i] = sl.header
            next[i] = nil
        }
		newNode.next[i].Store(next[i])
	}
	for i := 0;i < newHeight;i++{
		prev[i].next[i].Store(newNode)
	}
	oldHeight := sl.height.Load()
	if int32(newHeight) > oldHeight {
		sl.height.Store(int32(newHeight))
	}
	sl.length.Add(1)
}
func (sl *SkipList) Get(key []byte) *Entry {
	curr := sl.header
	for i := int(sl.getHeight()) - 1; i >= 0; i-- {
		for {
			next := curr.next[i].Load()
			if next == nil {
				break
			}
			cmp := bytes.Compare(next.entry.Key, key)
			if cmp < 0 {
				curr = next
			} else if cmp == 0 {
				return next.entry
			} else {
				break
			}
		}
	}
	next := curr.next[0].Load()
	if next != nil && bytes.Equal(next.entry.Key, key) {
		return next.entry
	}
	return nil
}
func (sl *SkipList) Scan(callback func(e *Entry) bool) {
	node := sl.header.next[0].Load()
	for node != nil {
		if !callback(node.entry) {
			break
		}
		node = node.next[0].Load()
	}
}
func (sl *SkipList) SklSize()int{
	return int(sl.length.Load())
}