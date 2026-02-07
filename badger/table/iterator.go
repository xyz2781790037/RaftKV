package table

import (
	"RaftKV/badger/y"
	"bytes"
)
var _ y.Iterator = (*MergeIterator)(nil)
type MergeIterator struct {
	left    *node
	right   *node
	small   *node // 指向当前 Key 较小的那个 node (left 或 right)
	curKey  []byte
}

// node 包装底层迭代器，缓存当前状态
type node struct {
	valid bool
	key   []byte
	iter  y.Iterator // 这里可以是 TableIterator，也可以是另一个 MergeIterator
}
func (n *node) setKey(){
	n.valid = n.iter.Valid()
	if n.valid {
		n.key = n.iter.Key()
	} else {
		n.key = nil
	}
}
func (n *node) next() {
	n.iter.Next()
	n.setKey()
}
func (n *node) rewind() {
	n.iter.Rewind()
	n.setKey()
}
func (n *node) seek(key []byte) {
	n.iter.Seek(key)
	n.setKey()
}
func NewMergeIterator(iters []y.Iterator) y.Iterator{
	switch len(iters){
	case 0:
		return &MergeIterator{}
	case 1:
		return iters[0]
	case 2:
		return newMergeIterator(iters[0],iters[1])
	default:
		mid := len(iters) / 2
		return newMergeIterator(
			NewMergeIterator(iters[:mid]),
			NewMergeIterator(iters[mid:]),
		)
	}
}
func newMergeIterator(lo, ro y.Iterator) *MergeIterator {
	mi := &MergeIterator{
		left:  &node{iter: lo},
		right: &node{iter: ro},
	}
	mi.Rewind()
	return mi
}
func (mi *MergeIterator) fix(){
	if !mi.left.valid{
		if !mi.right.valid{
			mi.small = nil
			return
		}
		mi.small = mi.right
		return
	}
	if !mi.right.valid {
		mi.small = mi.left
		return
	}
	if bytes.Compare(mi.left.key, mi.right.key) <= 0 {
		mi.small = mi.left
	} else {
		mi.small = mi.right
	}
}
func (mi *MergeIterator) Rewind(){
	if mi.left == nil || mi.right == nil {
		return
	}
	mi.left.rewind()
	mi.right.rewind()
	mi.fix()
	mi.setCurrent()
}
func (mi *MergeIterator) Seek(key []byte){
	mi.left.seek(key)
	mi.right.seek(key)
	mi.fix()
	mi.setCurrent()
}
func (mi *MergeIterator) Valid() bool{
	return mi.small != nil && mi.small.valid
}
func (mi *MergeIterator) Key() []byte{
	return mi.curKey
}
func (mi *MergeIterator) Value() y.ValueStruct{
	if mi.small == nil {
		return y.ValueStruct{}
	}
	return mi.small.iter.Value()
}
func (mi *MergeIterator) Close() error{
	err1 := mi.left.iter.Close()
	err2 := mi.right.iter.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
func (mi *MergeIterator) Next(){
	for mi.Valid(){
		if !bytes.Equal(mi.small.key,mi.curKey){
			break
		}
		mi.small.next()
		mi.fix()
	}
	mi.setCurrent()
}
func (mi *MergeIterator) setCurrent() {
	if mi.small == nil || !mi.small.valid {
		mi.curKey = nil
		return
	}
	mi.curKey = append(mi.curKey[:0], mi.small.key...)
}