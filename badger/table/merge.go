package table

import (
	"RaftKV/badger/y"
	"bytes"
)
var _ y.Iterator = (*MergeIterator)(nil)
type MergeIterator struct {
	left    node
	right   node
	small   *node  // 指向 &left 或 &right
	curKey  []byte // 仅用于去重判断 (保存上一个 Key)
	reverse bool   // 是否反向 (预留字段，目前默认 false)
}
type node struct {
	valid bool
	key   []byte
	iter  y.Iterator
}

func (n *node) setIterator(iter y.Iterator) {
	n.iter = iter
}

func (n *node) setKey() {
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
func NewMergeIterator(iters []y.Iterator,reverse bool) y.Iterator {
	switch len(iters) {
	case 0:
		return nil
	case 1:
		return iters[0]
	case 2:
		mi := &MergeIterator{
			reverse: reverse,
		}
		mi.left.setIterator(iters[0])
		mi.right.setIterator(iters[1])
		mi.small = &mi.left 
		mi.Rewind()
		return mi
	default:
		mid := len(iters) / 2
		return NewMergeIterator(
			[]y.Iterator{
			NewMergeIterator(iters[:mid],reverse),
			NewMergeIterator(iters[mid:],reverse),
		},reverse)
	}
}

func (mi *MergeIterator) fix() {
	if !mi.left.valid {
		if !mi.right.valid {
			mi.small = &mi.left
			return
		}
		mi.small = &mi.right
		return
	}
	if !mi.right.valid {
		mi.small = &mi.left
		return
	}
	cmp := y.CompareKeys(mi.left.key, mi.right.key)
	if cmp <= 0 {
		mi.small = &mi.left
	} else {
		mi.small = &mi.right
	}
}

func (mi *MergeIterator) Rewind() {
	mi.left.rewind()
	mi.right.rewind()
	mi.fix()
	mi.setCurrent()
}

func (mi *MergeIterator) Seek(key []byte) {
	mi.left.seek(key)
	mi.right.seek(key)
	mi.fix()
	mi.setCurrent()
}

func (mi *MergeIterator) Next() {
	for mi.Valid() {
		if !bytes.Equal(mi.small.key, mi.curKey) {
			break
		}
		mi.small.next()
		mi.fix()
	}
	mi.setCurrent()
}
func (mi *MergeIterator) setCurrent() {
	if !mi.small.valid {
		mi.curKey = nil
		return
	}
	mi.curKey = append(mi.curKey[:0], mi.small.key...)
}

func (mi *MergeIterator) Valid() bool {
	return mi.small.valid
}
func (mi *MergeIterator) Key() []byte {
	return mi.small.key
}

func (mi *MergeIterator) Value() y.ValueStruct {
	return mi.small.iter.Value()
}
func (mi *MergeIterator) Close() error {
	err1 := mi.left.iter.Close()
	err2 := mi.right.iter.Close()
	if err1 != nil {
		return err1
	}
	return err2
}