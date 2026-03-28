package skl

import (
	"RaftKV/badger/y"
)

type Iterator struct {
	list *SkipList
	n    *node // 当前指向的节点
}

var _ y.Iterator = (*Iterator)(nil)

// NewIterator 创建一个 SkipList 迭代器
func (sl *SkipList) NewIterator(reverse bool) y.Iterator {
	if reverse{

	}
	return &Iterator{
		list: sl,
		n:    nil,
	}
}

// Close 关闭迭代器 (内存表无需释放资源)
func (s *Iterator) Close() error {
	return nil
}

// Valid 判断迭代器是否有效
func (s *Iterator) Valid() bool {
	return s.n != nil
}

// Key 返回当前节点的 Key (Internal Key)
func (s *Iterator) Key() []byte {
	return s.n.entry.Key
}

func (s *Iterator) Value() y.ValueStruct {
	return y.ValueStruct{
		Meta:      s.n.entry.Meta,
		UserValue: s.n.entry.Value, // 直接拿原始值
		ExpiresAt: s.n.entry.ExpiresAt,
	}
}

// Next 移动到下一个节点
func (s *Iterator) Next() {
	// Level 0 是全量链表，直接走 Next[0]
	s.n = s.n.next[0].Load()
}

// Rewind 回到链表头部
func (s *Iterator) Rewind() {
	// header 是哨兵节点，header.next[0] 才是第一个真实数据
	s.n = s.list.header.next[0].Load()
}

// Seek 寻找第一个 >= key 的节点
func (s *Iterator) Seek(key []byte) {
	// 从最高层开始查找
	curr := s.list.header
	level := int(s.list.getHeight()) - 1

	for i := level; i >= 0; i-- {
		for {
			next := curr.next[i].Load()
			if next == nil {
				break
			}
			// 如果 next.Key < key，说明还得往右走
			if y.CompareKeys(next.entry.Key, key) < 0 {
				curr = next
			} else {
				// next.Key >= key，这一层找完了，往下一层钻
				break
			}
		}
	}
	
	// 循环结束后，curr 是“小于”key 的最后一个节点
	// 所以 curr.next[0] 就是“大于等于”key 的第一个节点
	s.n = curr.next[0].Load()
}