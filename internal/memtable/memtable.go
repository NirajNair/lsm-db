package memtable

import "github.com/huandu/skiplist"

type MemTable struct {
	list *skiplist.SkipList
	Size uint
}

func NewMemTable() *MemTable {
	return &MemTable{
		list: skiplist.New(skiplist.Bytes),
	}
}
func (m *MemTable) Put(key, value []byte, opts ...uint) error {
	m.list.Set(key, value)

	if len(opts) > 0 && opts[0] > 0 {
		m.Size += opts[0]
	}

	return nil
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	val, ok := m.list.GetValue(key)
	if !ok {
		return nil, false
	}
	return val.([]byte), true
}

func (m *MemTable) ForEach(fn func(key, value []byte) error) error {
	for elem := m.list.Front(); elem != nil; elem = elem.Next() {
		if err := fn(elem.Key().([]byte), elem.Value.([]byte)); err != nil {
			return err
		}
	}
	return nil
}
