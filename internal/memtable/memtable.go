package memtable

type MemTable struct {
	Data map[string][]byte
	Size uint
}

func NewMemTable() *MemTable {
	return &MemTable{
		Data: make(map[string][]byte),
	}
}

func (m *MemTable) Put(key string, value []byte, opts ...uint) error {
	m.Data[key] = value

	if len(opts) > 0 && opts[0] > 0 {
		m.Size += opts[0]
	}

	return nil
}

func (m *MemTable) Get(key string) ([]byte, bool) {
	value, ok := m.Data[key]
	if !ok {
		return nil, false
	}
	return value, true
}
