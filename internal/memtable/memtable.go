package memtable

type MemTable[K comparable, V any] struct {
	Data map[K]V
	Size uint
}

func NewMemTable[K comparable, V any]() *MemTable[K, V] {
	return &MemTable[K, V]{
		Data: make(map[K]V),
	}
}

func (m *MemTable[K, V]) Put(key K, value V, opts ...uint) error {
	m.Data[key] = value

	if len(opts) > 0 && opts[0] > 0 {
		m.Size += opts[0]
	}

	return nil
}

func (m *MemTable[K, V]) Get(key K) (V, bool) {
	value, ok := m.Data[key]
	var zero V
	if !ok {
		return zero, false
	}
	return value, true
}
