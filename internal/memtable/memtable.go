package memtable

type MemTable[K comparable, V any] struct {
	data map[K]V
}

func NewMemTable[K comparable, V any]() *MemTable[K, V] {
	return &MemTable[K, V]{
		data: make(map[K]V),
	}
}

func (m *MemTable[K, V]) Put(key K, value V) error {
	m.data[key] = value
	return nil
}

func (m *MemTable[K, V]) Get(key K) (V, bool) {
	value, ok := m.data[key]
	var zero V
	if !ok {
		return zero, false
	}
	return value, true
}
