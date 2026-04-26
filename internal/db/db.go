package db

import (
	"fmt"

	"github.com/NirajNair/lsm-db/internal/memtable"
)

type Db[K comparable, V any] struct {
	memTable        *memtable.MemTable[K, V]
	memTableSize    int
	maxMemTableSize int
}

func NewDb[K comparable, V any]() (*Db[K, V], error) {
	memTable := memtable.NewMemTable[K, V]()
	return &Db[K, V]{
		memTable: memTable,
	}, nil
}

func (db *Db[K, V]) Put(key K, value V) error {
	db.memTable.Put(key, value)

	if db.memTableSize >= db.maxMemTableSize {
		// flush MemTable to SSTable
	}

	return nil
}

func (db *Db[K, V]) Get(key K) (V, error) {
	if val, ok := db.memTable.Get(key); ok {
		return val, nil
	}
	var zero V
	return zero, fmt.Errorf("Key %v not found", key)
}
