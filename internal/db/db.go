package db

import (
	"fmt"
	"log"

	"github.com/NirajNair/lsm-db/internal/errs"
	"github.com/NirajNair/lsm-db/internal/memtable"
	"github.com/NirajNair/lsm-db/internal/sstable"
)

type Db[K comparable, V any] struct {
	memTable        *memtable.MemTable[K, V]
	memTableSize    int
	maxMemTableSize int
	ssTables        []*sstable.SSTable[K, V]
	ssTableCounter  int
}

func NewDb[K comparable, V any](maxMemTableSize int) (*Db[K, V], error) {
	memTable := memtable.NewMemTable[K, V]()
	ssTables := make([]*sstable.SSTable[K, V], 0)

	return &Db[K, V]{
		memTable:        memTable,
		maxMemTableSize: maxMemTableSize,
		ssTables:        ssTables,
	}, nil
}

func (db *Db[K, V]) Put(key K, value V) error {
	if err := db.memTable.Put(key, value); err != nil {
		return err
	}
	db.memTableSize++

	if db.memTableSize >= db.maxMemTableSize {
		if err := db.flushMemTable(); err != nil {
			return err
		}
	}

	return nil
}

func (db *Db[K, V]) Get(key K) (V, error) {
	if val, ok := db.memTable.Get(key); ok {
		return val, nil
	}

	var zero V

	// Read the newest SSTable first
	for i := len(db.ssTables) - 1; i >= 0; i-- {
		ssTable := db.ssTables[i]
		val, err := ssTable.Get(key)
		if err != nil {
			if err == errs.ErrKeyDeleted {
				return zero, errs.ErrKeyNotFound
			}
			if err == errs.ErrKeyNotFound {
				continue
			}
			return zero, err
		}
		return val, err
	}

	return zero, fmt.Errorf("Key %v not found", key)
}

func (db *Db[K, V]) flushMemTable() error {
	ssTablePath := fmt.Sprintf("data-%d.sstable", db.ssTableCounter)
	log.Printf("Flushing MemTable to file %s", ssTablePath)

	ssTable, err := sstable.WriteSST(db.memTable, ssTablePath)
	if err != nil {
		return err
	}

	db.ssTables = append(db.ssTables, ssTable)
	db.ssTableCounter++
	db.memTable = memtable.NewMemTable[K, V]()
	db.memTableSize = 0

	log.Println("MemTable flushed")
	return nil
}
