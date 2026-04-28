package db

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/NirajNair/lsm-db/internal/errs"
	"github.com/NirajNair/lsm-db/internal/fsutil"
	"github.com/NirajNair/lsm-db/internal/memtable"
	"github.com/NirajNair/lsm-db/internal/sstable"
	"github.com/NirajNair/lsm-db/internal/wal"
)

const (
	sstDir      = "data/sst"
	walDir      = "data/wal"
	walFilePath = walDir + "/db.wal"
)

type DB[K comparable, V any] struct {
	memTable        *memtable.MemTable[K, V]
	memTableSize    int
	maxMemTableSize int
	ssTables        []*sstable.SSTable[K, V]
	ssTableCounter  int
	wal             *wal.WAL[K, V]
	walPath         string
}

func NewDB[K comparable, V any](maxMemTableSize int) (*DB[K, V], error) {
	if err := cleanupTmpFiles(sstDir); err != nil {
		return nil, err
	}

	memTable, err := wal.ReplayWAL[K, V](walFilePath)
	if err != nil {
		return nil, err
	}

	// Open WAL file to append
	wal, err := wal.NewWAL[K, V](walFilePath)
	if err != nil {
		return nil, err
	}

	return &DB[K, V]{
		memTable:        memTable,
		maxMemTableSize: maxMemTableSize,
		ssTables:        make([]*sstable.SSTable[K, V], 0),
		wal:             wal,
		walPath:         walFilePath,
	}, nil
}

func (db *DB[K, V]) Put(key K, value V) error {
	if err := db.wal.Write(key, value); err != nil {
		return err
	}
	log.Printf("Written {'%v': '%v} to WAL", key, value)

	if err := db.memTable.Put(key, value); err != nil {
		return err
	}
	log.Printf("Added {'%v': '%v} to MemTable", key, value)
	db.memTableSize++

	if db.memTableSize >= db.maxMemTableSize {
		if err := db.flushMemTable(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB[K, V]) Update(key K, value V) error {
	if err := db.Put(key, value); err != nil {
		return err
	}
	log.Printf("Key %v updated!", key)

	return nil
}

func (db *DB[K, V]) Get(key K) (V, error) {
	var zero V

	if val, ok := db.memTable.Get(key); ok {
		if any(val).(string) == sstable.TOMBSTONE {
			return zero, errs.ErrKeyDeleted
		}
		return val, nil
	}

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

func (db *DB[K, V]) Delete(key K) error {
	if err := db.Put(key, any(sstable.TOMBSTONE).(V)); err != nil {
		return err
	}
	log.Printf("Key %v deleted!", key)

	return nil
}

func (db *DB[K, V]) rotateWAL() error {
	if err := db.wal.Close(); err != nil {
		return err
	}

	rotatedPath := fmt.Sprintf("%s/data-%02d.wal", walDir, db.ssTableCounter)
	if err := os.Rename(db.walPath, rotatedPath); err != nil {
		return err
	}

	if err := fsutil.SyncDir(db.walPath); err != nil {
		return err
	}

	newWAL, err := wal.NewWAL[K, V](db.walPath)
	if err != nil {
		return err
	}

	db.wal = newWAL
	return nil
}

func (db *DB[K, V]) flushMemTable() error {
	ssTablePath := fmt.Sprintf("%s/data-%d.sstable", sstDir, db.ssTableCounter)
	if err := os.MkdirAll(filepath.Dir(ssTablePath), 0755); err != nil {
		return err
	}

	if err := fsutil.SyncDir(ssTablePath); err != nil {
		return err
	}

	log.Printf("Flushing MemTable to file %s", ssTablePath)
	ssTable, err := sstable.WriteSST(db.memTable, ssTablePath)
	if err != nil {
		return err
	}
	log.Println("Successfully flushed MemTable!")

	log.Println("Rotating WAL..")
	if err := db.rotateWAL(); err != nil {
		return err
	}
	log.Println("Successfully rotated WAL!")

	db.ssTables = append(db.ssTables, ssTable)
	db.ssTableCounter++
	db.memTable = memtable.NewMemTable[K, V]()
	db.memTableSize = 0

	return nil
}

func cleanupTmpFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".tmp" {
			if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}
