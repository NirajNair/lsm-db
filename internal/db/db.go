package db

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/NirajNair/lsm-db/internal/errs"
	"github.com/NirajNair/lsm-db/internal/manifest"
	"github.com/NirajNair/lsm-db/internal/memtable"
	"github.com/NirajNair/lsm-db/internal/sstable"
	"github.com/NirajNair/lsm-db/internal/utils"
	"github.com/NirajNair/lsm-db/internal/wal"
)

const (
	sstDir       = "data/sst"
	walDir       = "data/wal"
	walFilePath  = walDir + "/db.wal"
	manifestPath = "data/MANIFEST"
)

type DB struct {
	mu              sync.RWMutex
	memTable        *memtable.MemTable
	rotatedMemTable *memtable.MemTable
	maxMemTableSize uint
	wal             *wal.WAL
	walPath         string
	manifest        *manifest.Manifest
	flushDone       sync.Cond
	flushErr        error
	flushWg         sync.WaitGroup
	closed          bool
}

func NewDB(maxMemTableSize uint) (*DB, error) {
	if err := cleanupTmpFiles(sstDir); err != nil {
		return nil, err
	}

	// Read manifest first — it lists all SSTables
	m, err := manifest.ReadManifest(manifestPath)
	if err != nil {
		return nil, err
	}

	// Clean up stale rotated WALs (their data is already in SSTables)
	if err := cleanupStaleWALs(m); err != nil {
		return nil, err
	}

	memTable, err := wal.ReplayWAL(walFilePath)
	if err != nil {
		return nil, err
	}

	var rotatedMemTable *memtable.MemTable
	if len(m.RotatedWALs) != 0 {
		rotatedMemTable, err = wal.ReplayWAL(m.RotatedWALs[0].Path)
		if err != nil {
			return nil, err
		}
	}

	walFile, err := wal.NewWAL(walFilePath)
	if err != nil {
		return nil, err
	}

	return &DB{
		memTable:        memTable,
		rotatedMemTable: rotatedMemTable,
		maxMemTableSize: maxMemTableSize,
		wal:             walFile,
		walPath:         walFilePath,
		manifest:        m,
	}, nil
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for db.memTable.Size >= db.maxMemTableSize {
		if db.rotatedMemTable != nil {
			// Unlocks mutex and waits for backgound flush to finish
			db.flushDone.Wait()
			if db.flushErr != nil {
				return fmt.Errorf("DB flush error: %w", db.flushErr)
			}
			continue
		}

		if err := db.rotateMemTable(); err != nil {
			return err
		}

		db.memTable = memtable.NewMemTable()

		// Add the background job to WaitGroup to support graceful shutdown
		db.flushWg.Add(1)
		go func() {
			defer db.flushWg.Done()
			db.flushRotatedMemTableAndCleanup()
		}()
	}

	size, err := db.wal.Write(key, value)
	if err != nil {
		return err
	}
	log.Printf("Written key to WAL (encoded size: %d bytes)", size)

	if err := db.memTable.Put(key, value, uint(size)); err != nil {
		return err
	}
	log.Printf("Added key to MemTable")

	return nil
}

func (db *DB) Update(key, value []byte) error {
	return db.Put(key, value)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if val, ok := db.memTable.Get(key); ok {
		if utils.IsTombstone(val) {
			return nil, errs.ErrKeyDeleted
		}
		return val, nil
	}

	if db.rotatedMemTable != nil {
		if val, ok := db.rotatedMemTable.Get(key); ok {
			if utils.IsTombstone(val) {
				return nil, errs.ErrKeyDeleted
			}
			return val, nil
		}
	}

	// Search SSTables via manifest, level by level.
	// Keys are raw bytes — the application is responsible for encoding
	keyBytes := key

	for _, level := range db.manifest.Levels {
		if level.Level == manifest.LevelZero {
			// L0: check newest files first, skip files whose range
			// doesn't contain the key.
			for i := len(level.Files) - 1; i >= 0; i-- {
				f := level.Files[i]
				if !keyInRange(keyBytes, f.MinKey, f.MaxKey) {
					continue
				}
				val, err := (&sstable.SSTable{Path: f.Path}).Get(key)
				if err != nil {
					if err == errs.ErrKeyDeleted {
						return nil, errs.ErrKeyNotFound
					}
					if err == errs.ErrKeyNotFound {
						continue
					}
					return nil, err
				}
				return val, nil
			}
		} else {
			// L1+: files are sorted and non-overlapping.
			// Binary search for the file that might contain the key.
			idx := levelSearch(level.Files, keyBytes)
			if idx >= 0 {
				f := level.Files[idx]
				if keyInRange(keyBytes, f.MinKey, f.MaxKey) {
					val, err := (&sstable.SSTable{Path: f.Path}).Get(key)
					if err != nil {
						if err == errs.ErrKeyDeleted {
							return nil, errs.ErrKeyNotFound
						}
						if err == errs.ErrKeyNotFound {
							continue
						}
						return nil, err
					}
					return val, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("key %v not found", key)
}

func (db *DB) Delete(key []byte) error {
	return db.Put(key, sstable.TOMBSTONE)
}

func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true

	db.mu.Unlock()
	// Wait for any background flushing operations to finish
	db.flushWg.Wait()
	db.mu.Lock()

	// Flush the current MemTable synchronously if it has data.
	if db.memTable.Size > 0 {
		ssTablePath := fmt.Sprintf("%s/data-%d.sstable", sstDir, db.manifest.SSTSeqNum)
		result, err := sstable.WriteSST(db.memTable, ssTablePath)
		if err != nil {
			db.mu.Unlock()
			return err
		}
		db.manifest.AddSSTable(result.SST.Path, manifest.LevelZero, result.MinKey, result.MaxKey)
		db.manifest.SSTSeqNum++
		manifest.WriteToFile(manifestPath, db.manifest)
	}

	// Close the WAL.
	err := db.wal.Close()
	db.mu.Unlock()
	return err
}

func (db *DB) rotateWAL() (string, error) {
	if err := db.wal.Close(); err != nil {
		return "", err
	}

	rotatedPath := fmt.Sprintf("%s/data-%02d.wal", walDir, db.manifest.WALSeqNum)
	if err := os.Rename(db.walPath, rotatedPath); err != nil {
		return "", err
	}

	if err := utils.SyncDir(db.walPath); err != nil {
		return "", err
	}

	db.manifest.AddRotatedWAL(rotatedPath)
	db.manifest.WALSeqNum++

	if err := manifest.WriteToFile(manifestPath, db.manifest); err != nil {
		return "", err
	}

	newWAL, err := wal.NewWAL(db.walPath)
	if err != nil {
		return "", err
	}

	db.wal = newWAL

	return rotatedPath, nil
}

func (db *DB) rotateMemTable() error {
	log.Println("Rotating WAL..")
	_, err := db.rotateWAL()
	if err != nil {
		return err
	}
	log.Println("Successfully rotated WAL!")

	db.rotatedMemTable = db.memTable
	return nil
}

func (db *DB) flushRotatedMemTableAndCleanup() {
	const maxRetries = 3
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(time.Duration(1<<uint(i)) * 500 * time.Millisecond)
			// 500ms, 1s, 2s
			log.Printf("Retrying flush (attempt %d/%d)...", i+1, maxRetries)
		}
		lastErr = db.flushRotatedMemTable()
		if lastErr == nil {
			db.flushDone.Broadcast()
			return
		}
		log.Printf("Flush attempt %d failed: %v", i+1, lastErr)
	}

	// All retries exhausted — set error state
	db.mu.Lock()
	db.flushErr = lastErr
	db.mu.Unlock()
	db.flushDone.Broadcast() // wake any waiting Put() calls
}

func (db *DB) flushRotatedMemTable() error {
	immutableRotatedMemTable := db.rotatedMemTable
	ssTablePath := fmt.Sprintf("%s/data-%d.sstable", sstDir, db.manifest.SSTSeqNum)
	if err := os.MkdirAll(filepath.Dir(ssTablePath), 0755); err != nil {
		return err
	}

	if err := utils.SyncDir(ssTablePath); err != nil {
		return err
	}

	log.Printf("Flushing MemTable to file %s", ssTablePath)
	result, err := sstable.WriteSST(immutableRotatedMemTable, ssTablePath)
	if err != nil {
		return err
	}

	db.mu.Lock()
	db.manifest.AddSSTable(result.SST.Path, manifest.LevelZero, result.MinKey, result.MaxKey)
	db.manifest.SSTSeqNum++

	if err := manifest.WriteToFile(manifestPath, db.manifest); err != nil {
		return err
	}
	log.Println("Successfully flushed MemTable!")

	db.rotatedMemTable = nil
	db.manifest.FlushedWALs = append(db.manifest.FlushedWALs, db.manifest.RotatedWALs[0])
	db.manifest.RotatedWALs = db.manifest.RotatedWALs[1:]
	db.mu.Unlock()

	// Cleans stale rotated WALs in Background
	go func() {
		// Clean up stale rotated WALs (their data is already in SSTables)
		if err := cleanupStaleWALs(db.manifest); err != nil {
			fmt.Printf("Failed cleaning up stale WALs: %v", err.Error())
		}
	}()

	return nil
}

// keyInRange checks if keyBytes falls within [minKey, maxKey].
// If minKey/maxKey are nil (not yet populated), returns true so we
// can't skip the file.
func keyInRange(keyBytes, minKey, maxKey []byte) bool {
	if len(minKey) == 0 || len(maxKey) == 0 {
		return true
	}
	return bytes.Compare(keyBytes, minKey) >= 0 &&
		bytes.Compare(keyBytes, maxKey) <= 0
}

// levelSearch returns the index of the file in files that might contain
// keyBytes, or -1 if no file could contain it.
func levelSearch(files []*manifest.FileMetadata, keyBytes []byte) int {
	if len(files) == 0 {
		return -1
	}
	lo, hi := 0, len(files)-1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		f := files[mid]
		if len(f.MaxKey) > 0 && bytes.Compare(keyBytes, f.MaxKey) > 0 {
			lo = mid + 1
		} else if len(f.MinKey) > 0 && bytes.Compare(keyBytes, f.MinKey) < 0 {
			hi = mid - 1
		} else {
			return mid
		}
	}
	if lo < len(files) {
		f := files[lo]
		if len(f.MinKey) == 0 || bytes.Compare(keyBytes, f.MinKey) >= 0 {
			return lo
		}
	}
	return -1
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

func cleanupStaleWALs(m *manifest.Manifest) error {
	if len(m.FlushedWALs) == 0 {
		return nil
	}

	for len(m.FlushedWALs) != 0 {
		wal := m.FlushedWALs[0]
		log.Printf("Cleaning up stale rotated WAL: %s", wal.Path)
		if err := os.Remove(wal.Path); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
		m.FlushedWALs = m.FlushedWALs[1:]
	}

	return nil

}
