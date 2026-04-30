package db

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

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
	maxMemTableSize uint
	wal             *wal.WAL
	walPath         string
	manifest        *manifest.Manifest
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
	if err := cleanupStaleWALs(walDir, m); err != nil {
		return nil, err
	}

	memTable, err := wal.ReplayWAL(walFilePath)
	if err != nil {
		return nil, err
	}

	walFile, err := wal.NewWAL(walFilePath)
	if err != nil {
		return nil, err
	}

	return &DB{
		memTable:        memTable,
		maxMemTableSize: maxMemTableSize,
		wal:             walFile,
		walPath:         walFilePath,
		manifest:        m,
	}, nil
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	size, err := db.wal.Write(key, value)
	if err != nil {
		return err
	}
	log.Printf("Written key to WAL (encoded size: %d bytes)", size)

	// TODO: Remove string conversion one Skip List is implemented
	// MemTable uses string keys internally (map[string][]byte) until
	// skip list is implemented. string(key) is a zero-cost reinterpretation.
	if err := db.memTable.Put(string(key), value, uint(size)); err != nil {
		return err
	}
	log.Printf("Added key to MemTable")

	if db.memTable.Size >= db.maxMemTableSize {
		if err := db.flushMemTable(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Update(key, value []byte) error {
	return db.Put(key, value)
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// TODO: Remove string conversion one Skip List is implemented
	// MemTable uses string keys internally until skip list is implemented.
	if val, ok := db.memTable.Get(string(key)); ok {
		if sstable.IsTombstone(val) {
			return nil, errs.ErrKeyDeleted
		}
		return val, nil
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

func (db *DB) flushMemTable() error {
	ssTablePath := fmt.Sprintf("%s/data-%d.sstable", sstDir, db.manifest.SSTSeqNum)
	if err := os.MkdirAll(filepath.Dir(ssTablePath), 0755); err != nil {
		return err
	}

	if err := utils.SyncDir(ssTablePath); err != nil {
		return err
	}

	log.Printf("Flushing MemTable to file %s", ssTablePath)
	result, err := sstable.WriteSST(db.memTable, ssTablePath)
	if err != nil {
		return err
	}
	db.manifest.AddSSTable(result.SST.Path, manifest.LevelZero, result.MinKey, result.MaxKey)
	db.manifest.SSTSeqNum++

	if err := manifest.WriteToFile(manifestPath, db.manifest); err != nil {
		return err
	}
	log.Println("Successfully flushed MemTable!")

	log.Println("Rotating WAL..")
	rotatedPath, err := db.rotateWAL()
	if err != nil {
		return err
	}

	// The rotated WAL is now redundant (its data is in the SSTable).
	if err := os.Remove(rotatedPath); err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: failed to remove rotated WAL %s: %v", rotatedPath, err)
	}
	log.Println("Successfully rotated WAL!")

	db.memTable = memtable.NewMemTable()

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

func cleanupStaleWALs(walDir string, m *manifest.Manifest) error {
	entries, err := os.ReadDir(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "data-") && strings.HasSuffix(entry.Name(), ".wal") {
			path := filepath.Join(walDir, entry.Name())
			log.Printf("Cleaning up stale rotated WAL: %s", path)
			if err := os.Remove(path); err != nil {
				return err
			}
		}
	}

	return nil
}
