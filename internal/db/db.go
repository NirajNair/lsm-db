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

	"github.com/NirajNair/lsm-db/internal/compaction"
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
	manifestErr     error
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

	// Clean up orphan SSTable files (crash recovery).
	if err := ReconcileSSTDir(m, sstDir); err != nil {
		log.Printf("Warning: SSTDir reconciliation failed: %v", err)
	}

	// Clean up flushed WALs (their data is already in SSTables).
	// Non-fatal: stale WALs are harmless, just waste disk space.
	if err := cleanupStaleWALs(m); err != nil {
		log.Printf("Warning: failed to clean up flushed WALs: %v", err)
	}

	memTable, err := wal.ReplayWAL(walFilePath)
	if err != nil {
		return nil, err
	}

	// Replay rotated WALs if any (crash recovery: WAL was rotated
	// but the MemTable wasn't flushed to an SSTable before the process died).
	var rotatedMemTable *memtable.MemTable
	if len(m.RotatedWALs) > 0 {
		rotatedMemTable, err = wal.ReplayWAL(m.RotatedWALs[0].Path)
		if err != nil {
			return nil, err
		}
	}

	walFile, err := wal.NewWAL(walFilePath)
	if err != nil {
		return nil, err
	}

	db := &DB{
		memTable:        memTable,
		rotatedMemTable: rotatedMemTable,
		maxMemTableSize: maxMemTableSize,
		wal:             walFile,
		walPath:         walFilePath,
		manifest:        m,
	}
	db.flushDone = *sync.NewCond(&db.mu)

	// If we have a rotated MemTable (crash recovery), flush it synchronously
	// before accepting any writes. This is safe because no goroutines are
	// running yet, so no lock is needed.
	if db.rotatedMemTable != nil {
		// Phase 1: Write SSTable to disk (don't write manifest yet)
		seqNum := db.manifest.SSTSeqNum
		sstPath := fmt.Sprintf("%s/data-%d.sstable", sstDir, seqNum)
		result, err := sstable.WriteSST(db.rotatedMemTable, sstPath)
		if err != nil {
			return nil, fmt.Errorf("failed to flush rotated MemTable on startup: %w", err)
		}
		// Capture the rotated WAL before we modify the slice — needed for rollback.
		rotatedWAL := db.manifest.RotatedWALs[0]
		// Phase 2: Update ALL in-memory manifest state
		db.manifest.AddSSTable(result.SST.Path, manifest.L0, result.MinKey, result.MaxKey)
		db.manifest.SSTSeqNum++
		db.manifest.FlushedWALs = append(db.manifest.FlushedWALs, rotatedWAL)
		db.manifest.RotatedWALs = db.manifest.RotatedWALs[1:]
		db.rotatedMemTable = nil
		// Phase 3: Persist manifest ONCE (atomic)
		if err := manifest.WriteToFile(manifestPath, db.manifest); err != nil {
			// Manifest write failed — full rollback
			if removeErr := os.Remove(sstPath); removeErr != nil {
				return nil, fmt.Errorf(
					"manifest write failed (%v) AND SSTable cleanup failed (%v): manual recovery needed",
					err, removeErr,
				)
			}
			// Undo in-memory state
			db.manifest.SSTSeqNum--
			level := db.manifest.Levels[manifest.L0]
			if len(level.Files) > 0 {
				lastFile := level.Files[len(level.Files)-1]
				level.CurrentSize -= lastFile.Size
				level.Files = level.Files[:len(level.Files)-1]
			}
			db.manifest.FlushedWALs = db.manifest.FlushedWALs[:len(db.manifest.FlushedWALs)-1]
			db.manifest.RotatedWALs = append([]*manifest.WAL{rotatedWAL}, db.manifest.RotatedWALs...)

			return nil, fmt.Errorf("manifest write failed on startup: %w", err)
		}
		// Only NOW is it safe to delete the WAL file
		if err := cleanupStaleWALs(db.manifest); err != nil {
			log.Printf("Warning: failed to clean up flushed WALs on startup: %v", err)
		}
	}

	// Run startup compaction if any levels need it.
	if err := db.compactLoop(); err != nil {
		log.Printf("Warning: startup compaction failed: %v", err)
	}

	return db, nil
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return errors.New("DB is closed")
	}

	if db.manifestErr != nil {
		return fmt.Errorf("DB is in inconsistent state (restart required): %w", db.manifestErr)
	}

	for db.memTable.Size >= db.maxMemTableSize {
		if db.rotatedMemTable != nil {
			// Wait for the background flush to finish instead of rejecting writes.
			// Put() holds db.mu (write lock) — Wait() releases it, waits for
			// Broadcast(), then reacquires it before returning.
			db.flushDone.Wait()
			if db.closed {
				return errors.New("DB is closed")
			}
			if db.flushErr != nil {
				return fmt.Errorf("DB flush error: %w", db.flushErr)
			}
			if db.manifestErr != nil {
				return fmt.Errorf("DB is in inconsistent state (restart required): %w", db.manifestErr)
			}
			continue
		}

		if err := db.rotateMemTable(); err != nil {
			return err
		}

		// Capture seqNum and immutable reference under the lock — these
		// are passed to the background goroutine so it doesn't need to
		// read db.manifest.SSTSeqNum or db.rotatedMemTable without a lock.
		seqNum := db.manifest.SSTSeqNum
		immutable := db.rotatedMemTable
		db.memTable = memtable.NewMemTable()

		db.flushWg.Add(1)
		go func() {
			defer db.flushWg.Done()
			db.flushRotatedMemTableAndCleanup(seqNum, immutable)
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
		if level.Level == manifest.L0 {
			// L0: check newest files first, skip files whose range
			// doesn't contain the key.
			for i := len(level.Files) - 1; i >= 0; i-- {
				f := level.Files[i]
				if !keyInRange(keyBytes, f.MinKey, f.MaxKey) {
					continue
				}
				val, err := (&sstable.SSTable{Path: f.Path}).Get(key)
				if err != nil {
					if errors.Is(err, errs.ErrKeyDeleted) {
						return nil, errs.ErrKeyNotFound
					}
					if errors.Is(err, errs.ErrKeyNotFound) {
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
						if errors.Is(err, errs.ErrKeyDeleted) {
							return nil, errs.ErrKeyNotFound
						}
						if errors.Is(err, errs.ErrKeyNotFound) {
							continue
						}
						return nil, err
					}
					return val, nil
				}
			}
		}
	}

	return nil, errs.ErrKeyNotFound
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

	// Wait for any background flush to complete.
	for db.rotatedMemTable != nil {
		db.flushDone.Wait()
	}

	// Log errors but don't prevent close. The data is still recoverable:
	// - flushErr: rotated WAL on disk has the data, will be replayed on restart
	// - manifestErr: process must be restarted for recovery
	if db.flushErr != nil {
		log.Printf("Warning: closing DB with flush error (data recoverable on restart): %v", db.flushErr)
	}
	if db.manifestErr != nil {
		log.Printf("Warning: closing DB with manifest error (restart required): %v", db.manifestErr)
	}

	// Flush the current (mutable) MemTable synchronously if it has data.
	// Only flush if there's no manifest error — the admin needs to restart
	// to recover from that state.
	if db.memTable.Size > 0 && db.manifestErr == nil {
		seqNum := db.manifest.SSTSeqNum

		// Phase 1: Write SSTable (no lock needed — db.closed=true, no concurrent writers)
		db.mu.Unlock()
		result, err := sstable.WriteSST(db.memTable, fmt.Sprintf("%s/data-%d.sstable", sstDir, seqNum))
		if err != nil {
			log.Printf("Warning: failed to flush MemTable during close: %v", err)
			db.mu.Lock()
		} else {
			// Phase 2: Update manifest (under lock)
			db.mu.Lock()
			db.manifest.AddSSTable(result.SST.Path, manifest.L0, result.MinKey, result.MaxKey)
			db.manifest.SSTSeqNum++

			if err := manifest.WriteToFile(manifestPath, db.manifest); err != nil {
				log.Printf("Warning: manifest write failed during close: %v", err)
				sstPath := fmt.Sprintf("%s/data-%d.sstable", sstDir, seqNum)
				os.Remove(sstPath)
				db.manifest.SSTSeqNum--
				level := db.manifest.Levels[manifest.L0]
				if len(level.Files) > 0 {
					lastFile := level.Files[len(level.Files)-1]
					level.CurrentSize -= lastFile.Size
					level.Files = level.Files[:len(level.Files)-1]
				}
			}

			db.memTable = memtable.NewMemTable()
		}
	}

	// Wait for all background goroutines (including WAL cleanup) to finish.
	db.mu.Unlock()
	db.flushWg.Wait()
	db.mu.Lock()

	err := db.wal.Close()
	db.mu.Unlock()
	return err
}

// rotateWAL closes the current WAL, renames it to a rotated path, creates
// a new WAL, and updates the manifest. If any step fails after the WAL is
// renamed, it attempts to roll back to maintain consistency.
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
		// Manifest write failed after WAL rotation — attempt rollback.
		log.Printf("CRITICAL: manifest write failed after WAL rotation: %v", err)
		if renameErr := os.Rename(rotatedPath, db.walPath); renameErr != nil {
			log.Printf("CRITICAL: WAL rollback also failed: %v (manual recovery needed)", renameErr)
		} else {
			// Rolled back WAL. Undo in-memory manifest changes.
			db.manifest.RotatedWALs = db.manifest.RotatedWALs[:len(db.manifest.RotatedWALs)-1]
			db.manifest.WALSeqNum--
			// Try to reopen the WAL at its original path.
			if reopenWAL, reopenErr := wal.NewWAL(db.walPath); reopenErr == nil {
				db.wal = reopenWAL
			}
		}
		return "", err
	}

	newWAL, err := wal.NewWAL(db.walPath)
	if err != nil {
		// New WAL creation failed. The rotated WAL is on disk and referenced
		// in the manifest. On restart, RotatedWALs will be replayed — no data loss.
		// But we can't accept writes without a WAL.
		log.Printf("CRITICAL: new WAL creation failed after rotation: %v", err)
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

// flushRotatedMemTableAndCleanup retries the flush up to maxRetries times
// with exponential backoff. It breaks out of the retry loop immediately if
// manifestErr is set, since that indicates an inconsistent state that requires
// a restart.
func (db *DB) flushRotatedMemTableAndCleanup(seqNum uint, immutable *memtable.MemTable) {
	const maxRetries = 3
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(time.Duration(1<<uint(i)) * 500 * time.Millisecond)
			log.Printf("Retrying flush (attempt %d/%d)...", i+1, maxRetries)
		}
		lastErr = db.flushRotatedMemTable(seqNum, immutable)
		if lastErr == nil {
			db.flushDone.Broadcast()

			// After successful flush, run compaction cascade.
			if err := db.compactLoop(); err != nil {
				log.Printf("Compaction error: %v", err)
				db.mu.Lock()
				db.manifestErr = err
				db.mu.Unlock()
			}
			return
		}
		log.Printf("Flush attempt %d failed: %v", i+1, lastErr)

		// Don't retry manifest errors — they require external intervention.
		db.mu.Lock()
		if db.manifestErr != nil {
			db.mu.Unlock()
			break
		}
		db.mu.Unlock()
	}

	// All retries exhausted (or manifest error encountered).
	db.mu.Lock()
	db.flushErr = lastErr
	db.mu.Unlock()
	db.flushDone.Broadcast()
}

// flushRotatedMemTable flushes the immutable MemTable to an SSTable and
// updates the manifest. It uses a two-phase approach:
//
//	Phase 1 (no lock): Write SSTable to disk — pure I/O, no shared state.
//	Phase 2 (under lock): Update manifest, move WALs, clear rotatedMemTable.
//
// seqNum and immutable are captured under db.mu in Put() and passed here
// to avoid accessing shared state without a lock.
func (db *DB) flushRotatedMemTable(seqNum uint, immutable *memtable.MemTable) error {
	// Phase 1 (no lock): Write SSTable to disk.
	// immutable is read-only, seqNum was captured under the lock in Put().
	ssTablePath := fmt.Sprintf("%s/data-%d.sstable", sstDir, seqNum)

	result, err := sstable.WriteSST(immutable, ssTablePath)
	if err != nil {
		return err
	}

	// Phase 2 (under lock): Update manifest and shared state.
	db.mu.Lock()

	db.manifest.AddSSTable(result.SST.Path, manifest.L0, result.MinKey, result.MaxKey)
	db.manifest.SSTSeqNum++

	if err := manifest.WriteToFile(manifestPath, db.manifest); err != nil {
		// Manifest write failed after SSTable was written to disk.
		// Attempt rollback: remove orphaned SSTable, undo in-memory changes.
		log.Printf("CRITICAL: manifest write failed: %v", err)
		if removeErr := os.Remove(ssTablePath); removeErr != nil {
			// Rollback also failed — DB is truly inconsistent.
			db.manifestErr = fmt.Errorf("manifest write failed (%v) AND SSTable cleanup failed (%v)", err, removeErr)
			db.mu.Unlock()
			return db.manifestErr
		}
		// Rollback succeeded — state is clean, can retry.
		db.manifest.SSTSeqNum--
		level := db.manifest.Levels[manifest.L0]
		if len(level.Files) > 0 {
			lastFile := level.Files[len(level.Files)-1]
			level.CurrentSize -= lastFile.Size
			level.Files = level.Files[:len(level.Files)-1]
		}
		db.mu.Unlock()
		return fmt.Errorf("manifest write failed (SSTable cleaned up): %w", err)
	}

	log.Println("Successfully flushed MemTable!")

	// Move WAL from RotatedWALs → FlushedWALs (data is now in an SSTable,
	// so the WAL is safe to delete).
	db.manifest.FlushedWALs = append(db.manifest.FlushedWALs, db.manifest.RotatedWALs[0])
	db.manifest.RotatedWALs = db.manifest.RotatedWALs[1:]

	db.rotatedMemTable = nil
	db.mu.Unlock()

	// Phase 3 (background): Clean up flushed WAL files.
	// Tracked by flushWg so Close() can wait for this to finish.
	db.flushWg.Add(1)
	go func() {
		defer db.flushWg.Done()
		db.mu.Lock()
		cleanupStaleWALs(db.manifest)
		db.mu.Unlock()
	}()

	return nil
}

// compactLoop runs compaction in a cascade until no level triggers
// compaction. It follows the same lock pattern as flushRotatedMemTable:
// lock for manifest reads/writes, unlock for disk I/O.
//
// Called from:
//  1. flushRotatedMemTableAndCleanup (background goroutine) after successful flush
//  2. NewDB (single-threaded) for startup compaction
func (db *DB) compactLoop() error {
	for i := 0; i < compaction.MaxCascadeDepth; i++ {
		db.mu.Lock()
		shouldCompact, level := compaction.ShouldCompact(db.manifest)
		if !shouldCompact {
			db.mu.Unlock()
			return nil
		}
		c := compaction.PickFiles(db.manifest, level)
		if c == nil {
			db.mu.Unlock()
			return nil
		}

		// Capture the next sequence number under the lock and reserve
		// range [startSeqNum, startSeqNum+maxOutputs) to prevent races
		// with concurrent flush goroutines that also advance SSTSeqNum.
		startSeqNum := db.manifest.SSTSeqNum
		maxOutputs := uint(len(c.SourceFiles) + len(c.TargetFiles))
		if maxOutputs == 0 {
			maxOutputs = 1
		}

		// Snapshot manifest state BEFORE reserving seqnums, so rollback
		// restores SSTSeqNum to its pre-compaction value.
		snapshot := db.manifest.DeepCopy()

		// Reserve sequence numbers: advance SSTSeqNum so concurrent
		// flushes don't collide with our file paths.
		db.manifest.SSTSeqNum = startSeqNum + maxOutputs
		db.mu.Unlock()

		// Phase 1 (no lock): Execute compaction — read/merge SSTables (I/O).
		results, err := compaction.Execute(c, sstDir, startSeqNum)
		if err != nil {
			// Un-reserve the sequence numbers since we won't use them.
			db.mu.Lock()
			db.manifest.SSTSeqNum = startSeqNum
			db.mu.Unlock()
			return fmt.Errorf("compaction L%d→L%d execute failed: %w", c.Level, c.TargetLevel, err)
		}

		// Phase 2 (under lock): Update manifest, adjust SSTSeqNum, write to disk.
		db.mu.Lock()
		compaction.ApplyCompaction(c, results, db.manifest)
		// Adjust SSTSeqNum to actual number of files produced (may be less
		// than maxOutputs if merging reduced the count).
		db.manifest.SSTSeqNum = startSeqNum + uint(len(results))
		if err := manifest.WriteToFile(manifestPath, db.manifest); err != nil {
			// Manifest write failed — rollback in-memory state.
			log.Printf("CRITICAL: manifest write failed after compaction L%d→L%d: %v", c.Level, c.TargetLevel, err)
			// Delete the new SSTable files we just wrote.
			for _, r := range results {
				if err := os.Remove(r.SST.Path); err != nil && !os.IsNotExist(err) {
					log.Printf("Warning: failed to remove SSTable %s during rollback: %v", r.SST.Path, err)
				}
			}
			// Restore manifest from snapshot (which has pre-compaction SSTSeqNum).
			db.manifest = snapshot
			db.mu.Unlock()
			return fmt.Errorf("manifest write failed after compaction: %w", err)
		}

		log.Printf("Compaction L%d→L%d complete, produced %d files", c.Level, c.TargetLevel, len(results))
		db.mu.Unlock()

		// Phase 3 (background): Delete old SSTable files.
		// Use flushWg so Close() waits for these goroutines.
		oldFiles := append(append([]*manifest.FileMetadata{}, c.SourceFiles...), c.TargetFiles...)
		db.flushWg.Add(1)
		go func(files []*manifest.FileMetadata) {
			defer db.flushWg.Done()
			for _, f := range files {
				os.Remove(f.Path)
			}
		}(oldFiles)
	}
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

// cleanupStaleWALs deletes WAL files listed in FlushedWALs and removes
// them from the manifest. The caller must hold db.mu or be in a single-
// threaded context (like NewDB).
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

// ReconcileSSTDir removes SSTable files on disk that are not referenced
// by the manifest. This handles crash recovery: if the process dies after
// writing a new SSTable but before updating the manifest (or after updating
// the manifest but before the old SSTables are deleted), orphan files will
// be cleaned up on restart.
func ReconcileSSTDir(m *manifest.Manifest, dir string) error {
	// Build set of referenced SSTable paths from manifest.
	referenced := make(map[string]bool)
	for _, level := range m.Levels {
		for _, f := range level.Files {
			referenced[f.Path] = true
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".sstable" {
			continue
		}
		fullPath := filepath.Join(dir, entry.Name())
		if !referenced[fullPath] {
			log.Printf("ReconcileSSTDir: removing orphan %s", fullPath)
			if err := os.Remove(fullPath); err != nil {
				log.Printf("Warning: failed to remove orphan %s: %v", fullPath, err)
				// Non-fatal: continue cleaning up other orphans.
			}
		}
	}
	return nil
}
