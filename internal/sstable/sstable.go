package sstable

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"os"
	"sort"

	types "github.com/NirajNair/lsm-db/internal"
	"github.com/NirajNair/lsm-db/internal/errs"
	"github.com/NirajNair/lsm-db/internal/memtable"
	"github.com/NirajNair/lsm-db/internal/utils"
)

// TOMBSTONE is a sentinel byte slice representing a deleted key.
// It uses 0x00 which is unlikely to appear as the first byte of most
// application data, so it serves as a safe deletion marker.
var TOMBSTONE = []byte{0x00}

// IsTombstone returns true if the value is the deletion sentinel.
func IsTombstone(value []byte) bool {
	return len(value) == 1 && value[0] == 0x00
}

type SSTable struct {
	Path string
}

type WriteResult struct {
	SST    *SSTable
	MinKey []byte
	MaxKey []byte
}

func WriteSST(memTable *memtable.MemTable, path string) (*WriteResult, error) {
	tmpFile, err := os.Create(path + ".tmp")
	if err != nil {
		return nil, err
	}

	// Collect entries for sorting.
	type sortedEntry struct {
		key   []byte
		value []byte
	}

	entries := make([]sortedEntry, 0, len(memTable.Data))
	for k, v := range memTable.Data {
		entries = append(entries, sortedEntry{
			key:   []byte(k),
			value: v,
		})
	}

	// Sort by raw key bytes — the application is responsible for
	// encoding keys in the desired ordering (big-endian for integers, etc).
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})

	encoder := gob.NewEncoder(tmpFile)
	for _, entry := range entries {
		pair := types.Entry{
			Key:   entry.key,
			Value: entry.value,
		}
		if err := encoder.Encode(pair); err != nil {
			return nil, err
		}
	}

	if err := tmpFile.Sync(); err != nil {
		return nil, err
	}
	if err := tmpFile.Close(); err != nil {
		return nil, err
	}

	if err := os.Rename(path+".tmp", path); err != nil {
		return nil, err
	}

	if err := utils.SyncDir(path); err != nil {
		return nil, err
	}

	var minKey, maxKey []byte
	if len(entries) > 0 {
		minKey = entries[0].key
		maxKey = entries[len(entries)-1].key
	}

	return &WriteResult{
		SST:    &SSTable{Path: path},
		MinKey: minKey,
		MaxKey: maxKey,
	}, nil
}

func (sst *SSTable) Get(key []byte) ([]byte, error) {
	log.Printf("Searching Key: %v in SSTable: %s", key, sst.Path)
	file, err := os.Open(sst.Path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)

	for {
		var pair types.Entry
		if err := decoder.Decode(&pair); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		cmp := bytes.Compare(pair.Key, key)
		if cmp == 0 {
			if IsTombstone(pair.Value) {
				return nil, errs.ErrKeyDeleted
			}
			return pair.Value, nil
		}

		if cmp > 0 {
			// Past the point where the key could exist (SST is sorted)
			return nil, errs.ErrKeyNotFound
		}
	}

	return nil, errs.ErrKeyNotFound
}
