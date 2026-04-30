package sstable

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"os"

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

	encoder := gob.NewEncoder(tmpFile)
	var minKey, maxKey []byte
	first := true

	err = memTable.ForEach(func(key, value []byte) error {
		pair := types.Entry{
			Key:   key,
			Value: value,
		}
		if err := encoder.Encode(pair); err != nil {
			return err
		}
		if first {
			minKey = key
			first = false
		}
		maxKey = key
		return nil
	})
	if err != nil {
		tmpFile.Close()
		os.Remove(path + ".tmp")
		return nil, err
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
