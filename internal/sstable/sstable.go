package sstable

import (
	"bytes"
	"log"
	"os"
	"path/filepath"

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

type Entry struct {
	Key   []byte
	Value []byte
}

type SSTable struct {
	Path string
}

type WriteResult struct {
	SST    *SSTable
	MinKey []byte
	MaxKey []byte
}

func WriteSSTFromEntries(entries []Entry, path string) (*WriteResult, error) {
	if len(entries) == 0 {
		return nil, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	if err := utils.SyncDir(path); err != nil {
		return nil, err
	}

	tmpFile, err := os.Create(path + ".tmp")
	if err != nil {
		return nil, err
	}

	var buf []byte
	minKey := entries[0].Key
	maxKey := entries[len(entries)-1].Key

	for _, e := range entries {
		buf = utils.EncodeEntry(buf[:0], e.Key, e.Value)
		if _, err := tmpFile.Write(buf); err != nil {
			tmpFile.Close()
			os.Remove(path + ".tmp")
			return nil, err
		}
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		os.Remove(path + ".tmp")
		return nil, err
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(path + ".tmp")
		return nil, err
	}

	if err := os.Rename(path+".tmp", path); err != nil {
		os.Remove(path + ".tmp")
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

func WriteSST(memTable *memtable.MemTable, path string) (*WriteResult, error) {
	var entries []Entry
	err := memTable.ForEach(func(key, value []byte) error {
		entries = append(entries, Entry{Key: key, Value: value})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return WriteSSTFromEntries(entries, path)
}

func (sst *SSTable) Get(key []byte) ([]byte, error) {
	log.Printf("Searching Key: %v in SSTable: %s", key, sst.Path)
	data, err := os.ReadFile(sst.Path)
	if err != nil {
		return nil, err
	}

	offset := 0
	for offset < len(data) {
		entryKey, entryValue, n, err := utils.DecodeEntry(data[offset:])
		if err != nil {
			return nil, err
		}
		offset += n

		cmp := bytes.Compare(entryKey, key)
		if cmp == 0 {
			if IsTombstone(entryValue) {
				return nil, errs.ErrKeyDeleted
			}
			return entryValue, nil
		}

		if cmp > 0 {
			return nil, errs.ErrKeyNotFound
		}
	}

	return nil, errs.ErrKeyNotFound
}

