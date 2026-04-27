package wal

import (
	"encoding/gob"
	"io"
	"os"
	"path/filepath"

	"github.com/NirajNair/lsm-db/internal/fsutil"
	"github.com/NirajNair/lsm-db/internal/memtable"
)

type WAL[K comparable, V any] struct {
	file    *os.File
	encoder *gob.Encoder
}

type WALEntry[K comparable, V any] struct {
	Key   K
	Value V
}

func NewWAL[K comparable, V any](path string) (*WAL[K, V], error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	if err := fsutil.SyncDir(path); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL[K, V]{
		file:    file,
		encoder: gob.NewEncoder(file),
	}, nil
}

func (w *WAL[K, V]) Write(key K, value V) error {
	entry := &WALEntry[K, V]{Key: key, Value: value}
	if err := w.encoder.Encode(entry); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL[K, V]) Close() error {
	if err := w.file.Close(); err != nil {
		return err
	}
	return nil
}

func ReplayWAL[K comparable, V any](path string) (*memtable.MemTable[K, V], error) {
	memTable := memtable.NewMemTable[K, V]()

	// Return the empty MemTable if WAL file does not exist
	if _, err := os.Stat(path); err != nil {
		return memTable, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	for {
		var entry WALEntry[K, V]
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		memTable.Put(entry.Key, entry.Value)
	}

	return memTable, nil
}
