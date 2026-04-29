package wal

import (
	"encoding/gob"
	"io"
	"os"
	"path/filepath"

	types "github.com/NirajNair/lsm-db/internal"
	"github.com/NirajNair/lsm-db/internal/memtable"
	"github.com/NirajNair/lsm-db/internal/utils"
)

type WAL[K comparable, V any] struct {
	file    *os.File
	encoder *gob.Encoder
}

func NewWAL[K comparable, V any](path string) (*WAL[K, V], error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	if err := utils.SyncDir(path); err != nil {
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

func (w *WAL[K, V]) Write(key K, value V) (int, error) {
	entry := &types.Pair[K, V]{Key: key, Value: value}
	currentOffset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if err := w.encoder.Encode(entry); err != nil {
		return 0, err
	}
	if err := w.file.Sync(); err != nil {
		return 0, err
	}
	newOffset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	return int(newOffset - currentOffset), nil
}

func (w *WAL[K, V]) Close() error {
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

func ReplayWAL[K comparable, V any](path string) (*memtable.MemTable[K, V], error) {
	memTable := memtable.NewMemTable[K, V]()

	// Return the empty MemTable if WAL file does not exist
	fileInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return memTable, nil
		}
		return nil, err
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	for {
		var entry types.Pair[K, V]
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		memTable.Put(entry.Key, entry.Value)
	}
	memTable.Size = uint(fileInfo.Size())
	return memTable, nil
}
