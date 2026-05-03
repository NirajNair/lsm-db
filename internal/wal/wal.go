package wal

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/NirajNair/lsm-db/internal/memtable"
	"github.com/NirajNair/lsm-db/internal/utils"
)

type WAL struct {
	file *os.File
}

func NewWAL(path string) (*WAL, error) {
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

	return &WAL{
		file: file,
	}, nil
}

func (w *WAL) Write(key, value []byte) (int, error) {
	encoded := utils.EncodeEntry(nil, key, value)
	n, err := w.file.Write(encoded)
	if err != nil {
		return 0, err
	}
	if err := w.file.Sync(); err != nil {
		return 0, err
	}
	return n, nil
}

func (w *WAL) Close() error {
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

func ReplayWAL(path string) (*memtable.MemTable, error) {
	memTable := memtable.NewMemTable()

	fileInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return memTable, nil
		}
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	offset := 0
	for offset < len(data) {
		key, value, n, err := utils.DecodeEntry(data[offset:])
		if err != nil {
			if offset == 0 {
				return nil, fmt.Errorf("WAL replay: failed to decode first entry: %w", err)
			}
			log.Printf("WAL replay: truncated entry at offset %d: %v", offset, err)
			break
		}
		memTable.Put(key, value)
		offset += n
	}

	memTable.Size = uint(fileInfo.Size())
	return memTable, nil
}
