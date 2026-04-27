package sstable

import (
	"encoding/gob"
	"io"
	"log"
	"os"
	"sort"

	"github.com/NirajNair/lsm-db/internal/errs"
	"github.com/NirajNair/lsm-db/internal/fsutil"
	"github.com/NirajNair/lsm-db/internal/memtable"
)

const TOMBSTONE = "--TOMBSTONE--"

type SSTable[K comparable, V any] struct {
	path string
}

type Pair[K comparable, V any] struct {
	Key   K
	Value V
}

func WriteSST[K comparable, V any](memTable *memtable.MemTable[K, V], path string) (*SSTable[K, V], error) {
	tmpFile, err := os.Create(path + ".tmp")
	if err != nil {
		return nil, err
	}

	pairs := make([]Pair[K, V], 0, len(memTable.Data))
	for k, v := range memTable.Data {
		pairs = append(pairs, Pair[K, V]{Key: k, Value: v})
	}

	sort.Slice(pairs, func(i int, j int) bool {
		return any(pairs[i].Key).(string) < any(pairs[j].Key).(string)
	})

	encoder := gob.NewEncoder(tmpFile)
	for _, pair := range pairs {
		if err := encoder.Encode(pair); err != nil {
			return nil, err
		}
	}

	tmpFile.Close()

	if err := os.Rename(path+".tmp", path); err != nil {
		return nil, err
	}

	if err := fsutil.SyncDir(path); err != nil {
		return nil, err
	}

	return &SSTable[K, V]{path: path}, nil
}

func (sst *SSTable[K, V]) Get(key K) (V, error) {
	var zero V

	log.Printf("Searching Key: %v in SSTable: %s", key, sst.path)
	file, err := os.Open(sst.path)
	if err != nil {
		return zero, err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)

	for {
		var pair Pair[K, V]
		if err := decoder.Decode(&pair); err != nil {
			if err == io.EOF {
				break
			}
			return zero, nil
		}

		storedKey := any(pair.Key).(string)
		queryKey := any(key).(string)
		if storedKey == queryKey {
			if any(pair.Value).(string) == TOMBSTONE {
				return zero, errs.ErrKeyDeleted
			}
			return pair.Value, nil
		}

		if storedKey > queryKey {
			return zero, errs.ErrKeyNotFound
		}
	}

	return zero, errs.ErrKeyNotFound
}
