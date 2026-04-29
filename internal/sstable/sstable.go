package sstable

import (
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

const TOMBSTONE = "--TOMBSTONE--"

type SSTable[K comparable, V any] struct {
	Path string
}

type WriteResult[K comparable, V any] struct {
	SST    *SSTable[K, V]
	MinKey []byte
	MaxKey []byte
}

func WriteSST[K comparable, V any](memTable *memtable.MemTable[K, V], path string) (*WriteResult[K, V], error) {
	tmpFile, err := os.Create(path + ".tmp")
	if err != nil {
		return nil, err
	}

	pairs := make([]types.Pair[K, V], 0, len(memTable.Data))
	for k, v := range memTable.Data {
		pairs = append(pairs, types.Pair[K, V]{Key: k, Value: v})
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
	if len(pairs) > 0 {
		minKey = []byte(any(pairs[0].Key).(string))
		maxKey = []byte(any(pairs[len(pairs)-1].Key).(string))
	}

	return &WriteResult[K, V]{
		SST:    &SSTable[K, V]{Path: path},
		MinKey: minKey,
		MaxKey: maxKey,
	}, nil
}

func (sst *SSTable[K, V]) Get(key K) (V, error) {
	var zero V

	log.Printf("Searching Key: %v in SSTable: %s", key, sst.Path)
	file, err := os.Open(sst.Path)
	if err != nil {
		return zero, err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)

	for {
		var pair types.Pair[K, V]
		if err := decoder.Decode(&pair); err != nil {
			if err == io.EOF {
				break
			}
			return zero, err
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
