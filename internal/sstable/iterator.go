package sstable

import (
	"io"
	"os"

	"github.com/NirajNair/lsm-db/internal/utils"
)

type SSTIter struct {
	data   []byte
	offset int
}

func (sst *SSTable) NewIterator() (*SSTIter, error) {
	data, err := os.ReadFile(sst.Path)
	if err != nil {
		return nil, err
	}
	return &SSTIter{data: data, offset: 0}, nil
}

func (it *SSTIter) Next() (key, value []byte, err error) {
	if it.offset >= len(it.data) {
		return nil, nil, io.EOF
	}
	key, value, n, err := utils.DecodeEntry(it.data[it.offset:])
	if err != nil {
		return nil, nil, err
	}
	it.offset += n
	return key, value, nil
}
