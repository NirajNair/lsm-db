package manifest

import (
	"encoding/gob"
	"os"

	"github.com/NirajNair/lsm-db/internal/utils"
)

type LevelNum int

const (
	LevelZero LevelNum = iota
	LevelOne
	LevelTwo
)

const (
	ONE_MB           = 1 << 20
	LevelZeroTrigger = 4 // Files
	LevelZeroMaxSize = 4 * ONE_MB
	LevelOneMaxSize  = LevelZeroMaxSize * LevelZeroTrigger
)

type Manifest struct {
	SSTSeqNum   uint
	WALSeqNum   uint
	RotatedWALs []*WAL
	FlushedWALs []*WAL
	Levels      []*LevelMetadata
}

type WAL struct {
	Path string
}

type LevelMetadata struct {
	Level       LevelNum
	CurrentSize uint
	MaxSize     uint
	Files       []*FileMetadata
}

type FileMetadata struct {
	Path   string
	SeqNum uint
	MinKey []byte
	MaxKey []byte
	Size   uint
}

func NewManifest() *Manifest {
	levels := []*LevelMetadata{
		{
			Level:   LevelZero,
			MaxSize: LevelZeroMaxSize,
			Files:   make([]*FileMetadata, 0),
		},
		{
			Level:   LevelOne,
			MaxSize: LevelOneMaxSize,
			Files:   make([]*FileMetadata, 0),
		},
	}

	return &Manifest{
		RotatedWALs: make([]*WAL, 0),
		FlushedWALs: make([]*WAL, 0),
		Levels:      levels,
	}
}

func ReadManifest(path string) (*Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewManifest(), nil
		}
		return nil, err
	}
	defer file.Close()

	var manifest Manifest
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&manifest)
	if err != nil {
		return nil, err
	}

	return &manifest, nil
}

func WriteToFile(path string, manifest *Manifest) error {
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(manifest); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := file.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	if err := utils.SyncDir(path); err != nil {
		return err
	}

	return nil
}

// Adds the new sst details and updates the manifest data.
// optKeys accepts MinKey and MaxKey values in the same order.
func (m *Manifest) AddSSTable(path string, level LevelNum, optKeys ...[]byte) error {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return err
	}
	fileSize := uint(fileInfo.Size())

	sstFileMetadata := &FileMetadata{
		SeqNum: m.SSTSeqNum,
		Path:   path,
		Size:   fileSize,
	}
	if len(optKeys) > 0 {
		sstFileMetadata.MinKey = optKeys[0]
	}
	if len(optKeys) > 1 {
		sstFileMetadata.MaxKey = optKeys[1]
	}

	m.Levels[level].Files = append(m.Levels[level].Files, sstFileMetadata)
	m.Levels[level].CurrentSize += fileSize
	return nil
}

// Adds rotated WALs path to Manifest.
func (m *Manifest) AddRotatedWAL(path string) error {
	m.RotatedWALs = append(m.RotatedWALs, &WAL{Path: path})
	return nil
}

// Adds flushed WALs path to Manifest.
func (m *Manifest) AddFlushedWAL(path string) error {
	m.FlushedWALs = append(m.FlushedWALs, &WAL{Path: path})
	return nil
}
