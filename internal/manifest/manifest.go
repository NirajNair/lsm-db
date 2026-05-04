package manifest

import (
	"encoding/gob"
	"os"

	"github.com/NirajNair/lsm-db/internal/utils"
)

type LevelNum int

const (
	L0 LevelNum = iota
	L1
	L2
)

const (
	ONE_MB             = 1 << 20
	SizeRatio          = 4
	MaxFileSize        = ONE_MB
	L0CompTriggerFiles = 4 // Files
	L0MaxSize          = 4 * MaxFileSize
	L1MaxSize          = SizeRatio * L0MaxSize
	L2MaxSize          = SizeRatio * L1MaxSize
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
	Level             LevelNum
	CurrentSize       uint
	MaxSize           uint
	Files             []*FileMetadata
	NextCompactionIdx int
}

type FileMetadata struct {
	Path   string
	MinKey []byte
	MaxKey []byte
	Size   uint
}

func NewManifest() *Manifest {
	levels := []*LevelMetadata{
		{
			Level:   L0,
			MaxSize: L0MaxSize,
			Files:   make([]*FileMetadata, 0),
		},
		{
			Level:   L1,
			MaxSize: L1MaxSize,
			Files:   make([]*FileMetadata, 0),
		},
		{
			Level:   L2,
			MaxSize: L2MaxSize,
			Files:   make([]*FileMetadata, 0),
		},
	}

	return &Manifest{
		RotatedWALs: make([]*WAL, 0),
		FlushedWALs: make([]*WAL, 0),
		Levels:      levels,
	}
}

const NumLevels = 3

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

	for len(manifest.Levels) < NumLevels {
		level := LevelNum(len(manifest.Levels))
		var maxSize uint
		switch level {
		case L0:
			maxSize = L0MaxSize
		case L1:
			maxSize = L1MaxSize
		case L2:
			maxSize = L2MaxSize
		}
		manifest.Levels = append(manifest.Levels, &LevelMetadata{
			Level:   level,
			MaxSize: maxSize,
			Files:   make([]*FileMetadata, 0),
		})
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
		Path: path,
		Size: fileSize,
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

func (m *Manifest) DeepCopy() *Manifest {
	newManifest := &Manifest{
		SSTSeqNum:   m.SSTSeqNum,
		WALSeqNum:   m.WALSeqNum,
		RotatedWALs: make([]*WAL, len(m.RotatedWALs)),
		FlushedWALs: make([]*WAL, len(m.FlushedWALs)),
		Levels:      make([]*LevelMetadata, len(m.Levels)),
	}
	for i, wal := range m.RotatedWALs {
		newManifest.RotatedWALs[i] = &WAL{Path: wal.Path}
	}
	for i, wal := range m.FlushedWALs {
		newManifest.FlushedWALs[i] = &WAL{Path: wal.Path}
	}
	for i, level := range m.Levels {
		newLevel := &LevelMetadata{
			Level:             level.Level,
			CurrentSize:       level.CurrentSize,
			MaxSize:           level.MaxSize,
			NextCompactionIdx: level.NextCompactionIdx,
			Files:             make([]*FileMetadata, len(level.Files)),
		}
		for j, f := range level.Files {
			newLevel.Files[j] = &FileMetadata{
				Path:   f.Path,
				MinKey: append([]byte(nil), f.MinKey...),
				MaxKey: append([]byte(nil), f.MaxKey...),
				Size:   f.Size,
			}
		}
		newManifest.Levels[i] = newLevel
	}
	return newManifest
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
