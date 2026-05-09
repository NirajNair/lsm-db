package compaction

import (
	"bytes"

	"github.com/NirajNair/lsm-db/internal/manifest"
)

const MaxCascadeDepth = 10

// Compaction describes a compaction job: merge SourceFiles + TargetFiles from
// Level into TargetLevel, producing new files in TargetLevel.
type Compaction struct {
	Level       manifest.LevelNum
	TargetLevel manifest.LevelNum
	SourceFiles []*manifest.FileMetadata
	TargetFiles []*manifest.FileMetadata
	IsLastLevel bool
}

// ShouldCompact returns (bool, level) that needs compaction, or (false, -1) if none.
// L0 triggers when file count >= L0CompTriggerFiles.
// L1+ triggers when CurrentSize >= MaxSize.
func ShouldCompact(m *manifest.Manifest) (bool, manifest.LevelNum) {
	if len(m.Levels[manifest.L0].Files) >= manifest.L0CompTriggerFiles {
		return true, manifest.L0
	}
	for i := 1; i < manifest.NumLevels; i++ {
		if m.Levels[i].CurrentSize >= m.Levels[i].MaxSize {
			return true, manifest.LevelNum(i)
		}
	}
	return false, -1
}

// PickFiles selects which files to compact for the given level.
// L0→L1: ALL L0 files + ALL L1 files (full merge, since L0 files overlap).
// L1→L2+: One source file (round-robin via NextCompactionIdx) + overlapping
// target files in the next level.
func PickFiles(m *manifest.Manifest, level manifest.LevelNum) *Compaction {
	targetLevel := level + 1
	isLastLevel := targetLevel == manifest.LevelNum(manifest.NumLevels-1)

	if level == manifest.L0 {
		sourceFiles := make([]*manifest.FileMetadata, len(m.Levels[level].Files))
		copy(sourceFiles, m.Levels[level].Files)
		targetFiles := make([]*manifest.FileMetadata, len(m.Levels[targetLevel].Files))
		copy(targetFiles, m.Levels[targetLevel].Files)
		return &Compaction{
			Level:       level,
			TargetLevel: targetLevel,
			SourceFiles: sourceFiles,
			TargetFiles: targetFiles,
			IsLastLevel: isLastLevel,
		}
	}

	levelMeta := m.Levels[level]
	if len(levelMeta.Files) == 0 {
		return nil
	}
	idx := levelMeta.NextCompactionIdx % len(levelMeta.Files)
	sourceFile := levelMeta.Files[idx]

	var targetFiles []*manifest.FileMetadata
	for _, f := range m.Levels[targetLevel].Files {
		if keyRangesOverlap(sourceFile.MinKey, sourceFile.MaxKey, f.MinKey, f.MaxKey) {
			targetFiles = append(targetFiles, f)
		}
	}
	return &Compaction{
		Level:       level,
		TargetLevel: targetLevel,
		SourceFiles: []*manifest.FileMetadata{sourceFile},
		TargetFiles: targetFiles,
		IsLastLevel: isLastLevel,
	}
}

// UpdateNextCompactionIdx advances the round-robin cursor for the given level.
func UpdateNextCompactionIdx(m *manifest.Manifest, level manifest.LevelNum) {
	levelMeta := m.Levels[level]
	if len(levelMeta.Files) == 0 {
		levelMeta.NextCompactionIdx = 0
	} else {
		levelMeta.NextCompactionIdx = (levelMeta.NextCompactionIdx + 1) % len(levelMeta.Files)
	}
}

// keyRangesOverlap returns true if the key ranges [minA, maxA] and [minB, maxB]
// overlap. If either range is empty (nil/zero-length min or max), it overlaps
// with everything as a safety default.
func keyRangesOverlap(minA, maxA, minB, maxB []byte) bool {
	if len(minA) == 0 || len(maxA) == 0 || len(minB) == 0 || len(maxB) == 0 {
		return true
	}
	return bytes.Compare(maxA, minB) >= 0 && bytes.Compare(minA, maxB) <= 0
}

