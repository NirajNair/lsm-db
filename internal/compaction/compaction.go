package compaction

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/NirajNair/lsm-db/internal/manifest"
	"github.com/NirajNair/lsm-db/internal/sstable"
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

// Execute runs a compaction by creating iterators for source+target files,
// merging them via MergeAndSplit, and returning the results.
// It generates output file paths using startSeqNum as the base sequence number.
// The caller is responsible for advancing SSTSeqNum under the appropriate lock.
func Execute(c *Compaction, sstDir string, startSeqNum uint) ([]*sstable.WriteResult, error) {
	var iters []*sstable.SSTIter
	var priorities []int
	var iterIndices []int

	// Source files get priority 0 (highest), iterIndices 0..len(source)-1
	for i, f := range c.SourceFiles {
		sst := &sstable.SSTable{Path: f.Path}
		iter, err := sst.NewIterator()
		if err != nil {
			return nil, fmt.Errorf("create iterator for %s: %w", f.Path, err)
		}
		iters = append(iters, iter)
		priorities = append(priorities, 0)
		iterIndices = append(iterIndices, i)
	}

	// Target files get priority 1, iterIndices continue from len(source)
	for i, f := range c.TargetFiles {
		sst := &sstable.SSTable{Path: f.Path}
		iter, err := sst.NewIterator()
		if err != nil {
			return nil, fmt.Errorf("create iterator for %s: %w", f.Path, err)
		}
		iters = append(iters, iter)
		priorities = append(priorities, 1)
		iterIndices = append(iterIndices, len(c.SourceFiles)+i)
	}

	// Generate file paths for output SSTables.
	// Worst case: all files merge into one, but may need multiple.
	// Pre-allocate paths for up to len(source) + len(target) output files.
	maxOutputs := max(len(c.SourceFiles)+len(c.TargetFiles), 1)
	var filePaths []string
	for i := range maxOutputs {
		filePaths = append(filePaths, fmt.Sprintf("%s/data-%d.sstable", sstDir, startSeqNum+uint(i)))
	}

	results, err := sstable.MergeAndSplit(iters, priorities, iterIndices, filePaths, c.IsLastLevel)
	if err != nil {
		return nil, err
	}

	return results, nil
}

// ApplyCompaction updates the manifest in-memory state after a successful
// compaction: removes source and target files from their levels, adds new
// output files to the target level, recalculates sizes, sorts by MinKey,
// and advances the round-robin cursor.
func ApplyCompaction(c *Compaction, results []*sstable.WriteResult, m *manifest.Manifest) {
	// 1. Remove source files from source level
	sourcePaths := make(map[string]bool)
	for _, f := range c.SourceFiles {
		sourcePaths[f.Path] = true
	}
	newSourceFiles := make([]*manifest.FileMetadata, 0)
	for _, f := range m.Levels[c.Level].Files {
		if !sourcePaths[f.Path] {
			newSourceFiles = append(newSourceFiles, f)
		}
	}
	m.Levels[c.Level].Files = newSourceFiles
	m.Levels[c.Level].CurrentSize = 0
	for _, f := range newSourceFiles {
		m.Levels[c.Level].CurrentSize += f.Size
	}

	// 2. Remove target files from target level
	targetPaths := make(map[string]bool)
	for _, f := range c.TargetFiles {
		targetPaths[f.Path] = true
	}
	newTargetFiles := make([]*manifest.FileMetadata, 0)
	for _, f := range m.Levels[c.TargetLevel].Files {
		if !targetPaths[f.Path] {
			newTargetFiles = append(newTargetFiles, f)
		}
	}

	// 3. Add new files to target level
	m.Levels[c.TargetLevel].Files = newTargetFiles
	for _, r := range results {
		m.Levels[c.TargetLevel].Files = append(m.Levels[c.TargetLevel].Files, &manifest.FileMetadata{
			Path:   r.SST.Path,
			MinKey: r.MinKey,
			MaxKey: r.MaxKey,
			Size:   r.Size,
		})
	}

	// 4. Recalculate target level size
	m.Levels[c.TargetLevel].CurrentSize = 0
	for _, f := range m.Levels[c.TargetLevel].Files {
		m.Levels[c.TargetLevel].CurrentSize += f.Size
	}

	// 5. Sort target level by MinKey
	sort.Slice(m.Levels[c.TargetLevel].Files, func(i, j int) bool {
		return bytes.Compare(m.Levels[c.TargetLevel].Files[i].MinKey, m.Levels[c.TargetLevel].Files[j].MinKey) < 0
	})

	// 6. Advance round-robin cursor for the source level
	UpdateNextCompactionIdx(m, c.Level)
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
