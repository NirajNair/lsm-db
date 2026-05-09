package sstable

import (
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"sort"

	"github.com/NirajNair/lsm-db/internal/manifest"
	"github.com/NirajNair/lsm-db/internal/utils"
)

type mergeEntry struct {
	key       []byte
	value     []byte
	priority  int
	iterIndex int
}

type mergeHeap []*mergeEntry

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	if h[i].priority != h[j].priority {
		return h[i].priority < h[j].priority
	}
	return h[i].iterIndex > h[j].iterIndex
}
func (h mergeHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x interface{}) { *h = append(*h, x.(*mergeEntry)) }
func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func MergeAndSplit(
	iters []*SSTIter,
	priorities []int,
	iterIndices []int,
	filePaths []string,
	isLastLevel bool,
) ([]*WriteResult, error) {
	if len(iters) == 0 {
		return nil, nil
	}

	h := make(mergeHeap, 0)
	for i, iter := range iters {
		key, value, err := iter.Next()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("iterator %d failed: %w", i, err)
		}
		heap.Push(&h, &mergeEntry{
			key:       key,
			value:     value,
			priority:  priorities[i],
			iterIndex: iterIndices[i],
		})
	}

	var results []*WriteResult
	var entries []Entry
	var accumulatedSize uint
	var fileIdx int
	prevKey := make([]byte, 0)

	for h.Len() > 0 {
		entry := heap.Pop(&h).(*mergeEntry)

		nextKey, nextValue, err := iters[entry.iterIndex].Next()
		if err == nil {
			heap.Push(&h, &mergeEntry{
				key:       nextKey,
				value:     nextValue,
				priority:  priorities[entry.iterIndex],
				iterIndex: entry.iterIndex,
			})
		} else if err != io.EOF {
			return nil, fmt.Errorf("iterator %d failed: %w", entry.iterIndex, err)
		}

		if bytes.Equal(entry.key, prevKey) {
			continue
		}

		if isLastLevel && IsTombstone(entry.value) {
			prevKey = append(prevKey[:0], entry.key...)
			continue
		}

		entries = append(entries, Entry{Key: entry.key, Value: entry.value})
		tempBuf := make([]byte, 0)
		accumulatedSize += uint(len(utils.EncodeEntry(tempBuf, entry.key, entry.value)))
		prevKey = append(prevKey[:0], entry.key...)

		if accumulatedSize >= manifest.MaxFileSize && fileIdx < len(filePaths) {
			result, err := WriteSSTFromEntries(entries, filePaths[fileIdx])
			if err != nil {
				return nil, fmt.Errorf("write SSTable %s: %w", filePaths[fileIdx], err)
			}
			results = append(results, result)
			entries = nil
			accumulatedSize = 0
			fileIdx++
		}
	}

	if len(entries) > 0 && fileIdx < len(filePaths) {
		result, err := WriteSSTFromEntries(entries, filePaths[fileIdx])
		if err != nil {
			return nil, fmt.Errorf("write SSTable %s: %w", filePaths[fileIdx], err)
		}
		results = append(results, result)
	}

	sort.Slice(results, func(i, j int) bool {
		return bytes.Compare(results[i].MinKey, results[j].MinKey) < 0
	})

	return results, nil
}

