package compaction

import (
	"bedrockdb/internal/sstable"
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
)

const (
	L0CompactionThreshold = 4

	MaxLevelSize = 10 * 1024 * 1024 // 10MB for L1
)

type heapEntry struct {
	key    string
	value  string
	sstIdx int
	iter   *sstIterator
}

type minHeap []heapEntry

func (h minHeap) Len() int      { return len(h) }
func (h minHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h minHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	return h[i].sstIdx < h[j].sstIdx
}
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(heapEntry)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type sstIterator struct {
	entries []sstable.Entry
	pos     int
}

func newSSTableIterator(path string) (*sstIterator, error) {
	r, err := sstable.Open(path)
	if err != nil {
		return nil, fmt.Errorf("compaction: open sstable %s: %w", path, err)
	}
	defer r.Close()

	entries, err := r.ScanAll()
	if err != nil {
		return nil, fmt.Errorf("compaction: scan %s: %w", path, err)
	}

	return &sstIterator{entries: entries}, nil
}

func (it *sstIterator) hasNext() bool {
	return it.pos < len(it.entries)
}

func (it *sstIterator) next() sstable.Entry {
	e := it.entries[it.pos]
	it.pos++
	return e
}

func Compact(inputPaths []string, outputPath string, isDeepest bool) error {
	if len(inputPaths) == 0 {
		return nil
	}

	iters := make([]*sstIterator, len(inputPaths))
	for i, path := range inputPaths {
		iter, err := newSSTableIterator(path)
		if err != nil {
			return err
		}
		iters[i] = iter
	}

	totalKeys := uint(0)
	for _, iter := range iters {
		totalKeys += uint(len(iter.entries))
	}

	h := &minHeap{}
	heap.Init(h)

	for i, iter := range iters {
		if iter.hasNext() {
			e := iter.next()
			heap.Push(h, heapEntry{
				key:    e.Key,
				value:  e.Value,
				sstIdx: i,
				iter:   iter,
			})
		}
	}

	w, err := sstable.NewWriter(outputPath, totalKeys)
	if err != nil {
		return fmt.Errorf("compaction: new writer: %w", err)
	}

	lastKey := ""

	for h.Len() > 0 {
		entry := heap.Pop(h).(heapEntry)

		if entry.key == lastKey {
			if entry.iter.hasNext() {
				e := entry.iter.next()
				heap.Push(h, heapEntry{
					key:    e.Key,
					value:  e.Value,
					sstIdx: entry.sstIdx,
					iter:   entry.iter,
				})
			}
			continue
		}

		lastKey = entry.key

		if entry.value == "\x00TOMBSTONE\x00" {
			if !isDeepest {
				if err := w.Add(entry.key, entry.value); err != nil {
					return fmt.Errorf("compaction: add tombstone: %w", err)
				}
			}
		} else {
			if err := w.Add(entry.key, entry.value); err != nil {
				return fmt.Errorf("compaction: add: %w", err)
			}
		}

		if entry.iter.hasNext() {
			e := entry.iter.next()
			heap.Push(h, heapEntry{
				key:    e.Key,
				value:  e.Value,
				sstIdx: entry.sstIdx,
				iter:   entry.iter,
			})
		}
	}

	return w.Finish()
}

func CompactDir(dir string, l0Paths []string, nextSST int) (string, error) {
	outputPath := filepath.Join(dir, fmt.Sprintf("sst-%06d.sst", nextSST))
	if err := Compact(l0Paths, outputPath, true); err != nil {
		return "", err
	}
	for _, path := range l0Paths {
		if err := os.Remove(path); err != nil {
			return "", fmt.Errorf("compaction: remove %s: %w", path, err)
		}
	}

	return outputPath, nil
}
