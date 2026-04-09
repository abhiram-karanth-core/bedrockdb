package db

import (
	"bedrockdb/internal/memtable"
	"bedrockdb/internal/sstable"
	"container/heap"
	"fmt"
)

type RangeEntry struct {
    Key   string `json:"key"`
    Value string `json:"value"`
}

type rangeHeapEntry struct {
	key    string
	value  string
	srcIdx int // lower = newer source
}

type rangeMinHeap []rangeHeapEntry

func (h rangeMinHeap) Len() int      { return len(h) }
func (h rangeMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h rangeMinHeap) Less(i, j int) bool {
	if h[i].key != h[j].key {
		return h[i].key < h[j].key
	}
	return h[i].srcIdx < h[j].srcIdx
}
func (h *rangeMinHeap) Push(x interface{}) { *h = append(*h, x.(rangeHeapEntry)) }
func (h *rangeMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}


func (db *DB) Range(start, end string) ([]RangeEntry, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// collect filtered slices from each source — index 0 = newest
	var sources [][]sstable.Entry

	// source 0 — active memtable
	var memEntries []sstable.Entry
	db.memtable.Ascend(func(key, value string) bool {
		if key > end {
			return false // stop early
		}
		if key >= start {
			memEntries = append(memEntries, sstable.Entry{Key: key, Value: value})
		}
		return true
	})
	sources = append(sources, memEntries)

	var immEntries []sstable.Entry
	if db.immutable != nil {
		db.immutable.Ascend(func(key, value string) bool {
			if key > end {
				return false
			}
			if key >= start {
				immEntries = append(immEntries, sstable.Entry{Key: key, Value: value})
			}
			return true
		})
	}
	sources = append(sources, immEntries)

	for _, sst := range db.sstables {
		filtered, err := sst.Scan(start, end)
		if err !=nil{
			return nil, fmt.Errorf("db: range scan: %w", err)
		}
		sources = append(sources, filtered)
	}

	h := &rangeMinHeap{}
	heap.Init(h)
	positions := make([]int, len(sources))

	for i, src := range sources {
		if len(src) > 0 {
			heap.Push(h, rangeHeapEntry{
				key:    src[0].Key,
				value:  src[0].Value,
				srcIdx: i,
			})
			positions[i] = 1
		}
	}

	
	var result []RangeEntry
	lastKey := ""

	for h.Len() > 0 {
		entry := heap.Pop(h).(rangeHeapEntry)

		pos := positions[entry.srcIdx]
		src := sources[entry.srcIdx]
		if pos < len(src) {
			heap.Push(h, rangeHeapEntry{
				key:    src[pos].Key,
				value:  src[pos].Value,
				srcIdx: entry.srcIdx,
			})
			positions[entry.srcIdx]++
		}

		if entry.key == lastKey {
			continue
		}
		lastKey = entry.key

		if memtable.IsTombstone(entry.value) {
			continue
		}

		result = append(result, RangeEntry{Key: entry.key, Value: entry.value})
	}

	return result, nil
}