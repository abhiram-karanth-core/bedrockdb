package db

import (
	"bedrockdb/internal/compaction"
	"bedrockdb/internal/memtable"
	"bedrockdb/internal/sstable"
	"bedrockdb/internal/wal"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	defaultMemtableSize = 64 * 1024 * 1024
	walFileName         = "wal.log"
)

type DB struct {
	mu           sync.RWMutex
	dir          string
	wal          *wal.WAL
	memtable     *memtable.Memtable
	immutable    *memtable.Memtable
	sstables     []*sstable.Reader
	flushing     bool
	compacting   bool
	nextSST      int
	closeOnce    sync.Once
	closeCh      chan struct{}
	flushCond    *sync.Cond
	memtableSize int64
}

func (db *DB) SetMemtableSize(size int64) {
	db.memtableSize = size
	db.memtable.SetMaxSize(size)
}
func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("db: mkdir: %w", err)
	}

	walPath := filepath.Join(dir, walFileName)
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("db: open wal: %w", err)
	}

	db := &DB{
		dir:          dir,
		wal:          w,
		memtable:     memtable.New(defaultMemtableSize),
		closeCh:      make(chan struct{}),
		memtableSize: defaultMemtableSize,
	}
	db.memtable = memtable.New(db.memtableSize)
	db.flushCond = sync.NewCond(&db.mu)
	if err := w.Replay(func(key, value []byte) {
		db.memtable.Put(string(key), string(value))
	}); err != nil {
		return nil, fmt.Errorf("db: wal replay: %w", err)
	}

	if err := db.loadSSTables(); err != nil {
		return nil, fmt.Errorf("db: load sstables: %w", err)
	}

	return db, nil
}

func (db *DB) Put(key, value string) error {
	if err := db.wal.Write([]byte(key), []byte(value)); err != nil {
		return fmt.Errorf("db: wal write: %w", err)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.memtable.Put(key, value)
	fmt.Printf("DEBUG put: size=%d max=%d full=%v flushing=%v\n",
		db.memtable.Size(), db.memtableSize, db.memtable.IsFull(), db.flushing)
	if db.memtable.IsFull() && !db.flushing {
		db.startFlush()
	}

	return nil
}

func (db *DB) Delete(key string) error {
	return db.Put(key, memtable.Tombstone)
}

func (db *DB) Get(key string) (string, bool, error) {
	db.mu.RLock()
	imm := db.immutable
	ssts := db.sstables
	mem := db.memtable
	db.mu.RUnlock()

	if val, found := mem.Get(key); found {
		if memtable.IsTombstone(val) {
			return "", false, nil
		}
		return val, true, nil
	}

	if imm != nil {
		if val, found := imm.Get(key); found {
			if memtable.IsTombstone(val) {
				return "", false, nil
			}
			return val, true, nil
		}
	}

	for _, sst := range ssts {
		val, found, err := sst.Get(key)
		if err != nil {
			return "", false, fmt.Errorf("db: sstable get: %w", err)
		}
		if found {
			if memtable.IsTombstone(val) {
				return "", false, nil
			}
			return val, true, nil
		}
	}

	return "", false, nil
}
func (db *DB) compact() {
	//acquire ownership + snapshot
	db.mu.Lock()
	if db.compacting {
		db.mu.Unlock()
		return
	}
	db.compacting = true

	selected := db.sizeTieredGroup()
	if selected == nil {
		db.compacting = false
		db.mu.Unlock()
		return
	}
	paths := make([]string, len(selected))
	for i, sst := range selected {
		paths[i] = sst.Path
	}
	compacting := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		compacting[p] = struct{}{}
	}
	var externalSSTs []*sstable.Reader
	for _, sst := range db.sstables {
		if _, ok := compacting[sst.Path]; !ok {
			externalSSTs = append(externalSSTs, sst)
		}
	}
	nextSST := db.nextSST
	db.nextSST++
	db.mu.Unlock()

	// release ownership
	defer func() {
		db.mu.Lock()
		db.compacting = false
		db.mu.Unlock()
	}()

	// run compaction
	outputPath, err := compaction.CompactDir(db.dir, paths, nextSST, externalSSTs)
	if err != nil {
		fmt.Printf("db: compact: %v\n", err)
		return
	}

	r, err := sstable.Open(outputPath)
	if err != nil {
		fmt.Printf("db: compact: open: %v\n", err)
		return
	}

	db.mu.Lock()
	compacted := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		compacted[p] = struct{}{}
	}
	for _, p := range paths {
		found := false
		for _, sst := range db.sstables {
			if sst.Path == p {
				found = true
				break
			}
		}
		if !found {
			db.mu.Unlock()
			r.Close()
			return
		}
	}
	remaining := make([]*sstable.Reader, 0, len(db.sstables)-len(paths)+1)
	remaining = append(remaining, r)
	for _, sst := range db.sstables {
		if _, skip := compacted[sst.Path]; skip {
			sst.Close()
		} else {
			remaining = append(remaining, sst)
		}
	}
	db.sstables = remaining
	db.mu.Unlock()
}
func (db *DB) startFlush() {
	db.immutable = db.memtable
	db.memtable = memtable.New(db.memtableSize)
	db.flushing = true
	go db.flush()
}

func (db *DB) flush() {
	fmt.Println("flush: started")
	defer fmt.Println("flush: done")
	defer func() {
		db.mu.Lock()
		db.flushing = false
		db.flushCond.Broadcast()
		db.mu.Unlock()
	}()
	db.mu.Lock()
	sstPath := filepath.Join(db.dir, fmt.Sprintf("sst-%06d.sst", db.nextSST))
	imm := db.immutable
	db.nextSST++
	db.mu.Unlock()

	keyCount := uint(0)
	imm.Ascend(func(key, value string) bool {
		keyCount++
		return true
	})

	w, err := sstable.NewWriter(sstPath, keyCount)
	if err != nil {
		fmt.Printf("db: flush: new writer: %v\n", err)
		return
	}

	imm.Ascend(func(key, value string) bool {
		if err := w.Add(key, value); err != nil {
			fmt.Printf("db: flush: add: %v\n", err)
			return false
		}
		return true
	})

	if err := w.Finish(); err != nil {
		fmt.Printf("db: flush: finish: %v\n", err)
		return
	}

	r, err := sstable.Open(sstPath)
	if err != nil {
		fmt.Printf("db: flush: open reader: %v\n", err)
		return
	}

	db.mu.Lock()
	db.sstables = append([]*sstable.Reader{r}, db.sstables...)
	db.immutable = nil

	db.rotateWALWithoutMutex()
	if db.sizeTieredGroup() != nil && !db.compacting {
		go db.compact()
	}
	db.mu.Unlock()
}

func (db *DB) rotateWAL() {
	db.mu.Lock()
	defer db.mu.Unlock()

	walPath := filepath.Join(db.dir, walFileName)
	db.wal.Close()
	os.Truncate(walPath, 0)
	w, err := wal.Open(walPath)
	if err != nil {
		fmt.Printf("db: rotate wal: %v\n", err)
		return
	}
	db.wal = w
}

func (db *DB) loadSSTables() error {
	pattern := filepath.Join(db.dir, "sst-*.sst")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))
	maxID := -1
	for _, path := range paths {
		r, err := sstable.Open(path)
		if err != nil {
			return fmt.Errorf("db: open sstable %s: %w", path, err)
		}
		db.sstables = append(db.sstables, r)
		base := filepath.Base(path)             // "sst-000011.sst"
		base = strings.TrimPrefix(base, "sst-") // "000011.sst"
		base = strings.TrimSuffix(base, ".sst") // "000011"
		id, err := strconv.Atoi(base)
		if err == nil && id > maxID {
			maxID = id
		}
	}
	db.nextSST = maxID + 1
	return nil
}

func (db *DB) Close() error {
	db.closeOnce.Do(func() { close(db.closeCh) })

	db.mu.Lock()
	defer db.mu.Unlock()

	for db.flushing {
		db.flushCond.Wait()
	}
	if db.memtable.Size() > 0 {
		db.startFlush()
		for db.flushing {
			db.flushCond.Wait()
		}
	}

	db.wal.Close()
	for _, sst := range db.sstables {
		sst.Close()
	}
	return nil
}

func (db *DB) Stats() string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return fmt.Sprintf(
		"memtable_size=%d\nimmutable=%v\nsst_count=%d\n",
		db.memtable.Size(),
		db.immutable != nil,
		len(db.sstables),
	)
}
func (db *DB) SSTables() string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var out strings.Builder

	for i, sst := range db.sstables {
		out.WriteString(fmt.Sprintf("[%d] %s\n", i, sst.Path))
	}

	return out.String()
}

// sizeTieredGroups groups sstables into buckets where max/min size < bucketRatio.
// Returns the first bucket that hits compactionThreshold, or nil.
func (db *DB) sizeTieredGroup() []*sstable.Reader {
	const bucketRatio = 2.0
	const threshold = compaction.L0CompactionThreshold

	if len(db.sstables) == 0 {
		return nil
	}

	// get file sizes
	type sized struct {
		r    *sstable.Reader
		size int64
	}
	files := make([]sized, 0, len(db.sstables))
	for _, sst := range db.sstables {
		info, err := os.Stat(sst.Path)
		if err != nil {
			continue
		}
		files = append(files, sized{sst, info.Size()})
	}

	// sort by size ascending
	sort.Slice(files, func(i, j int) bool {
		return files[i].size < files[j].size
	})

	// slide a window: expand while max/min < bucketRatio
	for i := 0; i < len(files); {
		j := i + 1
		for j < len(files) {
			ratio := float64(files[j].size) / float64(files[i].size)
			if ratio > bucketRatio {
				break
			}
			j++
		}
		// files[i:j] is one bucket
		if j-i >= threshold {
			group := make([]*sstable.Reader, j-i)
			for k, f := range files[i:j] {
				group[k] = f.r
			}
			return group
		}
		i = j
	}
	return nil
}

func (db *DB) rotateWALWithoutMutex() {
	walPath := filepath.Join(db.dir, walFileName)
	db.wal.Close()
	os.Truncate(walPath, 0)
	w, err := wal.Open(walPath)
	if err != nil {
		fmt.Printf("db: rotate wal: %v\n", err)
		return
	}
	db.wal = w
}
