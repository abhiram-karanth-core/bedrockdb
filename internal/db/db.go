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
	"sync"
	"time"
)

const (
	defaultMemtableSize = 64 * 1024 * 1024
	walFileName         = "wal.log"
)

type DB struct {
	mu         sync.RWMutex
	dir        string
	wal        *wal.WAL
	memtable   *memtable.Memtable
	immutable  *memtable.Memtable
	sstables   []*sstable.Reader
	flushing   bool
	compacting bool
	nextSST    int
	closeOnce  sync.Once
	closeCh    chan struct{}
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
		dir:      dir,
		wal:      w,
		memtable: memtable.New(defaultMemtableSize),
		closeCh:  make(chan struct{}),
	}

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
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.Write([]byte(key), []byte(value)); err != nil {
		return fmt.Errorf("db: wal write: %w", err)
	}

	db.memtable.Put(key, value)

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
	defer db.mu.RUnlock()

	if val, found := db.memtable.Get(key); found {
		if memtable.IsTombstone(val) {
			return "", false, nil
		}
		return val, true, nil
	}

	if db.immutable != nil {
		if val, found := db.immutable.Get(key); found {
			if memtable.IsTombstone(val) {
				return "", false, nil
			}
			return val, true, nil
		}
	}

	for _, sst := range db.sstables {
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

	k := compaction.L0CompactionThreshold
	if len(db.sstables) < k {
		db.compacting = false
		db.mu.Unlock()
		return
	}
	selected := db.sstables[:k]
	paths := make([]string, k)
	for i, sst := range selected {
		paths[i] = sst.Path
	}
	nextSST := db.nextSST
	db.mu.Unlock()

	// release ownership
	defer func() {
		db.mu.Lock()
		db.compacting = false
		db.mu.Unlock()
	}()

	// run compaction
	outputPath, err := compaction.CompactDir(db.dir, paths, nextSST)
	if err != nil {
		fmt.Printf("db: compact: %v\n", err)
		return
	}

	r, err := sstable.Open(outputPath)
	if err != nil {
		fmt.Printf("db: compact: open: %v\n", err)
		return
	}

	//install result safely
	db.mu.Lock()

	// state validation: abort if SST set changed during compaction
	for i := 0; i < k; i++ {
		if db.sstables[i].Path != paths[i] {
			db.mu.Unlock()
			r.Close()
			return
		}
	}
	remaining := make([]*sstable.Reader, len(db.sstables)-k)
	copy(remaining, db.sstables[k:])
	for _, sst := range selected {
		sst.Close()
	}
	db.sstables = append([]*sstable.Reader{r}, remaining...)
	db.nextSST++

	db.mu.Unlock()
}
func (db *DB) startFlush() {
	db.immutable = db.memtable
	db.memtable = memtable.New(defaultMemtableSize)
	db.flushing = true
	go db.flush()
}

func (db *DB) flush() {
	fmt.Println("flush: started")
	defer fmt.Println("flush: done")
	defer func() {
		db.mu.Lock()
		db.flushing = false
		db.mu.Unlock()
	}()
	db.mu.RLock()
	sstPath := filepath.Join(db.dir, fmt.Sprintf("sst-%06d.sst", db.nextSST))
	imm := db.immutable
	db.mu.RUnlock()

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
	db.nextSST++
	db.immutable = nil
	// db.flushing = false

	if len(db.sstables) >= compaction.L0CompactionThreshold && !db.compacting {
		go db.compact()
	}
	db.mu.Unlock()

	db.rotateWAL()
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

	for _, path := range paths {
		r, err := sstable.Open(path)
		if err != nil {
			return fmt.Errorf("db: open sstable %s: %w", path, err)
		}
		db.sstables = append(db.sstables, r)
	}

	db.nextSST = len(paths)
	return nil
}

func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		close(db.closeCh)
	})

	db.mu.Lock()

	for db.flushing {
		db.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		db.mu.Lock()
	}

	if db.memtable.Size() > 0 {
		db.startFlush()
		db.mu.Unlock()
		for {
			time.Sleep(10 * time.Millisecond)
			db.mu.Lock()
			if !db.flushing {
				break
			}
			db.mu.Unlock()
		}
	}

	defer db.mu.Unlock()

	db.wal.Close()
	for _, sst := range db.sstables {
		sst.Close()
	}

	return nil
}
