package memtable

import (
	"sync"

	"github.com/google/btree"
)


const Tombstone = "\x00TOMBSTONE\x00"

type item struct {
	key   string
	value string
}

func (a item) Less(b btree.Item) bool {
	return a.key < b.(item).key
}

type Memtable struct {
	mu      sync.RWMutex
	tree    *btree.BTree
	size    int64
	maxSize int64
}


func New(maxSize int64) *Memtable {
	return &Memtable{
		tree:    btree.New(32),
		maxSize: maxSize,
	}
}

func (m *Memtable) Put(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if old := m.tree.Get(item{key: key}); old != nil {
		o := old.(item)
		m.size -= int64(len(o.key) + len(o.value))
	}

	m.tree.ReplaceOrInsert(item{key: key, value: value})
	m.size += int64(len(key) + len(value))
}

func (m *Memtable) Delete(key string) {
	m.Put(key, Tombstone)
}

func (m *Memtable) Get(key string) (value string, found bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := m.tree.Get(item{key: key})
	if result == nil {
		return "", false
	}
	return result.(item).value, true
}


func (m *Memtable) IsFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size >= m.maxSize
}

func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}


func (m *Memtable) Ascend(fn func(key, value string) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.tree.Ascend(func(i btree.Item) bool {
		it := i.(item)
		return fn(it.key, it.value)
	})
}

func IsTombstone(value string) bool {
	return value == Tombstone
}

func (m *Memtable) SetMaxSize(size int64) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.maxSize = size
}