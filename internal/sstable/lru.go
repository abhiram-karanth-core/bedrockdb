package sstable

import (
	"container/list"
	"sync"
)

type lruEntry struct {
	offset  uint64
	entries []Entry
}

type lruCache struct {
	mu       sync.Mutex
	capacity int
	list     *list.List
	items    map[uint64]*list.Element
}

func newLRUCache(capacity int) *lruCache {
	return &lruCache{
		capacity: capacity,
		list:     list.New(),
		items:    make(map[uint64]*list.Element, capacity),
	}
}

func (c *lruCache) Get(offset uint64) ([]Entry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[offset]
	if !ok {
		return nil, false
	}
	c.list.MoveToFront(elem)
	return elem.Value.(*lruEntry).entries, true
}

func (c *lruCache) Put(offset uint64, entries []Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[offset]; ok {
		c.list.MoveToFront(elem)
		elem.Value.(*lruEntry).entries = entries
		return
	}

	if c.list.Len() >= c.capacity {
		c.evict()
	}

	entry := &lruEntry{offset: offset, entries: entries}
	elem := c.list.PushFront(entry)
	c.items[offset] = elem
}

func (c *lruCache) evict() {
	back := c.list.Back()
	if back == nil {
		return
	}
	c.list.Remove(back)
	delete(c.items, back.Value.(*lruEntry).offset)
}

func (c *lruCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.list.Len()
}
func (c *lruCache) Clear() {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.list.Init()
    c.items = make(map[uint64]*list.Element, c.capacity)
}