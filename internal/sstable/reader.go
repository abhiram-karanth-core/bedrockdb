package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/bits-and-blooms/bloom/v3"
)

const defaultCacheCapacity = 256

type Reader struct {
	file     *os.File
	fd       uintptr
	index    []indexEntry
	bloom    *bloom.BloomFilter
	cache    *lruCache
	Path     string
	MinKey   string
	MaxKey   string
	KeyCount uint64
}
// Iterator streams entries one at a time without loading the full file.
type Iterator struct {
	r          *Reader
	blockIdx   int      // current index into r.index
	blockBuf   []Entry  // decoded entries of current block
	posInBlock int      // position within blockBuf
}

func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable reader: open: %w", err)
	}

	r := &Reader{
		file:  f,
		fd:    f.Fd(),
		Path:  path,
		cache: newLRUCache(defaultCacheCapacity),
	}

	if err := r.loadFooter(); err != nil {
		f.Close()
		return nil, err
	}

	return r, nil
}

func (r *Reader) loadFooter() error {
	info, err := r.file.Stat()
	if err != nil {
		return fmt.Errorf("sstable reader: stat: %w", err)
	}
	fileSize := info.Size()

	if fileSize < footerSize {
		return errors.New("sstable reader: file too small")
	}

	var footer [footerSize]byte
	if _, err := r.file.ReadAt(footer[:], fileSize-footerSize); err != nil {
		return fmt.Errorf("sstable reader: read footer: %w", err)
	}

	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	bloomOffset := binary.LittleEndian.Uint64(footer[8:16])
	r.KeyCount  = binary.LittleEndian.Uint64(footer[16:24])  // ← new
	fileMagic  := binary.LittleEndian.Uint64(footer[24:32])

	if fileMagic != magic {
		return fmt.Errorf("sstable reader: invalid magic %x", fileMagic)
	}

	if err := r.loadIndex(indexOffset, bloomOffset); err != nil {
		return err
	}

	if err := r.loadBloom(bloomOffset); err != nil {
		return err
	}

	return nil
}

func (r *Reader) loadIndex(indexOffset, bloomOffset uint64) error {
	indexSize := bloomOffset - indexOffset
	if indexSize == 0 {
		return nil
	}

	buf := make([]byte, indexSize)
	if _, err := r.file.ReadAt(buf, int64(indexOffset)); err != nil {
		return fmt.Errorf("sstable reader: read index: %w", err)
	}

	pos := 0
	for pos < len(buf) {
		if pos+indexEntryHeader > len(buf) {
			break
		}

		keyLen := binary.LittleEndian.Uint32(buf[pos : pos+4])
		blockOffset := binary.LittleEndian.Uint64(buf[pos+4 : pos+12])
		pos += indexEntryHeader

		if pos+int(keyLen) > len(buf) {
			break
		}

		key := string(buf[pos : pos+int(keyLen)])
		pos += int(keyLen)

		r.index = append(r.index, indexEntry{
			firstKey:    key,
			blockOffset: blockOffset,
		})
	}
	if len(r.index) > 0 {
		r.MinKey = r.index[0].firstKey
		// MaxKey = last key in last block, read it now
		lastBlock, err := r.readBlock(r.index[len(r.index)-1].blockOffset)
		if err != nil {
			return fmt.Errorf("sstable reader: read last block for MaxKey: %w", err)
		}
		if len(lastBlock) > 0 {
			r.MaxKey = lastBlock[len(lastBlock)-1].Key
		}
	}

	return nil
}

func (r *Reader) loadBloom(bloomOffset uint64) error {
	var lenBuf [4]byte
	if _, err := r.file.ReadAt(lenBuf[:], int64(bloomOffset)); err != nil {
		return fmt.Errorf("sstable reader: read bloom len: %w", err)
	}
	bloomLen := binary.LittleEndian.Uint32(lenBuf[:])

	bloomBytes := make([]byte, bloomLen)
	if _, err := r.file.ReadAt(bloomBytes, int64(bloomOffset)+4); err != nil {
		return fmt.Errorf("sstable reader: read bloom bytes: %w", err)
	}

	r.bloom = &bloom.BloomFilter{}
	if err := r.bloom.UnmarshalBinary(bloomBytes); err != nil {
		return fmt.Errorf("sstable reader: unmarshal bloom: %w", err)
	}

	return nil
}

func (r *Reader) Get(key string) (string, bool, error) {
	if !r.bloom.TestString(key) {
		return "", false, nil
	}

	blockOffset, ok := r.findBlock(key)
	if !ok {
		return "", false, nil
	}

	entries, ok := r.cache.Get(blockOffset)
	if !ok {
		var err error
		entries, err = r.readBlock(blockOffset)
		if err != nil {
			return "", false, err
		}
		r.cache.Put(blockOffset, entries)
	}

	value, found := SearchBlock(entries, key)
	if !found {
		return "", false, nil
	}

	return value, true, nil
}

func (r *Reader) findBlock(key string) (uint64, bool) {
	if len(r.index) == 0 {
		return 0, false
	}

	i := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].firstKey > key
	})

	if i == 0 {
		return 0, false
	}

	return r.index[i-1].blockOffset, true
}

func (r *Reader) readBlock(offset uint64) ([]Entry, error) {
	buf := make([]byte, BlockSize)

	n, err := r.file.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("sstable reader: pread offset %d: %w", offset, err)
	}
	if n != BlockSize {
		return nil, fmt.Errorf("sstable reader: short read at %d: got %d want %d", offset, n, BlockSize)
	}

	entries, err := DecodeBlock(buf)
	if err != nil {
		return nil, fmt.Errorf("sstable reader: decode block at %d: %w", offset, err)
	}

	return entries, nil
}

func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) SeekBlockFirstIdx(key string) int {
	entries := r.index
	low, high := 0, len(entries)-1
	result := 0
	for low <= high {
		mid := (low + high) / 2
		if entries[mid].firstKey <= key {
			result = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return result
}
func (r *Reader) ScanAll() ([]Entry, error) {
	var all []Entry

	for _, idx := range r.index {
		entries, err := r.readBlock(idx.blockOffset)
		if err != nil {
			return nil, fmt.Errorf("sstable: scan block at %d: %w", idx.blockOffset, err)
		}
		all = append(all, entries...)
	}

	return all, nil
}
func (r *Reader) Scan(start, end string) ([]Entry, error) {
	if len(r.index) == 0 {
		return nil, nil
	}
	blockIdx := r.SeekBlockFirstIdx(start)
	var entries []Entry
	for i := blockIdx; i < len(r.index); i++ {
		offset := r.index[i].blockOffset
		var cached []Entry
		if c, ok := r.cache.Get(offset); ok {
			cached = c
		} else {
			block, err := r.readBlock(offset)
			if err != nil {
				return nil, err
			}
			r.cache.Put(offset, block)
			cached = block
		}
		done := false
		for _, e := range cached {
			if e.Key > end {
				done = true
				break
			}
			if e.Key >= start {
				entries = append(entries, e)
			}
		}
		if done {
			break
		}
	}
	return entries, nil
}
func (r *Reader) ClearCache() {
	r.cache.Clear()
}

func (r *Reader) MightContain(key string) bool {
	return r.bloom.TestString(key)
}


func (r *Reader) NewIterator() *Iterator {
	it := &Iterator{r: r}
	it.loadBlock(0) // prime first block
	return it
}

func (it *Iterator) loadBlock(idx int) {
	if idx >= len(it.r.index) {
		it.blockBuf = nil
		return
	}
	offset := it.r.index[idx].blockOffset
	entries, err := it.r.readBlock(offset)
	if err != nil {
		it.blockBuf = nil
		return
	}
	it.blockIdx = idx
	it.blockBuf = entries
	it.posInBlock = 0
}

func (it *Iterator) Valid() bool {
	return len(it.blockBuf) > 0 && it.posInBlock < len(it.blockBuf)
}

func (it *Iterator) Entry() Entry {
	return it.blockBuf[it.posInBlock]
}

func (it *Iterator) Next() {
	it.posInBlock++
	if it.posInBlock >= len(it.blockBuf) {
		it.loadBlock(it.blockIdx + 1) // advance to next block
	}
}