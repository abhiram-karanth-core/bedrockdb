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
	file  *os.File
	fd    uintptr
	index []indexEntry
	bloom *bloom.BloomFilter
	cache *lruCache
}


func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable reader: open: %w", err)
	}

	r := &Reader{
		file:  f,
		fd:    f.Fd(),
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
	fileMagic := binary.LittleEndian.Uint64(footer[16:24])

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
