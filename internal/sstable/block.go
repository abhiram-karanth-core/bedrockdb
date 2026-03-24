package sstable

import (
	"encoding/binary"
	"errors"
)

const (
	BlockSize     = 4096 // 4KB — aligning with Linux page size and NVMe sector size
	blockTrailer  = 4    // num_entries (4 bytes) at end of block
	entryHeader   = 8    // key_len (4 bytes) + val_len (4 bytes)
)

var ErrBlockFull = errors.New("block full")


type Entry struct {
	Key   string
	Value string 
}


// Layout on disk:
// [ entry 0 ][ entry 1 ]...[ entry N ][ num_entries (4 bytes) ]

// Each entry:
// | key_len (4 bytes) | val_len (4 bytes) | key bytes | value bytes |
type Block struct {
	entries []Entry
	size    int 
}

func NewBlock() *Block {
	return &Block{}
}


func (b *Block) Add(key, value string) error {
	needed := entryHeader + len(key) + len(value)
	if b.size+needed+blockTrailer > BlockSize {
		return ErrBlockFull
	}
	b.entries = append(b.entries, Entry{Key: key, Value: value})
	b.size += needed
	return nil
}


func (b *Block) FirstKey() string {
	if len(b.entries) == 0 {
		return ""
	}
	return b.entries[0].Key
}

func (b *Block) Entries() []Entry {
	return b.entries
}


func (b *Block) IsEmpty() bool {
	return len(b.entries) == 0
}


func (b *Block) Encode() []byte {
	buf := make([]byte, BlockSize)
	offset := 0

	for _, e := range b.entries {
		// key_len
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(e.Key)))
		offset += 4
		// val_len
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(e.Value)))
		offset += 4
		// key bytes
		copy(buf[offset:], e.Key)
		offset += len(e.Key)
		// value bytes
		copy(buf[offset:], e.Value)
		offset += len(e.Value)
	}

	binary.LittleEndian.PutUint32(buf[BlockSize-4:], uint32(len(b.entries)))

	return buf
}



func DecodeBlock(buf []byte) ([]Entry, error) {
	if len(buf) != BlockSize {
		return nil, errors.New("block: invalid size")
	}

	numEntries := binary.LittleEndian.Uint32(buf[BlockSize-4:])
	if numEntries == 0 {
		return nil, nil
	}

	entries := make([]Entry, 0, numEntries)
	offset := 0

	for i := uint32(0); i < numEntries; i++ {
		if offset+entryHeader > BlockSize-blockTrailer {
			return nil, errors.New("block: corrupt — entry header out of bounds")
		}

		keyLen := binary.LittleEndian.Uint32(buf[offset:])
		offset += 4
		valLen := binary.LittleEndian.Uint32(buf[offset:])
		offset += 4

		if offset+int(keyLen)+int(valLen) > BlockSize-blockTrailer {
			return nil, errors.New("block: corrupt — entry data out of bounds")
		}

		key := string(buf[offset : offset+int(keyLen)])
		offset += int(keyLen)

		value := string(buf[offset : offset+int(valLen)])
		offset += int(valLen)

		entries = append(entries, Entry{Key: key, Value: value})
	}

	return entries, nil
}


func SearchBlock(entries []Entry, key string) (string, bool) {
	for _, e := range entries {
		if e.Key == key {
			return e.Value, true
		}
	
		if e.Key > key {
			break
		}
	}
	return "", false
}