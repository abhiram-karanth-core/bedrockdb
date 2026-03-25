package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/bits-and-blooms/bloom/v3"
)

const (
	footerSize = 24
	magic      = uint64(0xDEADC0FFEEBEEF42)

	indexEntryHeader = 12

	bloomFalsePositiveRate = 0.01
)


type indexEntry struct {
	firstKey    string
	blockOffset uint64
}

type Writer struct {
	file        *os.File
	bufWriter   *bufio.Writer
	currentBlock *Block
	index       []indexEntry
	bloom       *bloom.BloomFilter
	offset      uint64 
}

func NewWriter(path string, estimatedKeys uint) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("sstable writer: create file: %w", err)
	}

	return &Writer{
		file:         f,
		bufWriter:    bufio.NewWriterSize(f, 256*1024), // 256KB write buffer
		currentBlock: NewBlock(),
		bloom:        bloom.NewWithEstimates(estimatedKeys, bloomFalsePositiveRate),
	}, nil
}

func (w *Writer) Add(key, value string) error {

	w.bloom.AddString(key)

	err := w.currentBlock.Add(key, value)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrBlockFull) {
		return fmt.Errorf("sstable writer: add to block: %w", err)
	}


	if err := w.flushBlock(); err != nil {
		return err
	}
	return w.currentBlock.Add(key, value)
}


func (w *Writer) flushBlock() error {
	if w.currentBlock.IsEmpty() {
		return nil
	}

	w.index = append(w.index, indexEntry{
		firstKey:    w.currentBlock.FirstKey(),
		blockOffset: w.offset,
	})

	encoded := w.currentBlock.Encode()
	if _, err := w.bufWriter.Write(encoded); err != nil {
		return fmt.Errorf("sstable writer: write block: %w", err)
	}
	w.offset += uint64(len(encoded))
	w.currentBlock = NewBlock()
	return nil
}

func (w *Writer) Finish() error {

	if err := w.flushBlock(); err != nil {
		return err
	}

	indexOffset := w.offset
	if err := w.writeIndexBlock(); err != nil {
		return err
	}

	bloomOffset := w.offset
	if err := w.writeBloomBlock(); err != nil {
		return err
	}

	if err := w.writeFooter(indexOffset, bloomOffset); err != nil {
		return err
	}


	if err := w.bufWriter.Flush(); err != nil {
		return fmt.Errorf("sstable writer: flush: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sstable writer: fsync: %w", err)
	}

	return w.file.Close()
}


func (w *Writer) writeIndexBlock() error {
	for _, entry := range w.index {
		var header [indexEntryHeader]byte
		binary.LittleEndian.PutUint32(header[0:4], uint32(len(entry.firstKey)))
		binary.LittleEndian.PutUint64(header[4:12], entry.blockOffset)

		if _, err := w.bufWriter.Write(header[:]); err != nil {
			return fmt.Errorf("sstable writer: write index header: %w", err)
		}
		if _, err := w.bufWriter.Write([]byte(entry.firstKey)); err != nil {
			return fmt.Errorf("sstable writer: write index key: %w", err)
		}
		w.offset += uint64(indexEntryHeader + len(entry.firstKey))
	}
	return nil
}

func (w *Writer) writeBloomBlock() error {
	bloomBytes, err := w.bloom.MarshalBinary()
	if err != nil {
		return fmt.Errorf("sstable writer: marshal bloom: %w", err)
	}

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(bloomBytes)))
	if _, err := w.bufWriter.Write(lenBuf[:]); err != nil {
		return fmt.Errorf("sstable writer: write bloom len: %w", err)
	}
	if _, err := w.bufWriter.Write(bloomBytes); err != nil {
		return fmt.Errorf("sstable writer: write bloom: %w", err)
	}
	w.offset += uint64(4 + len(bloomBytes))
	return nil
}

func (w *Writer) writeFooter(indexOffset, bloomOffset uint64) error {
	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint64(footer[8:16], bloomOffset)
	binary.LittleEndian.PutUint64(footer[16:24], magic)

	if _, err := w.bufWriter.Write(footer[:]); err != nil {
		return fmt.Errorf("sstable writer: write footer: %w", err)
	}
	w.offset += footerSize
	return nil
}