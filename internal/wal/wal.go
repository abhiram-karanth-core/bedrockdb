package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

// Record format on disk:
// | CRC (4 bytes) | key_len (4 bytes) | val_len (4 bytes) | key | value |

const (
	headerSize      = 12             // CRC(4) + key_len(4) + val_len(4)
	syncInterval    = 100 * time.Microsecond
	syncBufferBytes = 64 * 1024 // 64KB — flush if buffer exceeds this
)

// WAL is a write-ahead log. All writes go here before the memtable.
// Group commit: writes are buffered and fsynced every syncInterval
// or when buffer exceeds syncBufferBytes, whichever comes first.
type WAL struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer

	// group commit
	pending int           // number of writes waiting for fsync
	stopCh  chan struct{}
	doneCh  chan struct{}
}

// Open opens or creates a WAL file at the given path.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open file: %w", err)
	}

	w := &WAL{
		file:   f,
		writer: bufio.NewWriterSize(f, syncBufferBytes),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	// background goroutine: group commit ticker
	go w.syncLoop()

	return w, nil
}

// Write encodes one record and appends it to the WAL buffer.
// The write is not fsynced immediately — the group commit handles that.
func (w *WAL) Write(key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// build header: key_len + val_len
	var header [headerSize]byte
	binary.LittleEndian.PutUint32(header[4:8], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(value)))

	// CRC covers everything after the CRC field itself
	crc := crc32.NewIEEE()
	crc.Write(header[4:])
	crc.Write(key)
	crc.Write(value)
	binary.LittleEndian.PutUint32(header[0:4], crc.Sum32())

	// write header + key + value into the buffer
	if _, err := w.writer.Write(header[:]); err != nil {
		return fmt.Errorf("wal: write header: %w", err)
	}
	if _, err := w.writer.Write(key); err != nil {
		return fmt.Errorf("wal: write key: %w", err)
	}
	if _, err := w.writer.Write(value); err != nil {
		return fmt.Errorf("wal: write value: %w", err)
	}

	w.pending++

	// if buffer is getting full, flush immediately without waiting for ticker
	if w.writer.Buffered() >= syncBufferBytes {
		return w.syncLocked()
	}

	return nil
}

// Sync flushes the buffer and fsyncs to disk.
// Called by the group commit ticker and by Close.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncLocked()
}

// syncLocked is Sync without the lock — caller must hold w.mu.
func (w *WAL) syncLocked() error {
	if w.pending == 0 {
		return nil
	}
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush buffer: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	w.pending = 0
	return nil
}

// syncLoop is the background group commit goroutine.
// Fsyncs every syncInterval regardless of buffer size.
func (w *WAL) syncLoop() {
	defer close(w.doneCh)
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// ignore error here — Sync errors will surface on next Write
			_ = w.Sync()
		case <-w.stopCh:
			// final sync before shutdown
			_ = w.Sync()
			return
		}
	}
}

// Replay reads all valid records from the WAL and calls fn for each one.
// Stops at the first corrupt or incomplete record — this is expected
// behavior after a crash (partial write at the tail).
func (w *WAL) Replay(fn func(key, value []byte)) error {
	// seek to beginning for replay
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek: %w", err)
	}

	reader := bufio.NewReader(w.file)
	var header [headerSize]byte

	for {
		// read header
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// clean end of file or partial write at tail — stop here
				break
			}
			return fmt.Errorf("wal: read header: %w", err)
		}

		storedCRC := binary.LittleEndian.Uint32(header[0:4])
		keyLen := binary.LittleEndian.Uint32(header[4:8])
		valLen := binary.LittleEndian.Uint32(header[8:12])

		// read key + value
		data := make([]byte, keyLen+valLen)
		if _, err := io.ReadFull(reader, data); err != nil {
			if err == io.ErrUnexpectedEOF {
				// partial write — stop cleanly
				break
			}
			return fmt.Errorf("wal: read data: %w", err)
		}

		// verify CRC
		crc := crc32.NewIEEE()
		crc.Write(header[4:])
		crc.Write(data)
		if crc.Sum32() != storedCRC {
			// corrupt record — stop replay here
			break
		}

		fn(data[:keyLen], data[keyLen:])
	}

	return nil
}

// Close syncs and closes the WAL.
func (w *WAL) Close() error {
	close(w.stopCh)
	<-w.doneCh // wait for final sync

	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}