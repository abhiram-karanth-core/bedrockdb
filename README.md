# BedrockDB

BedrockDB is an embedded key-value storage engine built from scratch — 
write-ahead log with group commit, a sorted memtable, and immutable 
SSTables with bloom-filtered point lookups backed by a pread-based 
LRU block cache. No external storage dependencies.

---

## Architecture

```
Write path:  WAL → Memtable → (flush) → SSTable
Read path:   Memtable → Bloom filter → Index → LRU block cache → Block decode
```

**Write-ahead log (WAL)** — Group commit batches concurrent writes into a single fsync. Every write is durable before ack.

**Memtable** — Concurrent sorted structure (B-tree) protected by a read-write mutex.
 Reads take a read lock only; concurrent reads never block each other.

**SSTable** — Immutable on-disk files with a three-layer read path: bloom filter (skip absent keys), binary-searched sparse index (find the right block), LRU block cache (avoid redundant decodes).

### SSTable file layout

```
[ data block 0 ][ data block 1 ]...[ data block N ]  — 4KB each, page-aligned
[ index block ]                                        — sparse index: firstKey → blockOffset
[ bloom filter ]                                       — serialised bloom filter bytes, length-prefixed
[ footer ]                                             — indexOffset (8B) | bloomOffset (8B) | magic (8B)
```

Each data block encodes entries as:

```
| key_len (4B) | val_len (4B) | key bytes | value bytes | ... | num_entries (4B) |
```

The 4KB block size aligns with Linux page size and NVMe sector size, so a single `pread` maps to one page fault and one sector read.

---

## Benchmarks

All benchmarks on Linux/amd64, 13th Gen Intel Core i7-13650HX.

### WAL

| Benchmark | Throughput | ns/op |
|---|---|---|
| Group commit (default) | **120 MB/s** | 439 ns/op |
| Fsync-every-write | 0.10 MB/s | — |

Group commit delivers a **1,181× throughput advantage** over fsync-per-write. The durability guarantee is identical — both strategies fsync to disk before acknowledging the write. The difference is how many writes share each fsync.

### Memtable

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| BenchmarkPut | 83 | 64 | 2 |
| BenchmarkGet | 37 | 32 | 1 |
| BenchmarkConcurrentReadWrite | 319 | — | — |

`Get` is 2× faster than `Put`. `Put` acquires a write lock and may subtract the old value size on overwrite. `Get` acquires a read lock only — concurrent reads proceed in parallel.

### SSTable blocks

| Benchmark | ns/op | allocs/op | Notes |
|---|---|---|---|
| BenchmarkBlockEncode | 502 | 0 | Pure memcopy into pre-allocated 4KB buffer |
| BenchmarkBlockDecode | 2,961 | 191 | One allocation per entry (Go string conversion) |
| BenchmarkGetCacheHit | 237 | 0 | Bloom check + LRU lookup + linear scan, no decode |
| BenchmarkGetCacheMiss | 11,173 | 373 | Includes block read + decode |
| BenchmarkBloomFilterReject | 98 | 2 | Absent keys rejected before any I/O |

**Cache hit (237ns, 0 allocs)** — The hot path allocates nothing. Bloom filter test, LRU map lookup, and linear scan over already-decoded entries all stay on the stack or in existing heap objects.

**Block decode (191 allocs)** — Each `string(buf[...])` in `DecodeBlock` is a heap copy. This is a known tradeoff: correctness and simplicity first. The optimisation path is `unsafe.String` or keeping entries as `[]byte` until the caller needs a string. The LRU cache makes this a cold-path cost — a block is decoded once, then served from cache on subsequent reads.

**Bloom filter reject (98ns)** — Absent keys are rejected before touching the index or issuing any `pread`. This is the dominant case under a typical read-heavy workload with point lookups on non-existent keys.

---

## Design decisions

**Why 4KB blocks?**
Aligns with `PAGE_SIZE` on Linux. A cache miss triggers one page fault; the OS fetches exactly the data needed. Larger blocks increase read amplification on point lookups; smaller blocks increase index size and reduce LRU effectiveness.

**Why a sparse index?**
One index entry per block, not per key. At 4KB blocks with ~50-byte average keys, a 1GB SSTable needs ~5,000 index entries (~250KB in memory) rather than millions. The index fits in L2 cache during binary search.

**Why group commit in the WAL?**
`fsync` latency on NVMe is 50–200µs regardless of how many bytes were written. Batching N writes into one fsync amortises that latency across N operations. BedrockDB's WAL buffers writes in a bufio.Writer and a background 
goroutine fsyncs every 100µs — or immediately when the buffer 
exceeds 64KB. Writers never block waiting for fsync.

**Why `pread` + LRU over `mmap`?**
`mmap` delegates caching to the OS page cache, which sounds appealing but gives up control. The OS evicts pages under memory pressure without regard for access frequency — it doesn't know a block cache hit is worth more than a cold page. It also surfaces I/O errors as `SIGBUS` rather than return values, which are difficult to handle correctly in Go. `pread` keeps the read path explicit: BedrockDB decides which blocks stay resident, eviction policy is LRU over decoded `[]Entry` structs (not raw bytes), and errors propagate as normal Go errors. The tradeoff is that `pread` pays a syscall per cache miss whereas `mmap` faults in pages transparently — acceptable here because the LRU is designed to make cache misses rare on hot data.

**Why LRU over a simpler cache?**
LRU evicts cold blocks under memory pressure while keeping hot blocks resident. Under a Zipfian access distribution (typical for key-value workloads), a small cache captures a disproportionate fraction of reads. BedrockDB defaults to 256 entries (1MB of decoded blocks).

---

