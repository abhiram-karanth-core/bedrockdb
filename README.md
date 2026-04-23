# BedrockDB

BedrockDB is an LSM key-value storage engine built from scratch — 
write-ahead log with group commit, a sorted memtable, and immutable 
SSTables with bloom-filtered point lookups backed by a pread-based 
LRU block cache. No external storage dependencies.

---

## Architecture

```
Write path:  WAL → Memtable → (flush) → SSTable
Read path:   Memtable → Bloom filter → Sparse index → LRU block cache → Block decode
Range path:  N-way heap merge across Memtable + SSTables → deduplicate → filter tombstones
Crash recovery: WAL replay → Memtable rebuild → resume
Compaction (background): SSTables → N-way heap merge → fewer SSTables (triggered when a size bucket reaches threshold)
```

**Write-ahead log (WAL)** — Group commit batches concurrent writes into a single fsync. Every write is durable before ack. Writers never block waiting for fsync.

**Crash recovery** — On restart, `Open()` replays the WAL into a fresh memtable 
before accepting any reads or writes. Replay stops at the first incomplete or 
corrupt record (expected after a crash — the tail write is partial). All records 
before the corrupt tail are recovered. Once the memtable flushes to an SSTable, 
the WAL is truncated — only unflushed writes need replay. A clean shutdown 
(`Close()`) flushes the memtable and truncates the WAL, so restart is instant 
with nothing to replay.

**Memtable** — Concurrent sorted structure (B-tree) protected by a read-write mutex. Reads take a read lock only — concurrent reads never block each other.

**SSTable** — Immutable on-disk files with a three-layer read path: bloom filter (skip absent keys), binary-searched sparse index (find the right block), LRU block cache (avoid redundant decodes). SSTables are maintained in newest-first order, ensuring recent updates shadow older versions during lookups and compaction.

**Compaction** — Size-tiered compaction groups SSTables into size buckets where max_size / min_size < 2.0 and compacts any bucket that reaches the threshold. Two tiers emerge naturally under typical workloads — small (fresh memtable flushes) and large (previous compaction outputs) — each compacting independently when their bucket fills. Duplicate keys are resolved by recency. Tombstones are dropped during compaction using key range awareness and bloom filters across non-selected SSTables (see design decisions).

---

## Read Amplification

In STCS, read amplification is not structurally bounded to a fixed number of tiers — the number of tiers grows with compaction history and workload. The bound below is empirical for BedrockDB's two-tier steady state under typical write patterns.

### Observed steady state

Under typical workloads, BedrockDB naturally produces two size tiers:

- **Small tier** — fresh memtable flush outputs (~64MB each)
- **Large tier** — outputs of previous compactions (significantly larger)

Each tier compacts independently when its bucket accumulates T files.

### The fixed cost (+2)

Every read always checks:

- Active memtable — contains the most recent writes
- Immutable memtable — a frozen memtable waiting to be flushed

These are always present regardless of compaction state:

```
+1 (active memtable)
+1 (immutable memtable)
= +2
```

### Per-tier cost

Within each tier, a bucket holds at most **T − 1** SSTables before compaction fires. In the worst case, both tiers are just below threshold:

- Small tier: **T − 1 SSTables**
- Large tier: **T − 1 SSTables**

### Two-tier bound

```
active memtable      → 1
immutable memtable   → 1
small-tier SSTables  → T − 1
large-tier SSTables  → T − 1

total = 2(T − 1) + 2
```

For **T = 4**: `2(3) + 2 = 8 sources`

This bound holds as long as the workload produces exactly two tiers. If compaction falls behind and a third tier accumulates, the bound extends to `3(T − 1) + 2`, and so on. Unlike leveled compaction, STCS offers no hard structural guarantee on the number of tiers — the bound is workload-dependent.

Each SSTable probe involves a bloom filter check (cheap) and a possible disk read if the filter passes (expensive). This bound has real latency implications on cache misses.

### The tradeoff

The threshold **T** directly controls the balance between read and write cost:

- Higher **T** → fewer compactions → lower write amplification
- But also → more SSTables per tier → higher read amplification

This is the core tradeoff of size-tiered compaction: you reduce write cost by increasing **T**, but pay for it with increased read fan-out.

---

## SSTable file layout

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

The 4KB block size aligns with Linux page size and NVMe sector size — a single `pread` maps to one page fault and one sector read.

> Not sure how a storage engine differs from a database like PostgreSQL?
> [What BedrockDB is — and what it isn't →](https://bedrockdb-docs.vercel.app/)

> See how to run and use?
> [API & Usage →](usage.md)

---

## Benchmarks

All benchmarks on Linux/amd64, 13th Gen Intel Core i7-13650HX.

### WAL

| Benchmark | Throughput | ns/op |
|---|---|---|
| Group commit (default) | **120 MB/s** | 439 ns/op |
| Fsync-every-write | 0.10 MB/s | 519,012 ns/op |

Group commit delivers a **1,181× throughput advantage** over fsync-per-write. The durability guarantee is identical — both strategies fsync to disk. The difference is how many writes share each fsync.

### Memtable

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| Put | 83 | 64 | 2 |
| Get | 37 | 32 | 1 |
| ConcurrentReadWrite | 319 | — | — |

`Get` is 2× faster than `Put`. `Put` acquires a write lock and may subtract old value size on overwrite. `Get` acquires a read lock only — concurrent reads proceed in parallel.

### SSTable

| Benchmark | ns/op | allocs/op | Notes |
|---|---|---|---|
| BlockEncode | 502 | 0 | Pure memcopy into pre-allocated 4KB buffer |
| BlockDecode | 2,961 | 191 | One allocation per entry (Go string conversion) |
| Get (Memtable) | ~45 | 1 | Pure in-memory lookup |
| Get (SST cache hit) | ~550 | 2 | Bloom check + LRU hit + scan, no decode |
| Get (SST cache miss) | ~4600 | 149 | pread + full block decode |
| BloomFilterReject | 98 | 2 | Absent keys rejected before any I/O |

Cache hit avoids block decode entirely — decoded entries are reused directly from the LRU cache. The ~2 allocations are key/value handling only.

Block decode dominates the cold read path — ~150 allocations per block — but is amortised across subsequent cache hits via the LRU.

**Read path characteristics** — BedrockDB exhibits three distinct latency tiers:

- Memtable hits (~45ns): direct in-memory access
- SSTable cache hits (~550ns): decoded blocks reused via LRU
- SSTable cache misses (~4.6µs): require pread + block decode (~150 allocations)

This separation ensures predictable performance — hot data is served from cache with minimal overhead, while cold reads remain efficient due to 4KB aligned I/O and bounded decode cost.

### Range

| Benchmark | Window | ns/op | Notes |
|---|---|---|---|
| Range (memtable, small) | 100 keys | ~11,250 | In-memory B-tree scan, no I/O |
| Range (memtable, large) | 800 keys | ~81,000 | Same path, 8× more keys |
| Range (SST cache hit) | 100 keys | ~10,900 | Blocks warm in LRU, no pread |
| Range (SST cold, 5 SSTables) | 30k keys | ~5,500,000 | N-way heap merge, full block decodes |

Cache hit (~10,900ns) ≈ memtable (~11,250ns) for the same window — once blocks are warm in the LRU, the SSTable path costs the same as an in-memory scan. 

### DB layer

| Benchmark | ns/op | Notes |
|---|---|---|
| Put | 725 | WAL write (lock-free) + memtable insert |
| Get | 52 | Memtable hit |
| Get (SSTable cache miss) | 4,637 | Full stack: memtable miss → SSTable cold pread |
| MixedWorkload (80% read) | 770 | — |

### Compaction

| Benchmark | Duration | Notes |
|---|---|---|
| 4 SSTables × 500 keys | 2.18ms | N-way heap merge |

---

### HTTP (wrk2, 20 connections, 30s, corrected for coordinated omission)

| Endpoint | RPS | P50 | P90 | P99 |
|---|---|---|---|---|
| PUT | 500 | 0.99ms | 1.60ms | 2.36ms |
| GET (cache hit) | 1000 | 0.91ms | 1.53ms | 1.96ms |
| GET (cache miss, cold) | 1000 | 1.05ms | 1.67ms | 2.13ms |
| GET (absent key) | 2000 | 0.92ms | 1.59ms | 2.05ms |
| RANGE (cold, no cache) | 3000 | 1.39ms | 1.79ms | 2.36ms |

**GET vs GET miss are nearly identical** — bloom filter rejection (98ns) is invisible at the HTTP level. Latency is dominated by HTTP stack overhead, not the storage engine.

**LSM write semantics** — BedrockDB follows LSM tree design. Writes are sequential appends — fast on any storage media. Reads are optimised via bloom filters and LRU block cache to minimise read amplification across SSTables.

---

## Design decisions

**Why 4KB blocks?**
Aligns with `PAGE_SIZE` on Linux. A cache miss triggers one page fault — the OS fetches exactly the data needed. Larger blocks increase read amplification on point lookups; smaller blocks increase index size and reduce LRU effectiveness.

**Why a sparse index?**
One index entry per block, not per key. At 4KB blocks with ~50-byte average keys, a 1GB SSTable needs ~5,000 index entries (~250KB in memory) rather than millions. The index fits in L2 cache during binary search.

**Why group commit in the WAL?**
`fsync` latency on NVMe is 50–200µs regardless of how many bytes were written. Batching N writes into one fsync amortises that latency across N operations. BedrockDB buffers writes in a `bufio.Writer` and a background goroutine fsyncs every 100µs — or immediately when the buffer exceeds 64KB. Writers never block waiting for fsync.

**Why `pread` + LRU over `mmap`?**
`mmap` delegates caching to the OS page cache, which gives up control. The OS evicts pages under memory pressure without regard for access frequency — it doesn't know a block cache hit is worth more than a cold page. It also surfaces I/O errors as `SIGBUS` rather than return values, which are difficult to handle correctly in Go. `pread` keeps the read path explicit: BedrockDB decides which blocks stay resident, eviction policy is LRU over decoded `[]Entry` structs, and errors propagate as normal Go errors. The tradeoff is that `pread` pays a syscall per cache miss whereas `mmap` faults in pages transparently — acceptable because the LRU makes cache misses rare on hot data.

**Why LRU over a simpler cache?**
LRU evicts cold blocks under memory pressure while keeping hot blocks resident. Under a Zipfian access distribution (typical for key-value workloads), a small cache captures a disproportionate fraction of reads. BedrockDB defaults to 256 entries (1MB of decoded blocks).

**Why N-way heap merge for compaction and range queries?**
Both compaction and range queries require merging N sorted streams into one globally sorted output. A min-heap gives O(log N) per element across N iterators. The heap entry carries a source index — on key collision, the lower source index (newer SSTable) wins.

**Why size-tiered compaction?**
Size-tiered groups files of similar size rather than compacting by key range overlap (leveled) or recency (FIFO). This minimises write amplification — files are only rewritten when merged with peers of similar size, not on every level push. The tradeoff is higher read amplification than leveled compaction, mitigated by bloom filters and the LRU block cache. Cassandra uses the same strategy as its default for write-heavy workloads.

**How are tombstones dropped during compaction?**
In leveled compaction, tombstone dropping is straightforward — once a tombstone reaches the bottom level, no older version can exist below it. STCS has no such structural guarantee: a large previously-compacted SST may still hold the original value for a key being deleted in a smaller tier.

BedrockDB uses key range awareness combined with bloom filters to decide whether a tombstone is safe to drop. During compaction of a selected bucket, every tombstone is checked against all non-selected (external) SSTables. For each external SST, BedrockDB first checks whether the key falls within that SST's `[MinKey, MaxKey]` range — a cheap string comparison that eliminates irrelevant files entirely. Only if the key falls within range does it consult the bloom filter. If the bloom filter reports the key might exist, the tombstone is kept. If every external SST either has a non-overlapping range or a bloom miss for that key, the tombstone is safely dropped.

Bloom filters have false positives but never false negatives — a bloom miss is a guarantee of absence. The two-stage check (range first, bloom second) prevents bloom false positives from unrelated SSTs from unnecessarily retaining tombstones.

**Why double-buffer immutable memtable?**
When the active memtable hits the size threshold, it is frozen in place as the immutable memtable and a fresh memtable takes writes immediately. The flush goroutine drains the immutable to an SSTable in the background. Writers never stall waiting for a flush to complete — the double-buffer decouples write throughput from disk I/O latency.

**Why `sync.Cond` over busy-poll in `Close()`?**
The original `Close()` spun in a tight loop checking `db.flushing`. Replaced with `sync.Cond` — the goroutine sleeps until the flush goroutine signals completion. Eliminates CPU waste during shutdown with no change in correctness.

**Why string conversion on block decode?**
`BlockDecode` allocates once per entry — `string(buf[...])` copies bytes out of the block's backing buffer rather than returning a subslice. A `[]byte` subslice would be zero-copy and eliminate the 191 allocs, but it would pin the entire 4KB block buffer in memory for as long as the caller holds the value. The LRU cache evicts the block logically, but the GC cannot collect the backing array until all references are gone — breaking the memory boundedness the cache is designed to provide. The allocation cost is paid once per cache miss and amortised across all subsequent hits on that block.