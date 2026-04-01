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
```

**Write-ahead log (WAL)** — Group commit batches concurrent writes into a single fsync. Every write is durable before ack. Writers never block waiting for fsync.

**Memtable** — Concurrent sorted structure (B-tree) protected by a read-write mutex. Reads take a read lock only — concurrent reads never block each other.

**SSTable** — Immutable on-disk files with a three-layer read path: bloom filter (skip absent keys), binary-searched sparse index (find the right block), LRU block cache (avoid redundant decodes). SSTables are maintained in newest-first order, ensuring recent updates shadow older versions during lookups and compaction.

**Compaction** — Size-tiered compaction groups SSTables into size buckets where max_size / min_size < 2.0 and compacts any bucket that reaches the threshold. Two tiers emerge naturally — small (fresh memtable flushes) and large (previous compaction outputs) — each compacting independently when their bucket fills. Duplicate keys are resolved by recency and tombstones are dropped at compaction time. Read amplification is bounded at 2 * (threshold - 1) + 2 sources per lookup.

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

The 4KB block size aligns with Linux page size and NVMe sector size — a single `pread` maps to one page fault and one sector read.

> Not sure how a storage engine differs from a database like PostgreSQL?
> [What BedrockDB is — and what it isn't →](https://bedrockdb-docs.vercel.app/)

---

## HTTP API

```bash
# write
curl -X PUT http://localhost:8080/key/{key} \
  -H "Content-Type: application/json" \
  -d '{"value": "..."}'

# read
curl http://localhost:8080/key/{key}

# delete
curl -X DELETE http://localhost:8080/key/{key}

# range query — returns all keys in [start, end] inclusive, sorted
curl "http://localhost:8080/range?start=a&end=z"
```

### Example responses

```bash
# PUT
curl -X PUT http://localhost:8080/key/city \
  -H "Content-Type: application/json" \
  -d '{"value": "bangalore"}'
# {"key":"city","value":"bangalore"}

# GET
curl http://localhost:8080/key/city
# {"key":"city","value":"bangalore"}

# RANGE
curl "http://localhost:8080/range?start=city&end=name"
# [{"key":"city","value":"bangalore"},{"key":"name","value":"alice"}]

# GET deleted key
curl http://localhost:8080/key/city
# {"error":"not found"}
```

---
## CLI (Interactive Shell)

BedrockDB provides an interactive CLI for local debugging and inspection of the storage engine.
```bash
go run cmd/cli/main.go
```

**Supported commands**
```
# basic operations
put <key> <value>
get <key>
delete <key>
range <start> <end>

# observability
stats
sstables

# exit
exit
```

**Example**
```
bedrockdb> put user:1 abhiram
OK

bedrockdb> get user:1
abhiram

bedrockdb> stats
memtable_size=3
immutable=false
sst_count=1

bedrockdb> sstables
[0] data/sst-000001.sst
```

---

## Run
```bash
# HTTP server
go run cmd/server/main.go

# CLI
go run cmd/cli/main.go

# docker
docker compose up --build
```

Server listens on `:8080`. Data directory defaults to `./data`.
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
| GetCacheHit | 237 | 0 | Bloom check + LRU lookup + linear scan, no decode |
| GetCacheMiss | 11,173 | 373 | pread + decode |
| BloomFilterReject | 98 | 2 | Absent keys rejected before any I/O |

Cache hit at 237ns allocates nothing — bloom filter test, LRU map lookup, and linear scan over already-decoded entries stay on the stack or in existing heap objects. Block decode cost (191 allocs) is paid once per cache miss, then amortised across all subsequent reads of that block.

### DB layer

| Benchmark | ns/op | Notes |
|---|---|---|
| Put | 725 | WAL write + memtable insert |
| Get | 52 | Memtable hit |
| MixedWorkload (80% read) | 770 | — |

### Compaction

| Benchmark | Duration | Notes |
|---|---|---|
| 4 SSTables × 500 keys | 2.18ms | N-way heap merge |

### HTTP (wrk2, 20 connections, 30s, corrected for coordinated omission)

| Endpoint | RPS | P50 | P90 | P99 |
|---|---|---|---|---|
| PUT | 500 | 1.36ms | 2.71ms | 521ms |
| GET (cache hit) | 2000 | 1.06ms | 1.76ms | 625ms |
| GET (absent key) | 2000 | 1.04ms | 1.80ms | 621ms |
| RANGE | 200 | 4.58ms | 7.26ms | 622ms |

**GET vs GET miss are nearly identical** — bloom filter rejection (98ns) is invisible at the HTTP level. Latency is dominated by HTTP stack overhead, not the storage engine.

**P99 spikes (~520–625ms)** — WAL fsync and memtable flush compete with foreground requests under sustained load. P50 and P90 remain stable — the spike affects fewer than 1% of requests.

**RANGE P50 is 4.58ms** — range queries perform an N-way heap merge across the memtable 
and all SSTables, with per-SSTable block scans feeding the heap. Latency scales with the 
number of SSTables and result set size. Expected behaviour for an LSM tree without 
seek-based SSTable iterators.

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
Both compaction and range queries require merging N sorted streams into one globally sorted output. A min-heap gives O(log N) per element across N iterators. The heap entry carries a source index — on key collision, the lower source index (newer SSTable) wins. Tombstones are dropped at the deepest compaction level where no older versions can exist below.

**Why size-tiered compaction?**
Size-tiered groups files of similar size rather than compacting by key range overlap (leveled) or recency (FIFO). This minimises write amplification — files are only rewritten when merged with peers of similar size, not on every level push. The tradeoff is higher read amplification than leveled compaction, mitigated by bloom filters and the LRU block cache. Cassandra uses the same strategy as its default for write-heavy workloads.