Benchmarked the wal.

Group commit delivers 120 MB/s write throughput at 439 ns/op. Fsync-every-write drops to 0.10 MB/s — a 1,181x difference on the same hardware. The durability guarantee is identical — both strategies fsync to disk. The only difference is how many writes share each fsync.

Memtable Benchmarks

BenchmarkPut                   83 ns/op    64 B/op    2 allocs/op
BenchmarkGet                   37 ns/op    32 B/op    1 allocs/op
BenchmarkConcurrentReadWrite  319 ns/op

Get is 2x faster than Put.
Put takes write lock + possible size subtraction on overwrite.
Get takes read lock only — multiple concurrent reads don't block each other.