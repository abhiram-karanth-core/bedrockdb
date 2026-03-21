Benchmarked the wal.

Group commit delivers 120 MB/s write throughput at 439 ns/op. Fsync-every-write drops to 0.10 MB/s — a 1,181x difference on the same hardware. The durability guarantee is identical — both strategies fsync to disk. The only difference is how many writes share each fsync.