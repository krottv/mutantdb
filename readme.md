MutantDb is a key-value embedded persistent database that uses inside LSM-tree optimized for write performance. Written completely in Rust.
This is not a finished project and isn't ready for production, but it supports all basic use cases: read, write, iterate. For more info what is done and what is left, read features.md and todo.md

### Benchmarks
Run benchmarks using `cargo bench`
Todo: compare against RocksDb or BargerDb.

### Sources
I took inspiration from RocksDB https://github.com/facebook/rocksdb/wiki
There's a great tutorial about LSM in general https://skyzh.github.io/mini-lsm/00-overview.html
For questions I consulted another key-value db in rust - https://github.com/tikv/agatedb
Also a great db project that deserves a view - https://github.com/dgraph-io/badger

