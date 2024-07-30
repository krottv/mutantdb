
## Memtable

- **Structure**: The memtable is an in-memory data structure that stores key-value items in a sorted order using a SkipList.
- **Implementation Details**: It employs unsafe pointer manipulation for efficient memory management.

## Save and Restore State

- **Write-Ahead Log (WAL)**: The WAL records all items written to the memtable in a sequential file format, enhancing write performance by avoiding the need for immediate sorting.
- **Manifest**: The manifest maintains the leveled structure of the LSM tree, tracking which levels contain which SSTables.
- **Data Integrity**: Both WAL and manifest incorporate checksums and range validations to protect against data corruption.

## SSTable

- **Disk Storage**: Once the memtable exceeds a certain size threshold, it is flushed to disk as an SSTable and placed into level 0 of the LSM tree.
- **Sparse Index**: An in-memory sparse index of the SSTable allows for quick lookups of necessary blocks, which are typically around 4KB in size.

## Leveled Compaction Strategy

- **Compaction**: The database uses a leveled compaction strategy, as detailed in the [RocksDB Leveled Compaction](https://github.com/facebook/rocksdb/wiki/Leveled-Compaction) documentation.
- **Parallel Sub-Compaction**: Implemented parallel sub-compaction allows non-conflicting levels (e.g., levels 1 -> 2 and 4 -> 5) to be compacted in parallel using asynchronous operations and the Tokio runtime in Rust.

## Iterators

- **Iteration Capability**: The database supports iterating over all items, enabling efficient traversal of the key-value pairs.

## Concurrency

- **Thread Safety**: The database supports concurrent reads and writes from multiple threads.
- **Read-Write Lock**: Internally, it uses a read-write lock, meaning that writes block reads and vice versa. This design is similar to other databases like Badger, which also use a single thread for writes.
- **Write Operations**: Concurrent writes to the WAL are not possible due to file operation constraints. However, a dedicated thread can be used to handle writes via a channel, reducing client latency. This can be easily implemented on top of the existing database architecture.

These completed features establish a solid foundation for the key-value store, ensuring basic functionality and data integrity. The remaining tasks focus on optimizing performance, enhancing concurrency, and adding advanced features to make the database production-ready.