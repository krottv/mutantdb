Those are features that need to be done to consider it production ready minimalistic embedded persistent db

## Iterators

- **SeekTo Method**: Implement a `SeekTo` method that allows iterating only over necessary items.
- **Separate Key and Value Return**: Modify iterators to return keys and values separately to avoid decoding the value when it is not necessary.
- **Return References**: Ensure iterators return references to bytes (`&[u8]`) instead of owned values to prevent unnecessary copying if the user does not need it.
- **Read-Lock on Memtable Skiplist**: Current iterators require a read-lock on the memtable skiplist, which blocks all writes. To solve this, consider copying memtables and references to necessary SSTables, thus creating an iterator over a snapshot of the database state when it was created.
- **Remove Used Iterators**: Implement functionality to remove used iterators from `MergeIterator` to allow memory to be released sooner.

## Picking the Most Recent Version of Key-Value

- **Current Search Method**: The current method searches from level 0 to level x and returns the first found value. This is incorrect because level x-1 does not guarantee the most recent version compared to level x.
- **Guarantee or Search All Levels**: Either provide a guarantee that each level has the most recent version or search all levels. This involves a trade-off, as the guarantee might make compaction less performant but allows breaking the search as soon as an item is found, optimizing the happy case.
- **Potential MVCC Feature**: This improvement is postponed due to the potential Multi-Version Concurrency Control (MVCC) feature, which requires reading entries on all levels and returning data relevant to the current transaction or the most recent version.

## Garbage Collection for SSTables

- **Current Deletion Process**: SSTables are currently marked for deletion after compaction and are deleted when `Drop` is called.
- **Power Outage Scenario**: If a user creates an iterator (reference to SSTable) and then compaction marks those SSTables for deletion, a power outage could leave these tables as garbage, as `Drop` was not called. Implement a mechanism to clean unused SSTables in such cases.

## BloomFilter

- **Read Performance Optimization**: Implementing a Bloom filter would optimize read performance since Bloom filters can indicate if an item is absent in a collection, eliminating the need for a logarithmic search for an absent item.

## Storing Big Values Separately

- **Separate Storage for Big Values**: Store large values separately to prevent copying them during the compaction process, significantly improving performance. For more details, refer to _[WiscKey: Separating Keys from Values in SSD-conscious Storage](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)_.

## Compression

- **Block Compression**: Implement compression for blocks in SSTables using algorithms like Snappy. https://github.com/google/snappy
- **Key Compression**: Use prefix delta encoding to compress keys.

## More Parallelization

- **Parallel Operations**: Enhance parallelization for opening `MergeIterators`, recovering memtables, and recovering the manifest.