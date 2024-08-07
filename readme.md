MutantDb is a key-value embedded persistent database that uses inside LSM-tree optimized for write performance. Written completely in Rust.
This is not a finished project and it isn't ready for production, but it supports all basic use cases: read, write, iterate. For more info what is done and what is left, read `completed_features.md` and `to_be_done.md`

### How to use
``` rust
let db_opts = Arc::new(  
    DbOptions {  
        path: PathBuf::from("my_path_to_db"),  
        key_comparator: comparator,  
        ..Default::default()  
    }  
);  
  
// second parameter whether to start compaction otherwise manual  
let core = Mutant::open(db_opts, true).unwrap();  
  
core.add(  
    // key  
    Bytes::from(10.to_be_bytes().to_vec()),  
    // value  
    Bytes::from(10.to_be_bytes().to_vec())  
).unwrap();  
  
// remove by key  
core.remove(Bytes::from(10.to_be_bytes().to_vec())).unwrap();  
  
// iterate over all key-values  
for entry in core.into_iter() {  
    println!("value version is {}", entry.val_obj.version);  
    println!("key is {}", bytes_to_int(&entry.key));  
}  
  
// just drop it to close the database  
drop(core);
```

### Benchmarks
Run benchmarks using `cargo bench`
Todo: compare against RocksDB or BadgerDB.

### Sources
I took inspiration from RocksDB C++ storage by meta https://github.com/facebook/rocksdb/wiki
BadgerDB go-lang storage https://github.com/dgraph-io/badger
There's a great tutorial about LSM in general https://skyzh.github.io/mini-lsm/00-overview.html
For questions I consulted another key-value db in rust - https://github.com/tikv/agatedb

