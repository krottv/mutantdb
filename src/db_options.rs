use std::path::PathBuf;
use std::sync::Arc;
use bytes::Bytes;
use crate::comparator::{BytesStringUtf8Comparator, KeyComparator};
use crate::compact::{CompactionOptions, SimpleLeveledOpts};

pub struct DbOptions {
    // in bytes. actual size can exceed this if single entry is bigger
    pub max_wal_size: u64,
    // in bytes. actual size can exceed this if single entry is bigger
    pub max_memtable_size: u64,
    // since we have one table it is fine
    pub key_comparator: Arc<dyn KeyComparator<Bytes>>,
    
    pub wal_path: PathBuf,
    
    pub sstables_path: PathBuf,
    
    pub block_max_size: u32,
    
    pub compaction: Arc<CompactionOptions>
}

impl Default for DbOptions {
    fn default() -> Self {
        return DbOptions {
            // 10 mb
            max_wal_size: 100000,
            // 5 mb
            max_memtable_size: 50000,
            
            key_comparator: Arc::new(BytesStringUtf8Comparator {}),
            
            wal_path: PathBuf::from( "/"),
            
            sstables_path: PathBuf::from( "/"),
            
            // 4kb power of 2
            block_max_size: 4096,
            
            compaction: Arc::new(CompactionOptions::SimpleLeveled(SimpleLeveledOpts::default()))
        }
    }
}