use std::path::PathBuf;
use std::sync::Arc;
use bytes::Bytes;
use crate::comparator::{BytesStringUtf8Comparator, KeyComparator};
use crate::levels::CompactionStrategy;

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
    
    pub compaction_strategy: CompactionStrategy
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
            
            compaction_strategy: CompactionStrategy::SimpleLeveled
        }
    }
}

// todo: validate creation. For example levels can't be < 2
pub struct LevelsOptions {
    pub level_max_size: u64,
    
    // second_level_max_size = next_level_size_multiple * level_max_size
    pub next_level_size_multiple: u32,
    
    pub num_levels: u32
}

impl Default for LevelsOptions {
    fn default() -> Self {
        LevelsOptions {
            // 20 mb
            level_max_size: 200000,
            
            next_level_size_multiple: 10,
            
            num_levels: 7
        }
    }
}