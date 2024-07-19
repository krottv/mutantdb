use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;

use crate::compact::{CompactionOptions, LeveledOpts};
use crate::comparator::{BytesStringUtf8Comparator, KeyComparator};
use crate::errors::Result;

pub struct DbOptions {
    // in bytes. actual size can exceed this if single entry is bigger
    // usize because mmap supports only usize, whereas file can be u64
    pub max_wal_size: usize,
    // in bytes. actual size can exceed this if single entry is bigger
    pub max_memtable_size: u64,
    // since we have one table it is fine
    pub key_comparator: Arc<dyn KeyComparator<Bytes>>,
    // path for key files, like wal, sstables, manifest.
    // one dir is used to all of them in order to perform fsync
    pub path: PathBuf,
    
    pub block_max_size: u32,
    
    pub compaction: Arc<CompactionOptions>,

    pub manifest_deletion_threshold: usize
}

impl Default for DbOptions {
    fn default() -> Self {
        return DbOptions {
            // 10 mb
            max_wal_size: 100000,
            // 5 mb
            max_memtable_size: 50000,
            
            key_comparator: Arc::new(BytesStringUtf8Comparator {}),
            
            path: PathBuf::from( "/"),
            
            // 4kb power of 2
            block_max_size: 4096,
            
            compaction: Arc::new(CompactionOptions::Leveled(LeveledOpts::default())),
            
            // 1 instance of ManifestChange approximately 15 bytes.
            manifest_deletion_threshold: 5000
        }
    }
}

impl DbOptions {
    // separated even thou often the same dir is used
    pub fn wal_path(&self) -> PathBuf {
        self.path.join("wals")
    }
    pub fn sstables_path(&self) -> PathBuf {
        self.path.join("sstables")
    }
    pub fn manifest_path(&self) -> PathBuf {
        self.path.join("manifest")
    }
    pub fn manifest_rewrite_path(&self) -> PathBuf {
        self.path.join("manifest_rewrite")
    }
    
    pub fn create_dirs(&self) -> Result<()> {
        fs::create_dir_all(self.wal_path())?;
        fs::create_dir_all(self.sstables_path())?;
        Ok(())
    }
}