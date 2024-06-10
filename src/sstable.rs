pub mod id_generator;

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::mem::ManuallyDrop;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic;

use bytes::Bytes;
use memmap2::{Advice, Mmap};
use prost::Message;

use proto::meta::{BlockIndex, TableIndex};

use crate::entry::Entry;
use crate::errors::Result;
use crate::db_options::DbOptions;
use crate::util::no_fail;

/**
Format on disk

list blocks (arbitrary size)
----
index_size: 8 bytes
---
Table index (arbitrary size)
----
blocks_size: 8 bytes

 */

//todo: checksum
pub struct SSTable {
    pub index: TableIndex,
    // cache for blocks. Simpler then hashmap based
    // manually drop because we need to drop it before deleting file to prevent errors
    pub mmap: ManuallyDrop<Mmap>,
    pub file_path: PathBuf,
    pub opts: Arc<DbOptions>,
    pub first_key: Bytes,
    pub last_key: Bytes,
    pub size_on_disk: u64,
    pub delete_on_drop: atomic::AtomicBool,
    pub id: usize,
}
/*
You still need an in-memory index to tell you the offsets for some of the keys, 
but it can be sparse: one key for every few kilobytes of segment file is sufficient, 
because a few kilobytes can be scanned very quickly.
 */

//todo: compression
pub struct Block {
    pub block_index: BlockIndex,
    pub entries: VecDeque<Entry>,
}

impl SSTable {
    //todo: if creating from memtable, then first_key and last_key can be obtained from SkipList in O(1) time
    // without IO
    pub fn from_builder(index: TableIndex,
                        file_path: PathBuf,
                        opts: Arc<DbOptions>,
                        size_on_disk: u64,
                        id: usize) -> Result<SSTable> {
        unsafe {
            let file = File::open(&file_path)?;
            let mmap = ManuallyDrop::new(Mmap::map(&file)?);
            mmap.advise(Advice::Random)?;
            
            
            let mut sstable = SSTable {
                index,
                mmap,
                file_path,
                opts,
                first_key: Bytes::new(),
                last_key: Bytes::new(),
                size_on_disk,
                delete_on_drop: atomic::AtomicBool::new(false),
                id
            };
            sstable.init_first_last_keys();
            return Ok(sstable);
        }
    }

    /*
    Notes on BufReader:
    - read does not provide any guarantees about the number of bytes read. 
    It may read any number of bytes up to the buffer size, including 0 bytes.
    
    - read_exact guarantees that it will either read the exact
     number of bytes requested (the buffer size) or return an error.
     */
    pub fn open(file_path: PathBuf, opts: Arc<DbOptions>, id: usize) -> Result<SSTable> {
        let file = File::open(&file_path)?;
        let file_size = file.metadata()?.len();
        let mut reader = BufReader::new(file);

        // Read the last 8 bytes from footer
        reader.seek(SeekFrom::Start(file_size - 8))?;
        let mut blocks_size_bytes = [0u8; 8];
        reader.read_exact(&mut blocks_size_bytes)?;
        let blocks_size = u64::from_be_bytes(blocks_size_bytes);

        // Seek to the position of the index size and read the next 8 bytes
        reader.seek(SeekFrom::Start(blocks_size))?;
        let mut index_size_bytes = [0u8; 8];
        reader.read_exact(&mut index_size_bytes)?;
        let index_size = u64::from_be_bytes(index_size_bytes);

        // Seek to the start of the index and read the index data
        let index_start = blocks_size + 8;
        reader.seek(SeekFrom::Start(index_start))?;

        let mut index_data = vec![0u8; index_size as usize];
        reader.read_exact(&mut index_data)?;

        let buf = Bytes::from(index_data);
        let index = TableIndex::decode(buf)?;

        let size_on_disk = reader.stream_position()?;

        return Self::from_builder(index, file_path, opts, size_on_disk, id);
    }

    pub fn init_first_last_keys(&mut self) {
        if self.index.blocks.is_empty() {
            return;
        }

        // unwrap because size is guaranteed to be non 0
        let mut first_block = self.get_block(self.index.blocks.get(0).unwrap());
        self.first_key = first_block.entries.pop_front().unwrap().key;

        let mut last_block = self.get_block(self.index.blocks.get(self.index.blocks.len() - 1).unwrap());
        self.last_key = last_block.entries.pop_back().unwrap().key;
    }

    // binary search. 
    // [1, 4, 8, 16, 32, 64], t = 9
    // find last less then. 
    // [T, T, T, F, F, F]
    //        ^
    pub fn bsearch_block_index(&self, key: &Bytes) -> Option<&BlockIndex> {
        if self.opts.key_comparator.compare(key, &self.first_key).is_lt() {
            return None;
        } else if self.opts.key_comparator.compare(key, &self.last_key).is_gt() {
            return None;
        } else if self.index.blocks.len() == 0 {
            return None;
        }

        let mut left = 0;
        let mut right = self.index.blocks.len() - 1;
        let mut res: usize = 0;
        let mut initialized = false;

        while left <= right {
            let mid = (left as i64 + (right as i64 - left as i64) / 2) as usize;
            //todo: clone here is unnecessary copying of bytes. But how to avoid?
            // make comparator operate on vec? But fucking generics are weird.
            // possible with asRef if was a concrete type
            let mid_bytes = Bytes::from(self.index.blocks[mid].key.clone());
            let ord = self.opts.key_comparator.compare(&mid_bytes, key);

            match ord {
                Ordering::Equal => {
                    return self.index.blocks.get(mid);
                }
                Ordering::Less => {
                    res = mid;
                    left = mid + 1;
                    initialized = true
                }
                Ordering::Greater => {
                    // attempt to subtract with overflow
                    if mid == 0 {
                        break;
                    } else {
                        right = mid - 1;
                    }
                }
            }
        }
        if !initialized {
            return None;
        }

        return self.index.blocks.get(res);
    }

    pub fn get_block(&self, index: &BlockIndex) -> Block {
        let slice = &self.mmap[index.offset as usize..index.offset as usize + index.len as usize];
        let mut buf = Bytes::copy_from_slice(slice);

        let mut res: VecDeque<Entry> = VecDeque::new();

        while !buf.is_empty() {
            let entry = Entry::decode(&mut buf);
            res.push_back(entry);
        }

        Block {
            block_index: index.clone(),
            entries: res,
        }
    }

    pub fn bsearch_block(&self, key: &Bytes) -> Option<Block> {
        return match self.bsearch_block_index(key) {
            None => {
                None
            }
            Some(index) => {
                Some(self.get_block(index))
            }
        };
    }

    pub fn find_entry(&self, key: &Bytes) -> Option<Entry> {
        if let Some(mut block) = self.bsearch_block(key) {
            if let Ok(found_index) = block.entries.binary_search_by(|e| {
                self.opts.key_comparator.compare(&e.key, key)
            }) {
                // safe to make swap remove because we read and discard the whole block.
                let removed = block.entries.swap_remove_back(found_index);
                return removed;
            }
        }

        return None;
    }

    pub fn create_path(id: usize) -> PathBuf {
        let s = format!("{id:}.mem");
        PathBuf::from(s)
    }

    pub fn mark_delete(&self) {
        self.delete_on_drop.store(true, atomic::Ordering::Relaxed);
    }

    fn drop_no_fail(&mut self) -> Result<()> {
        unsafe {
            ManuallyDrop::drop(&mut self.mmap);
        }

        if self.delete_on_drop.load(atomic::Ordering::Relaxed) {
            fs::remove_file(&self.file_path)?;
        }

        Ok(())
    }
}

impl Drop for SSTable {
    fn drop(&mut self) {
        no_fail(self.drop_no_fail(), format!("Drop sstable").as_str())
    }
}

/*
Test cases:
1. Add entries save and do bsearch to find (absent and present cases).
2. Check that necessary amount of blocks created
3. Check that reopen works.
4. wal is deleted after
5. check search of block
 */
#[cfg(test)]
pub(crate) mod tests {
    use std::mem::ManuallyDrop;
    use std::sync::{Arc, atomic};

    use bytes::Bytes;
    use memmap2::MmapOptions;
    use tempfile::{tempdir, TempDir};

    use proto::meta::{BlockIndex, TableIndex};

    use crate::builder::Builder;
    use crate::comparator::BytesI32Comparator;
    use crate::entry;
    use crate::entry::{Entry, ValObj};
    use crate::iterators::sstable_iterator::SSTableIterator;
    use crate::memtables::memtable::Memtable;
    use crate::db_options::DbOptions;
    use crate::sstable::SSTable;

    pub(crate) fn create_sstable<'a>(tmp_dir: &TempDir, opts: Arc<DbOptions>, entries: Vec<Entry>, id: usize) -> SSTable {
        let sstable_path = tmp_dir.path().join(format!("{id:}.mem"));
        let wal_path = tmp_dir.path().join(format!("{id:}.wal"));
        let memtable = Arc::new(Memtable::new(1, wal_path, opts.clone()).unwrap());
        for entry in entries {
            memtable.add(entry).unwrap();
        }
        Builder::build_from_memtable(memtable, sstable_path, opts, id).unwrap()
    }

    #[test]
    fn basic_create() {
        test_basic_create(false)
    }

    #[test]
    fn basic_reopen() {
        test_basic_create(true)
    }

    fn test_basic_create(recreate: bool) {
        let e1 = Entry::new(Bytes::from("1key"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("2key"), Bytes::from("value2"), entry::META_ADD);
        let e3 = Entry {
            key: Bytes::from("3key"),
            val_obj: ValObj {
                value: Bytes::from("value3"),
                meta: 10,
                user_meta: 15,
                version: 1000,
            },
        };
        let tmp_dir = tempdir().unwrap();
        let wal_path = tmp_dir.path().join("1.wal");
        let sstable_path = tmp_dir.path().join("1.mem");
        let opts = Arc::new(DbOptions {
            max_wal_size: 1000,
            block_max_size: 1000,
            ..Default::default()
        });

        let memtable = Arc::new(Memtable::new(1, wal_path, opts.clone()).unwrap());
        memtable.add(e1.clone()).unwrap();
        memtable.add(e2.clone()).unwrap();
        memtable.add(e3.clone()).unwrap();

        let mut sstable = Builder::build_from_memtable(memtable, sstable_path.clone(), opts.clone(), 1).unwrap();
        if recreate {
            drop(sstable);
            sstable = SSTable::open(sstable_path.clone(), opts.clone(), 1).unwrap()
        }

        let block = sstable.get_block(&sstable.index.blocks[0]);

        assert_eq!(block.entries.len(), 3);
        assert_eq!(&e1, &block.entries[0]);
        assert_eq!(&e2, &block.entries[1]);
        assert_eq!(&e3, &block.entries[2]);

        assert_eq!(&sstable.first_key, &e1.key.to_vec());
        assert_eq!(&sstable.last_key, &e3.key.to_vec());

        assert_eq!(sstable.index.blocks.len(), 1);
        assert_eq!(&sstable.index.blocks.get(0).unwrap().key, &e1.key.to_vec());
        assert_eq!(&e3, &sstable.find_entry(&e3.key).unwrap());
        assert_eq!(&e2, &sstable.find_entry(&e2.key).unwrap());
        assert_eq!(&e1, &sstable.find_entry(&e1.key).unwrap());
        let absent_key = Bytes::from("key4");
        assert_eq!(None, sstable.find_entry(&absent_key));

        // iterator
        let mut iterator = SSTableIterator::new(Arc::new(sstable));

        assert_eq!(Some(e1), iterator.next());
        assert_eq!(Some(e2), iterator.next());
        assert_eq!(Some(e3), iterator.next());
        assert_eq!(None, iterator.next());
    }

    fn test_many_blocks(recreate: bool) {
        let e1 = Entry::new(Bytes::from("1key"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("2key"), Bytes::from("value2"), entry::META_ADD);
        let e3 = Entry {
            key: Bytes::from("3key"),
            val_obj: ValObj {
                value: Bytes::from("value3"),
                meta: 10,
                user_meta: 15,
                version: 1000,
            },
        };
        let tmp_dir = tempdir().unwrap();
        let wal_path = tmp_dir.path().join("1.wal");
        let sstable_path = tmp_dir.path().join("1.mem");
        let opts = Arc::new(DbOptions {
            max_wal_size: 1000,
            block_max_size: 1,
            ..Default::default()
        });

        let memtable = Arc::new(Memtable::new(1, wal_path, opts.clone()).unwrap());
        memtable.add(e1.clone()).unwrap();
        memtable.add(e2.clone()).unwrap();
        memtable.add(e3.clone()).unwrap();

        let mut sstable = Builder::build_from_memtable(memtable, sstable_path.clone(), opts.clone(), 1).unwrap();
        if recreate {
            sstable = SSTable::open(sstable_path, opts, 1).unwrap()
        }

        assert_eq!(sstable.index.blocks.len(), 3);
        assert_eq!(&sstable.first_key, &e1.key.to_vec());
        assert_eq!(&sstable.last_key, &e3.key.to_vec());

        let block0 = sstable.get_block(&sstable.index.blocks[0]);
        assert_eq!(&sstable.index.blocks[0].key, &e1.key.to_vec());
        assert_eq!(block0.entries.len(), 1);
        assert_eq!(&e1, &block0.entries[0]);

        let block1 = sstable.get_block(&sstable.index.blocks[1]);
        assert_eq!(&sstable.index.blocks[1].key, &e2.key.to_vec());
        assert_eq!(block1.entries.len(), 1);
        assert_eq!(&e2, &block1.entries[0]);

        let block2 = sstable.get_block(&sstable.index.blocks[2]);
        assert_eq!(&sstable.index.blocks[2].key, &e3.key.to_vec());
        assert_eq!(block2.entries.len(), 1);
        assert_eq!(&e3, &block2.entries[0]);

        assert_eq!(&e3, &sstable.find_entry(&e3.key).unwrap());
        assert_eq!(&e2, &sstable.find_entry(&e2.key).unwrap());
        assert_eq!(&e1, &sstable.find_entry(&e1.key).unwrap());
        let absent_key = Bytes::from("key4");
        assert_eq!(None, sstable.find_entry(&absent_key));

        // iterator
        let mut iterator = SSTableIterator::new(Arc::new(sstable));

        assert_eq!(Some(e1), iterator.next());
        assert_eq!(Some(e2), iterator.next());
        assert_eq!(Some(e3), iterator.next());
        assert_eq!(None, iterator.next());
    }

    #[test]
    fn many_blocks() {
        test_many_blocks(false)
    }


    #[test]
    fn many_blocks_reopen() {
        test_many_blocks(true)
    }

    #[test]
    fn search_block_index() {
        let mut block_indexes: Vec<BlockIndex> = Vec::new();

        // [0,4,8,12,16,20,24,28]
        for (i, key) in (0..30).step_by(4).enumerate() {
            block_indexes.push(BlockIndex {
                key: (key as i32).to_be_bytes().to_vec(),
                offset: i as u64,
                len: i as u32,
            });
        }

        let opts = Arc::new(DbOptions {
            key_comparator: Arc::new(BytesI32Comparator {}),
            ..Default::default()
        });

        let table_index = TableIndex {
            blocks: block_indexes.clone(),
            max_version: 0,
            key_count: 0,
        };
        let tmp_dir = tempdir().unwrap();
        let sstable_path = tmp_dir.path().join("1.mem");


        let sstable = SSTable {
            index: table_index,
            mmap: ManuallyDrop::new(MmapOptions::new().map_anon().unwrap().make_read_only().unwrap()),
            file_path: sstable_path,
            opts: opts,
            first_key: Bytes::from(0i32.to_be_bytes().to_vec()),
            last_key: Bytes::from(28i32.to_be_bytes().to_vec()),
            size_on_disk: 0,
            delete_on_drop: atomic::AtomicBool::new(false),
            id: 1
        };

        let mut key = Bytes::from(16i32.to_be_bytes().to_vec());
        assert_eq!(sstable.bsearch_block_index(&key), Some(&block_indexes[4]));

        key = Bytes::from(18i32.to_be_bytes().to_vec());
        assert_eq!(sstable.bsearch_block_index(&key), Some(&block_indexes[4]));

        key = Bytes::from(5i32.to_be_bytes().to_vec());
        assert_eq!(sstable.bsearch_block_index(&key), Some(&block_indexes[1]));

        key = Bytes::from(4i32.to_be_bytes().to_vec());
        assert_eq!(sstable.bsearch_block_index(&key), Some(&block_indexes[1]));

        key = Bytes::from(3i32.to_be_bytes().to_vec());
        assert_eq!(sstable.bsearch_block_index(&key), Some(&block_indexes[0]));

        key = Bytes::from(29i32.to_be_bytes().to_vec());
        assert_eq!(sstable.bsearch_block_index(&key), None);
    }
}
