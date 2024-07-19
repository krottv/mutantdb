use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::mem::ManuallyDrop;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic;

use bytes::{Buf, Bytes, BytesMut};
use memmap2::{Advice, Mmap};
use prost::Message;

use proto::meta::{BlockIndex, TableIndex};

use crate::db_options::DbOptions;
use crate::entry::Entry;
use crate::errors::Error::CorruptedFileError;
use crate::errors::Result;
use crate::util::no_fail;

pub mod id_generator;

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

// todo: refactor sstable to use u64 id. Because it is possible to have 4 billions sstables, but not 2^63
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

pub struct Block {
    pub block_index: BlockIndex,
    pub entries: VecDeque<Entry>,
}

impl SSTable {
    pub const BLOCK_HEADER_SIZE: usize = 4;

    pub fn from_builder(index: TableIndex,
                        file_path: PathBuf,
                        opts: Arc<DbOptions>,
                        size_on_disk: u64,
                        id: usize) -> Result<SSTable> {
        unsafe {
            let file = File::open(&file_path)?;
            let mmap = ManuallyDrop::new(Mmap::map(&file)?);
            mmap.advise(Advice::Random)?;


            let sstable = SSTable {
                index,
                mmap,
                file_path,
                opts,
                first_key: Bytes::new(),
                last_key: Bytes::new(),
                size_on_disk,
                delete_on_drop: atomic::AtomicBool::new(false),
                id,
            };

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

        let mut temp_buf = BytesMut::with_capacity(12);
        temp_buf.resize(12, 0);

        reader.read_exact(&mut temp_buf)?;
        let expected_crc_index = temp_buf.get_u32();
        let index_size = temp_buf.get_u64();

        // Seek to the start of the index and read the index data
        let index_start = blocks_size + 12;
        reader.seek(SeekFrom::Start(index_start))?;

        temp_buf.clear();
        temp_buf.resize(index_size as usize, 0);
        reader.read_exact(&mut temp_buf)?;

        let actual_crc_index = crc32fast::hash(&temp_buf);
        if actual_crc_index != expected_crc_index {
            return Err(CorruptedFileError)
        }

        let index = TableIndex::decode(temp_buf.freeze())?;

        let size_on_disk = reader.stream_position()?;

        let mut sstable = Self::from_builder(index, file_path, opts, size_on_disk, id)?;

        sstable.init_first_last_keys()?;
        sstable.validate();

        Ok(sstable)
    }

    pub fn init_first_last_keys(&mut self) -> Result<()> {
        if self.index.blocks.is_empty() {
            panic!("trying to init empty sstable")
        }

        // unwrap because size is guaranteed to be non 0
        let mut first_block = self.get_block(self.index.blocks.first().unwrap())?;
        self.first_key = first_block.entries.pop_front().unwrap().key;

        let mut last_block = self.get_block(self.index.blocks.last().unwrap())?;
        self.last_key = last_block.entries.pop_back().unwrap().key;

        return Ok(());
    }

    pub fn validate(&self) {
        if self.opts.key_comparator.compare(&self.last_key, &self.first_key).is_lt() {
            panic!("invalid table key right < left. table_id {}, key_count {}", self.id, self.index.key_count)
        }
    }

    pub fn get_block(&self, index: &BlockIndex) -> Result<Block> {
        Entry::check_range_mmap(self.mmap.len(), index.offset as usize, Self::BLOCK_HEADER_SIZE)?;
        let mut buf_header: Bytes = Bytes::copy_from_slice(&self.mmap[index.offset as usize..index.offset as usize + Self::BLOCK_HEADER_SIZE]);
        let expected_crc = buf_header.get_u32();

        Entry::check_range_mmap(self.mmap.len(), index.offset as usize + Self::BLOCK_HEADER_SIZE,
                                index.len as usize - Self::BLOCK_HEADER_SIZE)?;
        let mut buf = Bytes::copy_from_slice(&self.mmap[index.offset as usize + Self::BLOCK_HEADER_SIZE..index.offset as usize + index.len as usize]);

        let actual_crc = crc32fast::hash(&buf);

        if expected_crc != actual_crc {
            return Err(CorruptedFileError);
        }

        let mut res: VecDeque<Entry> = VecDeque::new();

        while !buf.is_empty() {
            let entry = Entry::decode(&mut buf)?;
            res.push_back(entry);
        }

        Ok(
            Block {
                block_index: index.clone(),
                entries: res,
            }
        )
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
        no_fail(self.drop_no_fail(), "Drop sstable")
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::builder::Builder;
    use crate::db_options::DbOptions;
    use crate::entry;
    use crate::entry::{Entry, ValObj};
    use crate::errors::Error;
    use crate::errors::Error::CorruptedFileError;
    use crate::iterators::sstable_iterator::SSTableIterator;
    use crate::memtables::memtable::Memtable;
    use crate::sstable::SSTable;
    use crate::wal::tests::corrupt_byte;
    use crate::wal::Wal;

    pub(crate) fn create_sstable<'a>(opts: Arc<DbOptions>, entries: Vec<Entry>, id: usize) -> SSTable {
        let sstable_path = opts.sstables_path().join(SSTable::create_path(id));
        let wal_path = opts.wal_path().join(Wal::create_path(id));
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

        let block = sstable.get_block(&sstable.index.blocks[0]).unwrap();

        assert_eq!(block.entries.len(), 3);
        assert_eq!(&e1, &block.entries[0]);
        assert_eq!(&e2, &block.entries[1]);
        assert_eq!(&e3, &block.entries[2]);

        assert_eq!(&sstable.first_key, &e1.key.to_vec());
        assert_eq!(&sstable.last_key, &e3.key.to_vec());

        assert_eq!(sstable.index.blocks.len(), 1);
        assert_eq!(&sstable.index.blocks.get(0).unwrap().key, &e1.key.to_vec());

        let mut iter = SSTableIterator::new(Arc::new(sstable));

        assert_eq!(&e3, &iter.find_entry(&e3.key).unwrap());
        assert_eq!(&e2, &iter.find_entry(&e2.key).unwrap());
        assert_eq!(&e1, &iter.find_entry(&e1.key).unwrap());
        let absent_key = Bytes::from("key4");
        assert_eq!(None, iter.find_entry(&absent_key));

        iter.rewind();
        assert_eq!(Some(e1), iter.next());
        assert_eq!(Some(e2), iter.next());
        assert_eq!(Some(e3), iter.next());
        assert_eq!(None, iter.next());
    }

    fn test_many_blocks(recreate: bool) {
        let e1 = Entry::new(Bytes::from("1key"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("2key"), Bytes::from("value2"), entry::META_ADD);
        let e3 = Entry {
            key: Bytes::from("3key"),
            val_obj: ValObj {
                value: Bytes::from("value3"),
                meta: 10,
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

        let block0 = sstable.get_block(&sstable.index.blocks[0]).unwrap();
        assert_eq!(&sstable.index.blocks[0].key, &e1.key.to_vec());
        assert_eq!(block0.entries.len(), 1);
        assert_eq!(&e1, &block0.entries[0]);

        let block1 = sstable.get_block(&sstable.index.blocks[1]).unwrap();
        assert_eq!(&sstable.index.blocks[1].key, &e2.key.to_vec());
        assert_eq!(block1.entries.len(), 1);
        assert_eq!(&e2, &block1.entries[0]);

        let block2 = sstable.get_block(&sstable.index.blocks[2]).unwrap();
        assert_eq!(&sstable.index.blocks[2].key, &e3.key.to_vec());
        assert_eq!(block2.entries.len(), 1);
        assert_eq!(&e3, &block2.entries[0]);

        let mut iter = SSTableIterator::new(Arc::new(sstable));

        assert_eq!(&e3, &iter.find_entry(&e3.key).unwrap());
        assert_eq!(&e2, &iter.find_entry(&e2.key).unwrap());
        assert_eq!(&e1, &iter.find_entry(&e1.key).unwrap());
        let absent_key = Bytes::from("key4");
        assert_eq!(None, iter.find_entry(&absent_key));

        // iterator
        iter.rewind();
        assert_eq!(Some(e1), iter.next());
        assert_eq!(Some(e2), iter.next());
        assert_eq!(Some(e3), iter.next());
        assert_eq!(None, iter.next());
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
    fn corrupted_block() {
        let e1 = Entry::new(Bytes::from("1key"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("2key"), Bytes::from("value2"), entry::META_ADD);
        let e3 = Entry::new(Bytes::from("3key"), Bytes::from("value3"), entry::META_ADD);

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

        let sstable = Builder::build_from_memtable(memtable, sstable_path.clone(), opts.clone(), 1).unwrap();
        assert_eq!(sstable.index.blocks.len(), 3);
        drop(sstable);

        corrupt_byte(sstable_path.clone(), 5);

        let err = SSTable::open(sstable_path, opts, 1).err().unwrap();
        match err {
            CorruptedFileError => {}
            _ => {
                panic!("wrong error")
            }
        }
    }
}
