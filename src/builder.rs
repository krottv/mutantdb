use std::cmp::max;
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use prost::Message;

use proto::meta::{BlockIndex, TableIndex};

use crate::entry::{Entry, ValObj};
use crate::errors::Result;
use crate::memtables::memtable::Memtable;
use crate::db_options::DbOptions;
use crate::sstable::SSTable;

pub struct Builder {
    opts: Arc<DbOptions>,
    counter: u64,
    writer: BufWriter<File>,
    index: TableIndex,
    block: BlockIndex,
    block_offset: u64,
    buffer: BytesMut,
    max_block_size: usize,
    file_path: PathBuf,
    max_version: u64,
    sstable_id: usize,
    first_key: Bytes,
    last_key: Bytes,
}

impl Builder {
    pub fn new(file_path: PathBuf,
               opts: Arc<DbOptions>,
               max_block_size: usize,
               sstable_id: usize,
    ) -> Result<Self> {
        let index = TableIndex::default();
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)?;

        let writer = BufWriter::new(file);
        let buffer = BytesMut::with_capacity(max_block_size);

        Ok(Builder {
            opts,
            counter: 0,
            writer,
            index,
            block: BlockIndex::default(),
            block_offset: 0,
            buffer,
            max_block_size,
            file_path,
            max_version: 0,
            sstable_id,
            first_key: Bytes::new(),
            last_key: Bytes::new(),
        })
    }

    pub fn add_entry(&mut self, key: &Bytes, val_obj: &ValObj) -> Result<()> {
        if self.counter == 0 {
            self.block.key = key.clone();
            self.block.offset = self.block_offset;

            self.first_key = key.clone();
        } else {
            let ord = self.opts.key_comparator.compare(&key, &self.last_key);
            if ord.is_eq() {
                panic!("shouldn't add duplicated key")
            } else if ord.is_lt() {
                panic!("keys are in wrong order. should be ascending")
            }
        }
        self.last_key = key.clone();

        // write entry
        let ensure_size = Entry::get_encoded_size(key, val_obj);
        if ensure_size > self.buffer.capacity() {
            self.buffer.reserve(ensure_size - self.buffer.capacity())
        }
        Entry::encode(key, val_obj, &mut self.buffer);
        let encoded_size = self.writer.write(&self.buffer)?;
        self.buffer.clear();

        // add block to index
        if self.block.len != 0 && (self.block.len as usize + encoded_size) > self.max_block_size {
            self.block_offset += self.block.len as u64;

            let old_val = mem::take(&mut self.block);
            self.index.blocks.push(old_val);

            self.block.offset = self.block_offset;
            self.block.key = key.clone();
        }
        self.block.len += encoded_size as u32;

        self.counter += 1;
        self.max_version = max(self.max_version, val_obj.version);
        return Ok(());
    }

    pub fn build(mut self) -> Result<SSTable> {
        if self.block.len != 0 {
            self.index.blocks.push(self.block);
        }

        self.index.key_count = self.counter;
        self.index.max_version = self.max_version;
        let index_size = self.index.encoded_len();

        let all_entries_size = self.writer.stream_position()?;
        self.writer.write(&index_size.to_be_bytes())?;

        self.buffer.reserve(index_size);
        self.index.encode(&mut self.buffer)?;
        self.writer.write(&self.buffer)?;

        // size of entries is at the end
        self.writer.write(&all_entries_size.to_be_bytes())?;
        self.writer.flush()?;

        let size_on_disk = self.writer.stream_position()?;

        let mut sstable = SSTable::from_builder(self.index, self.file_path, self.opts.clone(),
                                     size_on_disk, self.sstable_id)?;
        sstable.first_key = self.first_key;
        sstable.last_key = self.last_key;
        
        sstable.validate();
        
        Ok(sstable)
    }

    pub fn build_from_memtable(mem: Arc<Memtable>,
                               file_path: PathBuf,
                               opts: Arc<DbOptions>,
                               sstable_id: usize) -> Result<SSTable> {
        let mem_inner = mem.inner.read().unwrap();
        let max_block_size = max(opts.block_max_size, mem_inner.compute_max_entry_size() as u32);

        let mut builder = Builder::new(file_path, opts, max_block_size as usize, sstable_id)?;

        for entry in mem_inner.skiplist.into_iter() {
            builder.add_entry(&entry.key, &entry.value)?
        }

        builder.build()
    }

    pub fn is_empty(&self) -> bool {
        self.counter == 0
    }
}