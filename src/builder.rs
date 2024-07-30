use std::cmp::max;
use std::fs::File;
use std::io::{BufWriter, Seek, Write};
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
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
    table_index: TableIndex,
    block_index: BlockIndex,
    crc_buf: BytesMut,
    block_offset: u64,
    block_buf: BytesMut,
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
            table_index: index,
            block_index: BlockIndex::default(),
            crc_buf: BytesMut::with_capacity(SSTable::BLOCK_HEADER_SIZE as usize),
            block_offset: 0,
            block_buf: buffer,
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
            self.block_index.key = key.clone();
            self.block_index.offset = self.block_offset;
            self.block_index.len += SSTable::BLOCK_HEADER_SIZE as u32;

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
        let entry_size = Entry::get_encoded_size(key, val_obj);
        
        // add block to index
        if self.block_index.len > SSTable::BLOCK_HEADER_SIZE as u32 && (self.block_index.len as usize + entry_size) > self.max_block_size {
            self.block_offset += self.block_index.len as u64;

            let old_val = mem::take(&mut self.block_index);
            self.table_index.blocks.push(old_val);
            
            self.block_index.len += SSTable::BLOCK_HEADER_SIZE as u32;
            self.block_index.offset = self.block_offset;
            self.block_index.key = key.clone();
            
            self.write_buf()?;
        }

        // write entry
        self.block_buf.reserve(entry_size);
        Entry::encode(key, val_obj, &mut self.block_buf);
        self.block_index.len += entry_size as u32;

        self.counter += 1;
        self.max_version = max(self.max_version, val_obj.version);
        return Ok(());
    }
    
    fn write_buf(&mut self) -> Result<()> {
        let crc = crc32fast::hash(&self.block_buf);
        self.crc_buf.put_u32(crc);

        self.writer.write(&self.crc_buf)?;
        self.writer.write(&self.block_buf)?;

        self.crc_buf.clear();
        self.block_buf.clear();
        
        Ok(())
    }

    pub fn build(mut self) -> Result<SSTable> {
        if self.block_index.len > SSTable::BLOCK_HEADER_SIZE as u32 {
            self.table_index.blocks.push(self.block_index);

            // todo: remove code duplicate with write_buf(&mut self)
            let crc = crc32fast::hash(&self.block_buf);
            self.crc_buf.put_u32(crc);

            self.writer.write(&self.crc_buf)?;
            self.writer.write(&self.block_buf)?;

            self.crc_buf.clear();
            self.block_buf.clear();
        }

        self.table_index.key_count = self.counter;
        self.table_index.max_version = self.max_version;
        let index_size = self.table_index.encoded_len();
        
        self.block_buf.reserve(index_size);
        self.table_index.encode(&mut self.block_buf)?;
        let crc = crc32fast::hash(&self.block_buf);
        self.crc_buf.put_u32(crc);

        let all_blocks_size = self.writer.stream_position()?;
        self.writer.write(&self.crc_buf)?;
        self.writer.write(&(index_size as u64).to_be_bytes())?;
        self.writer.write(&self.block_buf)?;
        self.writer.write(&all_blocks_size.to_be_bytes())?;
        self.writer.flush()?;

        let size_on_disk = self.writer.stream_position()?;

        let mut sstable = SSTable::from_builder(self.table_index, self.file_path, self.opts.clone(),
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