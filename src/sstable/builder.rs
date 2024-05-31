use std::cmp::max;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use prost::Message;

use proto::meta::{BlockIndex, TableIndex};

use crate::entry::{Entry, ValObj};
use crate::errors::Result;
use crate::memtable::Memtable;
use crate::opts::DbOptions;
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
}

impl Builder {
    pub fn new(file_path: PathBuf,
               opts: Arc<DbOptions>,
               max_count: u64,
               max_block_size: usize,
               all_entries_size: u64,
               max_version: u64,
    ) -> Result<Self> {
        let mut index = TableIndex::default();
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)?;

        let mut writer = BufWriter::new(file);
        let buffer = BytesMut::with_capacity(max_block_size);

        writer.write(&all_entries_size.to_be_bytes())?;

        index.max_version = max_version;
        index.key_count = max_count;

        Ok(Builder {
            opts,
            counter: 0,
            writer,
            index,
            block: BlockIndex::default(),
            block_offset: 8,
            buffer,
            max_block_size,
            file_path,
        })
    }

    pub fn add_entry(&mut self, key: &Bytes, val_obj: &ValObj) -> Result<()> {
        if self.block.offset == 0 && self.index.blocks.is_empty() {
            self.block.key = key.to_vec();
            self.block.offset = self.block_offset;

            self.index.first_key = key.to_vec();
        } else if self.counter == (self.index.key_count - 1) {
            self.index.last_key = key.to_vec();
        }

        // write entry
        Entry::encode(key, val_obj, &mut self.buffer);
        let encoded_size = self.writer.write(&self.buffer)?;
        self.buffer.clear();

        // add block to index
        if self.block.len != 0 && (self.block.len as usize + encoded_size) > self.max_block_size {
            self.block_offset += self.block.len as u64;

            let old_val = mem::take(&mut self.block);
            self.index.blocks.push(old_val);

            self.block.offset = self.block_offset;
            self.block.key = key.to_vec();
        }
        self.block.len += encoded_size as u32;

        self.counter += 1;

        return Ok(());
    }

    pub fn build(mut self) -> Result<SSTable> {
        if self.block.len != 0 {
            self.index.blocks.push(self.block);
        }
        let index_size = self.index.encoded_len();
        self.writer.write(&index_size.to_be_bytes())?;

        self.buffer.reserve(index_size);
        self.index.encode(&mut self.buffer)?;
        self.writer.write(&self.buffer)?;
        self.writer.flush()?;

        return SSTable::from_builder(self.index, self.file_path, self.opts.clone());
    }

    pub fn build_from_memtable(mem: &Memtable,
                               file_path: PathBuf,
                               opts: Arc<DbOptions>, ) -> Result<SSTable> {
        let max_block_size = max(opts.block_max_size, mem.inner.compute_max_entry_size() as u32);
        let all_entries_size: u64 = mem.inner.cur_size;
        let max_count = mem.inner.skiplist.size as u64;

        let mut builder = Builder::new(file_path, opts, max_count, max_block_size as usize,
                                       all_entries_size, mem.inner.max_version)?;

        for entry in mem.inner.skiplist.into_iter() {
            builder.add_entry(&entry.key, &entry.value)?
        }

        builder.build()
    }
}