use std::cmp::max;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use bytes::BytesMut;
use prost::Message;

use proto::meta::{BlockIndex, TableIndex};

use crate::entry::Entry;
use crate::errors::Result;
use crate::memtable::Memtable;
use crate::opts::DbOptions;
use crate::sstable::SSTable;

pub struct Builder {
    
}

impl Builder {
    
    // see SSTable for the format
    // todo: refactor to a class?
    pub fn build<'a>(mem: Memtable, file_path: PathBuf, opts: &'a DbOptions) -> Result<SSTable<'a>> {
        let mut memtable = mem;
        // max block size is max of entry and opts.block_size
        let mut max_block_size = opts.block_max_size as usize;
        let mut all_entries_size: u64 = 0;
        for entry in &memtable.inner.skiplist {
            
            let encoded_size = Entry::get_encoded_size(&entry.key, &entry.value);
            max_block_size = max(encoded_size, max_block_size);
            all_entries_size += encoded_size as u64;
        }
        
        let mut index = TableIndex::default();
        index.key_count = memtable.inner.skiplist.size as u64;
        index.max_version = memtable.inner.max_version;
        
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path.clone())?;
        
        let mut writer = BufWriter::new(file);
        let mut buffer = BytesMut::with_capacity(max_block_size);
        
        writer.write(&all_entries_size.to_be_bytes())?;
        
        let mut block = BlockIndex::default();
        let mut block_offset: u64 = 8;
        
        // drain is not actually necessary
        for (i, entry) in memtable.inner.skiplist.drain().enumerate() {
            
            if i == 0 {
                block.key = entry.key.to_vec();
                block.offset = block_offset;

                index.first_key = entry.key.to_vec();
            } else if i == (index.key_count as usize - 1) {
                index.last_key = entry.key.to_vec();
            }

            // write entry
            Entry::encode(&entry.key, &entry.value, &mut buffer);
            let encoded_size = writer.write(&buffer)?;
            buffer.clear();
            
            // add block to index
            if block.len != 0 && (block.len as usize + encoded_size) > max_block_size {
                
                block_offset += block.len as u64;
                index.blocks.push(block);
                block = BlockIndex::default();
                block.offset = block_offset;
                block.key = entry.key.to_vec();
            }
            block.len += encoded_size as u32;
        }
        
        if block.len != 0 {
            index.blocks.push(block);
        }
        let index_size = index.encoded_len();
        writer.write(&index_size.to_be_bytes())?;
        
        buffer.reserve(index_size);
        index.encode(&mut buffer)?;
        writer.write(&buffer)?;
        writer.flush()?;
        
        // todo: delete wal when saved to manifest
        memtable.wal.delete()?;
        
        return SSTable::from_builder(index, file_path, opts)
    }
}