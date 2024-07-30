use std::sync::Arc;
use bytes::Bytes;
use crate::entry::Entry;
use crate::iterators::block_iterator::BlockIterator;
use crate::sstable::{Block, SSTable};

pub struct SSTableIterator {
    sstable: Arc<SSTable>,
    block: Option<Block>,
    block_iterator: BlockIterator,
    entry_position: usize,
    is_valid: bool
}

impl SSTableIterator {
    pub fn new(sstable: Arc<SSTable>) -> Self {
        SSTableIterator {
            sstable: sstable.clone(),
            block: None,
            block_iterator: BlockIterator::new(sstable),
            entry_position: 0,
            is_valid: true
        }
    }
    
    pub fn seek_to(&mut self, key: &Bytes) -> bool {
        if self.block_iterator.seek_to(key) {
            self.block = self.block_iterator.next();
            
            // if seek_to is true, then next is guaranteed.
            if let Ok(found_index) = self.block.as_ref().unwrap().entries.binary_search_by(|e| {
                self.sstable.opts.key_comparator.compare(&e.key, key)
            }) {
                self.entry_position = found_index;
                return true;
            }
        } 
        return false;
    }
    
    pub fn rewind(&mut self) {
        self.entry_position = 0;
        self.block = None;
        self.block_iterator.rewind();
    }

    pub fn find_entry(&mut self, key: &Bytes) -> Option<Entry> {
        if self.seek_to(key) {
            self.next()
        } else {
            None
        }
    }
}

impl Iterator for SSTableIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        // get from current block or nullify it if invalid
        if let Some(current_block) = &mut self.block {
            if let Some(entry) = current_block.entries.get(self.entry_position) {
                // todo: return references, don't clone.
                self.entry_position += 1;
                return Some(entry.clone());
            } else {
                self.block = None;
            }
        }
        
        return if let Some(block) = self.block_iterator.next() {
            self.entry_position = 1;
            let entry = block.entries.get(0).cloned();
            if entry.is_none() {
                panic!("empty block is invalid behaviour")
            }
            self.block = Some(block);
            entry
        } else {
            if !self.block_iterator.is_valid() {
                self.is_valid = false;
            }
            None
        }
    }
}