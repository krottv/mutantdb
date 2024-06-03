use std::sync::Arc;
use crate::entry::Entry;
use crate::sstable::{Block, SSTable};

pub struct SSTableIterator {
    sstable: Arc<SSTable>,
    block: Option<Block>,
    block_position: usize
}

impl SSTableIterator {
    pub fn new(sstable: Arc<SSTable>) -> Self {
        SSTableIterator {
            sstable,
            block: None,
            block_position: 0
        }
    }
}

impl Iterator for SSTableIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        // get from current block or nullify it if invalid
        if let Some(current_block) = &mut self.block {
            if current_block.entries.len() == 0 {
                self.block_position += 1;
                self.block = None;
            } else {
                return current_block.entries.pop_front();
            }
        }

        return if let Some(block_index) = self.sstable.index.blocks.get(self.block_position) {
            let mut block = self.sstable.get_block(block_index);
            let entry = block.entries.pop_front();
            self.block = Some(block);
            entry
        } else {
            None
        }
    }
}