use std::cmp::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use crate::sstable::{Block, SSTable};

pub struct BlockIterator {
    sstable: Arc<SSTable>,
    block_position: usize,
    is_valid: bool,
}

impl BlockIterator {
    
    pub fn new(sstable: Arc<SSTable>) -> BlockIterator {
        return BlockIterator {
            sstable,
            block_position: 0,
            is_valid: true
        }
    }
    
    pub fn rewind(&mut self) {
        self.block_position = 0;
    }
    
    pub fn is_valid(&self) -> bool {
        return self.is_valid;
    }
    
    // binary search. 
    // [1, 4, 8, 16, 32, 64], t = 9
    // find last less then. 
    // [T, T, T, F, F, F]
    //        ^
    fn bsearch_block_index(&self, key: &Bytes) -> Option<usize> {
        let sstable = &self.sstable;

        if sstable.opts.key_comparator.compare(key, &sstable.first_key).is_lt() {
            return None;
        } else if sstable.opts.key_comparator.compare(key, &sstable.last_key).is_gt() {
            return None;
        } else if sstable.index.blocks.len() == 0 {
            return None;
        }

        let mut left = 0;
        let mut right = sstable.index.blocks.len() - 1;
        let mut res: usize = 0;
        let mut initialized = false;

        while left <= right {
            let mid = (left as i64 + (right as i64 - left as i64) / 2) as usize;
            let ord = sstable.opts.key_comparator.compare(&sstable.index.blocks[mid].key, key);

            match ord {
                Ordering::Equal => {
                    return Some(mid);
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

        return Some(res);
    }

    pub fn seek_to(&mut self, key: &Bytes) -> bool {
        return match self.bsearch_block_index(key) {
            None => {
                false
            }
            Some(index) => {
                self.block_position = index;
                true
            }
        };
    }
}

impl Iterator for BlockIterator {
    type Item = Block;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_valid {
            return None;
        }

        return if let Some(index) = self.sstable.index.blocks.get(self.block_position) {
            self.block_position += 1;
            let block_res = self.sstable.get_block(index);
            if let Ok(block) = block_res {
                Some(block)
            } else {
                log::log!(log::Level::Warn, "can't read block with offset {}, len {}, err {}", index.offset, index.len, block_res.err().unwrap());
                self.is_valid = false;
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::mem::ManuallyDrop;
    use std::sync::{Arc, atomic};
    use bytes::Bytes;
    use memmap2::MmapOptions;
    use tempfile::tempdir;
    use proto::meta::{BlockIndex, TableIndex};
    use crate::comparator::BytesI32Comparator;
    use crate::db_options::DbOptions;
    use crate::iterators::block_iterator::BlockIterator;
    use crate::sstable::SSTable;

    #[test]
    fn search_block_index() {
        let mut block_indexes: Vec<BlockIndex> = Vec::new();

        // [0,4,8,12,16,20,24,28]
        for (i, key) in (0..30).step_by(4).enumerate() {
            block_indexes.push(BlockIndex {
                key: Bytes::from((key as i32).to_be_bytes().to_vec()),
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
            opts,
            first_key: Bytes::from(0i32.to_be_bytes().to_vec()),
            last_key: Bytes::from(28i32.to_be_bytes().to_vec()),
            size_on_disk: 0,
            delete_on_drop: atomic::AtomicBool::new(false),
            id: 1,
        };

        let block_iter = BlockIterator::new(Arc::new(sstable));

        let mut key = Bytes::from(16i32.to_be_bytes().to_vec());
        assert_eq!(block_iter.bsearch_block_index(&key), Some(4));

        key = Bytes::from(18i32.to_be_bytes().to_vec());
        assert_eq!(block_iter.bsearch_block_index(&key), Some(4));

        key = Bytes::from(5i32.to_be_bytes().to_vec());
        assert_eq!(block_iter.bsearch_block_index(&key), Some(1));

        key = Bytes::from(4i32.to_be_bytes().to_vec());
        assert_eq!(block_iter.bsearch_block_index(&key), Some(1));

        key = Bytes::from(3i32.to_be_bytes().to_vec());
        assert_eq!(block_iter.bsearch_block_index(&key), Some(0));

        key = Bytes::from(29i32.to_be_bytes().to_vec());
        assert_eq!(block_iter.bsearch_block_index(&key), None);
    }
}