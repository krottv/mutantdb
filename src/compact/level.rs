use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;

use crate::comparator::KeyComparator;
use crate::db_options::DbOptions;
use crate::entry::{Entry, Key, ValObj};
use crate::iterators::concat_iterator::ConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::sstable_iterator::SSTableIterator;
use crate::sstable::SSTable;

#[derive(Clone)]
pub struct Level {
    // sorted except first level.
    pub run: VecDeque<Arc<SSTable>>,
    // id of the level
    pub id: usize,
    pub size_on_disk: u64,
}

impl Level {
    pub fn add(&mut self, sstable: Arc<SSTable>) {
        self.size_on_disk += sstable.size_on_disk;
        self.run.push_front(sstable)
    }

    pub fn new_empty(id: usize) -> Level {
        Level {
            run: VecDeque::new(),
            id,
            size_on_disk: 0,
        }
    }

    pub fn calc_size_on_disk(&mut self) {
        let mut size_on_disk = 0u64;
        for sstable in &self.run {
            size_on_disk += sstable.size_on_disk;
        }

        self.size_on_disk = size_on_disk;
    }
    
    pub fn sort_tables(&mut self, key_comparator: &dyn KeyComparator<Bytes>) {
        if self.id == 0 {
            // todo: test sort order
            self.run.make_contiguous().sort_unstable_by(|x, y| {
                y.id.cmp(&x.id)
            });
        } else {
            self.run.make_contiguous().sort_unstable_by(|x, y| {
                // since keys are non-overlapping, order doesn't matter
                key_comparator.compare(&x.first_key, &y.first_key)
            });
        }
    }
    
    pub fn validate(&self, key_comparator: &dyn KeyComparator<Bytes>) {
        // check overlapping intervals
        if self.id != 0 && !self.run.is_empty() {
            let mut prev_right = &Bytes::new();
            
            for (index, table) in self.run.iter().enumerate() {
                if index != 0 && key_comparator.compare(&table.first_key, prev_right).is_le() {
                    panic!("overlapping sstable keys on level {}, table_id {}", self.id, table.id)
                } else if key_comparator.compare(&table.last_key, &table.first_key).is_lt() {
                    panic!("invalid table key right < left on level {}, table_id {}", self.id, table.id)
                }
                else {
                    prev_right = &table.last_key;
                }
            }
        }
    }

    pub fn new(id: usize, tables: &[Arc<SSTable>]) -> Level {
        let mut size_on_disk = 0u64;
        let mut run = VecDeque::with_capacity(tables.len());
        for table in tables {
            size_on_disk += table.size_on_disk;
            run.push_back(table.clone());
        }
        Level {
            run,
            id,
            size_on_disk,
        }
    }

    pub fn create_iterator_for_level(&self, entry_comparator: Arc<dyn KeyComparator<Entry>>) -> Box<dyn Iterator<Item=Entry>> {
        // cloning elements. not optimal. Unfortunately there's no way to create a &[] from immutable &self.
        let vec: Vec<Arc<SSTable>> = self.run.iter().cloned().collect();
        Self::create_iterator_for_tables(entry_comparator, self.id, &vec)
    }

    pub fn create_iterator_for_tables(entry_comparator: Arc<dyn KeyComparator<Entry>>,
                                      level_id: usize, tables: &[Arc<SSTable>]) -> Box<dyn Iterator<Item=Entry>> {
        if level_id == 0 {
            Box::new(Self::create_iterator_l0(entry_comparator, tables))
        } else {
            Box::new(Self::create_iterator_lx(tables))
        }
    }

    fn create_iterator_l0(entry_comparator: Arc<dyn KeyComparator<Entry>>, tables: &[Arc<SSTable>]) -> impl Iterator<Item=Entry> {
        let mut iterators: Vec<Box<dyn Iterator<Item=Entry>>> = Vec::new();

        for sstable in tables {
            let iter = SSTableIterator::new(sstable.clone());
            iterators.push(Box::new(iter));
        }

        return MergeIterator::new(iterators, entry_comparator);
    }

    fn create_iterator_lx(tables: &[Arc<SSTable>]) -> impl Iterator<Item=Entry> {
        let iterators: Vec<SSTableIterator> = tables.iter().map(|x| {
            SSTableIterator::new(x.clone())
        }).collect();

        return ConcatIterator::new(iterators);
    }


    fn get_val_l0(&self, key: &Key) -> Option<ValObj> {
        for sstable in self.run.iter() {
            if let Some(entry) = sstable.find_entry(key) {
                return Some(entry.val_obj);
            }
        }
        None
    }

    fn get_sstable_of_sorted(&self, key: &Key, key_comparator: &dyn KeyComparator<Key>) -> Option<&SSTable> {
        let bsearch_res = self.run.binary_search_by(|x| {
            let cmp_first = key_comparator.compare(&x.first_key, key);
            let cmp_last = key_comparator.compare(&x.last_key, key);

            if cmp_first.is_le() && cmp_last.is_ge() {
                return Ordering::Equal
            } else if cmp_first.is_lt() {
                return Ordering::Less;
            } else {
                return Ordering::Greater;
            }
        });

        if let Ok(index_sstable) = bsearch_res {
            Some(self.run.get(index_sstable).unwrap().as_ref())
        } else {
            None
        }
    }

    fn get_val_lx(&self, key: &Key, key_comparator: &dyn KeyComparator<Key>) -> Option<ValObj> {
        if let Some(sstable) = self.get_sstable_of_sorted(key, key_comparator) {
            if let Some(entry) = sstable.find_entry(key) {
                return Some(entry.val_obj);
            }
        }
        None
    }

    pub fn get_val(&self, key: &Key, key_comparator: &dyn KeyComparator<Key>) -> Option<ValObj> {
        if self.id == 0 {
            self.get_val_l0(key)
        } else {
            self.get_val_lx(key, key_comparator)
        }
    }

    pub fn select_oldest_sstable(&self) -> Option<Arc<SSTable>> {
        let res = self.run.iter().min_by_key(|x| {
            x.id
        });
        res.cloned()
    }

    pub fn select_sstables(&self, lowest_key: &Bytes, highest_key: &Bytes, db_options: Arc<DbOptions>) -> Vec<Arc<SSTable>> {
        if self.id == 0 {
            panic!("should be called only on non-overlapping tables")
        }
        // a binary search can be employed. But not a huge optimization, considering small sample size.

        self.run.iter().filter(|x| {
            db_options.key_comparator.compare(&x.last_key, lowest_key).is_ge() &&
                db_options.key_comparator.compare(&x.first_key, highest_key).is_le()
        }).cloned().collect()
    }
}

#[cfg(test)]
pub mod tests {
    use std::cmp::Ordering;
    use std::collections::VecDeque;
    use std::mem::ManuallyDrop;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use memmap2::MmapOptions;

    use proto::meta::TableIndex;

    use crate::compact::level::Level;
    use crate::comparator::BytesI32Comparator;
    use crate::core::tests::int_to_bytes;
    use crate::db_options::DbOptions;
    use crate::sstable::id_generator::SSTableIdGenerator;
    use crate::sstable::SSTable;

    fn create_sstable(first_key: i32, last_key: i32, id: usize) -> Arc<SSTable> {
        let opts = Arc::new(DbOptions {
            key_comparator: Arc::new(BytesI32Comparator {}),
            ..Default::default()
        });

        Arc::new(SSTable {
            index: TableIndex::default(),
            mmap: ManuallyDrop::new(MmapOptions::new().map_anon().unwrap().make_read_only().unwrap()),
            file_path: PathBuf::new(),
            opts,
            first_key: int_to_bytes(first_key),
            last_key: int_to_bytes(last_key),
            size_on_disk: 0,
            delete_on_drop: AtomicBool::new(false),
            id
        })
    }

    #[test]
    fn get_sstable_of_sorted() {
        let id_generator = SSTableIdGenerator::new(0);
        let key_comparator = BytesI32Comparator {};

        let level = Level::new(0,
                               &vec![
                                   create_sstable(1, 3, id_generator.get_new()),
                                   create_sstable(4, 8, id_generator.get_new()),
                                   create_sstable(9, 14, id_generator.get_new()),
                                   create_sstable(16, 18, id_generator.get_new()),
                                   create_sstable(25, 30, id_generator.get_new()),
                               ]);
        
        assert_eq!(level.run.get(0).unwrap().id, 1);
        assert_eq!(level.run.get(1).unwrap().id, 2);

        assert!(level.get_sstable_of_sorted(&int_to_bytes(0), &key_comparator).is_none());
        assert!(level.get_sstable_of_sorted(&int_to_bytes(15), &key_comparator).is_none());
        assert!(level.get_sstable_of_sorted(&int_to_bytes(20), &key_comparator).is_none());
        assert!(level.get_sstable_of_sorted(&int_to_bytes(31), &key_comparator).is_none());


        assert_eq!(level.get_sstable_of_sorted(&int_to_bytes(1), &key_comparator).unwrap().id, 1);
        assert_eq!(level.get_sstable_of_sorted(&int_to_bytes(2), &key_comparator).unwrap().id, 1);
        assert_eq!(level.get_sstable_of_sorted(&int_to_bytes(3), &key_comparator).unwrap().id, 1);

        assert_eq!(level.get_sstable_of_sorted(&int_to_bytes(8), &key_comparator).unwrap().id, 2);
        assert_eq!(level.get_sstable_of_sorted(&int_to_bytes(5), &key_comparator).unwrap().id, 2);
        assert_eq!(level.get_sstable_of_sorted(&int_to_bytes(7), &key_comparator).unwrap().id, 2);
        assert_eq!(level.get_sstable_of_sorted(&int_to_bytes(4), &key_comparator).unwrap().id, 2);

        assert_eq!(level.get_sstable_of_sorted(&int_to_bytes(25), &key_comparator).unwrap().id, 5);
        assert_eq!(level.get_sstable_of_sorted(&int_to_bytes(30), &key_comparator).unwrap().id, 5);
    }
    fn bsearch_range(ranges: &VecDeque<(i32, i32)>, target: i32) -> Result<usize, usize> {
        return ranges.binary_search_by(|x| {
            let (first, last) = x;
            
            
            let cmp_first = first.cmp(&target);
            let cmp_last = last.cmp(&target);
            
            if cmp_first.is_le() && cmp_last.is_ge() {
                return Ordering::Equal
            } else if cmp_first.is_lt() {
                return Ordering::Less;
            } else {
                return Ordering::Greater;
            }
        });
    }
    
    #[test]
    fn binary_search_range() {
        let mut ranges = VecDeque::new();
        ranges.push_back((1, 3));
        ranges.push_back((4, 8));
        ranges.push_back((9, 14));
        ranges.push_back((16, 18));
        ranges.push_back((25, 30));
        
        assert_eq!(bsearch_range(&ranges, 1), Ok(0));
        assert_eq!(bsearch_range(&ranges, 2), Ok(0));
        assert_eq!(bsearch_range(&ranges, 3), Ok(0));
        
        assert_eq!(bsearch_range(&ranges, 8), Ok(1));
        assert_eq!(bsearch_range(&ranges, 5), Ok(1));
        assert_eq!(bsearch_range(&ranges, 7), Ok(1));
        assert_eq!(bsearch_range(&ranges, 4), Ok(1));
        
        assert_eq!(bsearch_range(&ranges, 25), Ok(4));
        assert_eq!(bsearch_range(&ranges, 30), Ok(4));

        assert!(bsearch_range(&ranges, 0).is_err());
        assert!(bsearch_range(&ranges, 15).is_err());
        assert!(bsearch_range(&ranges, 20).is_err());
        assert!(bsearch_range(&ranges, 31).is_err());
    }
}