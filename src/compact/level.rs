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
    // sorted except first level. Size is at least 2 guaranteed
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

    pub fn set_new_run(&mut self, run: VecDeque<Arc<SSTable>>) {
        self.run = run;
        self.recalc_run();
    }

    pub fn recalc_run(&mut self) {
        let mut size_on_disk = 0u64;
        for sstable in &self.run {
            size_on_disk += sstable.size_on_disk;
        }

        self.size_on_disk = size_on_disk;
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

    fn get_val_lx(&self, key: &Key, key_comparator: Arc<dyn KeyComparator<Key>>) -> Option<ValObj> {
        let bsearch_res = self.run.binary_search_by(|x| {
            let cmp_first = key_comparator.compare(key, &x.first_key);
            if cmp_first.is_lt() {
                return Ordering::Less;
            }

            let cmp_last = key_comparator.compare(key, &x.last_key);
            if cmp_last.is_gt() {
                return Ordering::Greater;
            }

            return Ordering::Equal;
        });

        if let Ok(index_sstable) = bsearch_res {
            if let Some(entry) = self.run[index_sstable].find_entry(key) {
                return Some(entry.val_obj);
            }
        }

        None
    }

    pub fn get_val(&self, key: &Key, key_comparator: Arc<dyn KeyComparator<Key>>) -> Option<ValObj> {
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
            db_options.key_comparator.compare(&x.first_key, lowest_key).is_ge() &&
                db_options.key_comparator.compare(&x.last_key, highest_key).is_ge()
        }).cloned().collect()
    }
}