use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;
use bytes::Bytes;
use crate::comparator::KeyComparator;
use crate::entry::{Entry, EntryComparator, Key, ValObj};
use crate::iterators::concat_iterator::ConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::sstable_iterator::SSTableIterator;
use crate::sstable::SSTable;

#[derive(Clone)]
pub struct Level {
    // sorted except first level. Size is at least 2 guaranteed
    pub run: VecDeque<Arc<SSTable>>,
    // id of the level
    pub id: u32,
    pub size_on_disk: u64,
}

impl Level {
    pub fn add(&mut self, sstable: Arc<SSTable>) {
        self.size_on_disk += sstable.size_on_disk;
        self.run.push_front(sstable)
    }

    pub fn new_empty(id: u32) -> Level {
        Level {
            run: VecDeque::new(),
            id,
            size_on_disk: 0,
        }
    }

    pub fn create_iterator_for_level(&self, entry_comparator: Arc<dyn KeyComparator<Entry>>) -> Box<dyn Iterator<Item=Entry>> {
        if self.id == 0 {
            Box::new(self.create_iterator_l0(entry_comparator))
        } else {
            Box::new(self.create_iterator_lx())
        }
    }

    fn create_iterator_l0(&self, entry_comparator: Arc<dyn KeyComparator<Entry>>) -> impl Iterator<Item=Entry> {
        let mut iterators: Vec<Box<dyn Iterator<Item=Entry>>> = Vec::new();

        for sstable in &self.run {
            let iter = SSTableIterator::new(sstable.clone());
            iterators.push(Box::new(iter));
        }

        return MergeIterator::new(iterators, entry_comparator);
    }

    fn create_iterator_lx(&self) -> impl Iterator<Item=Entry> {
        let iterators: Vec<SSTableIterator> = self.run.iter().map(|x| {
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
}