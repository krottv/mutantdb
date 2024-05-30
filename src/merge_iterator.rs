/*
This iterator is used for compaction process.

It will take multiple sstables and produce always valid key next in sorted order. (like lazy merge sort).
In this way to compact, we would only need to create new SSTables from the iterator.

We assume that the first iterator has the earliest data
*/

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;

use bytes::Bytes;

use crate::comparator::KeyComparator;
use crate::entry::Entry;
use crate::sstable::sstable_iterator::SSTableIterator;

pub struct MergeIterator<'a> {
    iterators: Vec<SSTableIterator<'a>>,
    heap: BinaryHeap<HeapElem<'a>>,
    comparator: &'a dyn KeyComparator<Bytes>
}

#[derive(Clone)]
struct HeapElem<'a> {
    entry: Entry,
    comparator: &'a dyn KeyComparator<Bytes>,
    iterator_index: usize,
}

impl<'a> fmt::Debug for HeapElem<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeapElem")
            .field("entry", &self.entry)
            .field("iterator_index", &self.iterator_index)
            .finish()
    }
}

impl<'a> PartialEq<Self> for HeapElem<'a> {
    fn eq(&self, other: &Self) -> bool {
        return self.iterator_index == other.iterator_index &&
            self.comparator.compare(&self.entry.key, &other.entry.key).is_eq();
    }
}

impl<'a> Eq for HeapElem<'a> {}

impl<'a> PartialOrd for HeapElem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        return Some(self.cmp(other))
    }
}

impl<'a> Ord for HeapElem<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.comparator.compare(&self.entry.key, &other.entry.key)
            .then_with(|| {
                self.iterator_index.cmp(&other.iterator_index)
            }).reverse()
    }
}

/*
1 -> 2 -> 5 -> 7
2 -> 3 -> 4 -> 7

res = 1 -> 2 (first) -> 3 -> 4 -> 5 -> 7 (first)

maintain a heap (item, iterator_index)

Setup:
    - Add all first items
    - Ensure that heap always has items from all iterators (unless some iterator is drained)

Next:
    - pop item from heap to return later
    - keep popping items if heap.head has the same key
    - when pop item, increment position of iterator at index
    - add item or skip if it is == heap.head
*/
impl<'a> MergeIterator<'a> {
    pub fn new(iterators: Vec<SSTableIterator<'a>>, comparator: &'a dyn KeyComparator<Bytes>) -> Self {
        let mut heap = BinaryHeap::new();
        let mut iterators = iterators;
        
        for i in 0..iterators.len() {
            
            if let Some(entry) = iterators[i].next() {
                heap.push(HeapElem {
                    entry,
                    comparator, 
                    iterator_index: i
                });
            }
        }

        MergeIterator {
            iterators,
            heap,
            comparator
        }
    }

    fn get_next(&mut self, index: usize) -> Option<Entry> {
        if let Some(iter) = self.iterators.get_mut(index) {
            if let Some(entry) = iter.next() {
                return Some(entry);
            }
        }
        
        return None
    }
    
    fn pop_heap(&mut self) -> Option<Entry> {
        if let Some(heap_popped) = self.heap.pop() {

            // compensation
            if let Some(nxt) = self.get_next(heap_popped.iterator_index) {
                self.heap.push(HeapElem {
                    entry: nxt,
                    comparator: self.comparator,
                    iterator_index: heap_popped.iterator_index
                });
            }

            return Some(heap_popped.entry)
        } else {
            None
        }
    }
}

/**
SSTables don't have duplicates.

1 -> 3 -> 4 -> 6
          ^
0 -> 3 -> 4 -> 5
          ^
3 -> 4 -> 5 -> 6
     ^

heap = [3, 3, 3]
res = [0, 1]

- when popping, need to check that item in heap is not like previous
*/

impl<'a> Iterator for MergeIterator<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(first_pop) = self.pop_heap() {
            
            loop {
                if let Some(peek) = self.heap.peek() {
                    if self.comparator.compare(&peek.entry.key, &first_pop.key).is_eq() {
                        let _unused = self.pop_heap();
                    } else {
                        break;
                    }
                    
                } else {
                    break;
                }          
            }
            
            Some(first_pop)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tempfile::TempDir;

    use crate::comparator::{BytesI32Comparator, BytesStringUtf8Comparator};
    use crate::entry::{Entry, META_ADD, ValObj};
    use crate::memtable::Memtable;
    use crate::opts::DbOptions;
    use crate::sstable::builder::Builder;
    use crate::sstable::SSTable;
    use crate::sstable::sstable_iterator::SSTableIterator;

    use super::*;

    fn create_sstable<'a>(tmp_dir: &TempDir, opts: &'a DbOptions, entries: Vec<Entry>, id: usize) -> SSTable<'a> {
        let sstable_path = tmp_dir.path().join(format!("{id:}.mem"));
        let wal_path = tmp_dir.path().join(format!("{id:}.wal"));
        let mut memtable = Memtable::new(1, wal_path, &opts).unwrap();
        for entry in entries {
            memtable.add(entry).unwrap();
        }
        Builder::build_from_memtable(memtable, sstable_path, &opts).unwrap()
    }
    
    fn new_entry(key: u8, value: u8) -> Entry {
        Entry {
            key: Bytes::from(key.to_string()),
            val_obj: ValObj {
                value: Bytes::from(value.to_string()),
                meta: META_ADD,
                user_meta: key,
                version: value as u64
            }
        }
    }
    
    #[test]
    fn ord_heap() {
        let mut heap = BinaryHeap::new();
        let comparator = BytesStringUtf8Comparator {};

        let e4 = HeapElem {
            entry: new_entry(4, 4),
            comparator: &comparator,
            iterator_index: 0
        };
        let e2 = HeapElem {
            entry: new_entry(2, 2),
            comparator: &comparator,
            iterator_index: 2
        };
        let e3 = HeapElem {
            entry: new_entry(3, 3),
            comparator: &comparator,
            iterator_index: 0
        };
        let e1 = HeapElem {
            entry: new_entry(1, 1),
            comparator: &comparator,
            iterator_index: 5
        };
        
        heap.push(e4.clone());
        heap.push(e2.clone());
        heap.push(e3.clone());
        heap.push(e1.clone());
        
        assert_eq!(Some(e1), heap.pop());
        assert_eq!(Some(e2), heap.pop());
        assert_eq!(Some(e3), heap.pop());
        assert_eq!(Some(e4), heap.pop());
        assert_eq!(None, heap.pop());
    }
    
    #[test]
    fn single_merge_iterator() {
        let tmp_dir = TempDir::new().unwrap();
        let opts = DbOptions {
            block_max_size: 1,
            ..Default::default()
        };
        let comparator = BytesStringUtf8Comparator {};

        let e1 = new_entry(1, 1);
        let e3 = new_entry(3, 3);
        let e2 = new_entry(2, 2);
        let e4 = new_entry(4, 4);

        let sstable = create_sstable(&tmp_dir, &opts, vec![e4.clone(), e3.clone(), e2.clone(), e1.clone()], 1);
        let mut merge_iter = MergeIterator::new(vec![SSTableIterator::new(&sstable)], &comparator);

        assert_eq!(merge_iter.next(), Some(e1));
        assert_eq!(merge_iter.next(), Some(e2));
        assert_eq!(merge_iter.next(), Some(e3));
        assert_eq!(merge_iter.next(), Some(e4));
        assert_eq!(merge_iter.next(), None);
    }

    #[test]
    fn all_iterators_empty_test() {
        let tmp_dir = TempDir::new().unwrap();
        let opts = DbOptions {
            block_max_size: 1000,
            ..Default::default()
        };
        let sstable1 = create_sstable(&tmp_dir, &opts,vec![], 1);
        let sstable2 = create_sstable(&tmp_dir, &opts,vec![], 2);

        let iter1 = SSTableIterator::new(&sstable1);
        let iter2 = SSTableIterator::new(&sstable2);

        let comparator = Box::new(BytesI32Comparator {});
        let mut merge_iter = MergeIterator::new(vec![iter1, iter2], comparator.as_ref());

        assert_eq!(merge_iter.next(), None);
    }

    #[test]
    fn basic_merge_test() {
        let tmp_dir = TempDir::new().unwrap();
        let opts = DbOptions {
            block_max_size: 1000,
            ..Default::default()
        };
        let comparator = BytesStringUtf8Comparator {};
        
        let e1 = new_entry(1, 1);
        let e2 = new_entry(2, 2);
        let e3 = new_entry(3, 3);
        let e4 = new_entry(4, 4);

        let sstable1 = create_sstable(&tmp_dir, &opts, vec![e1.clone(), e3.clone()], 1);
        let sstable2 = create_sstable(&tmp_dir, &opts, vec![e2.clone(), e4.clone()], 2);

        let mut iter1 = SSTableIterator::new(&sstable1);
        let mut iter2 = SSTableIterator::new(&sstable2);
        
        assert_eq!(iter1.next(), Some(e1.clone()));
        assert_eq!(iter1.next(), Some(e3.clone()));
        assert_eq!(iter1.next(), None);
        
        assert_eq!(iter2.next(), Some(e2.clone()));
        assert_eq!(iter2.next(), Some(e4.clone()));
        assert_eq!(iter2.next(), None);

        iter1 = SSTableIterator::new(&sstable1);
        iter2 = SSTableIterator::new(&sstable2);

        let mut merge_iter = MergeIterator::new(vec![iter1, iter2], &comparator);

        assert_eq!(merge_iter.next(), Some(e1));
        assert_eq!(merge_iter.next(), Some(e2));
        assert_eq!(merge_iter.next(), Some(e3));
        assert_eq!(merge_iter.next(), Some(e4));
        assert_eq!(merge_iter.next(), None);
    }

    #[test]
    fn duplicate_keys_test() {
        let tmp_dir = TempDir::new().unwrap();
        let opts = DbOptions {
            block_max_size: 1000,
            ..Default::default()
        };
        let comparator = BytesStringUtf8Comparator {};
        
        let e1 = Entry::new(Bytes::from("1key"), Bytes::from("value1"), META_ADD);
        let e2 = Entry::new(Bytes::from("2key"), Bytes::from("value2"), META_ADD);
        let e3 = Entry::new(Bytes::from("2key"), Bytes::from("value3"), META_ADD);
        let e4 = Entry::new(Bytes::from("3key"), Bytes::from("value4"), META_ADD);

        let sstable1 = create_sstable(&tmp_dir, &opts, vec![e1.clone(), e3.clone()], 1);
        let sstable2 = create_sstable(&tmp_dir, &opts, vec![e2.clone(), e4.clone()], 2);

        let iter1 = SSTableIterator::new(&sstable1);
        let iter2 = SSTableIterator::new(&sstable2);

        let mut merge_iter = MergeIterator::new(vec![iter1, iter2], &comparator);

        assert_eq!(merge_iter.next(), Some(e1));
        // e2 is removed because is it from the iterator that is next in order
        assert_eq!(merge_iter.next(), Some(e3)); 
        assert_eq!(merge_iter.next(), Some(e4));
        assert_eq!(merge_iter.next(), None);
    }

    #[test]
    fn empty_iterators_test() {
        let tmp_dir = TempDir::new().unwrap();
        let opts = DbOptions {
            block_max_size: 1000,
            ..Default::default()
        };
        let comparator = BytesStringUtf8Comparator {};
        
        let e1 = Entry::new(Bytes::from("1key"), Bytes::from("value1"), META_ADD);
        let e2 = Entry::new(Bytes::from("2key"), Bytes::from("value2"), META_ADD);

        let sstable1 = create_sstable(&tmp_dir, &opts,vec![e1.clone(), e2.clone()], 1);
        let sstable2 = create_sstable(&tmp_dir, &opts,vec![], 2);

        let iter1 = SSTableIterator::new(&sstable1);
        let iter2 = SSTableIterator::new(&sstable2);

        let mut merge_iter = MergeIterator::new(vec![iter1, iter2], &comparator);

        assert_eq!(merge_iter.next(), Some(e1));
        assert_eq!(merge_iter.next(), Some(e2));
        assert_eq!(merge_iter.next(), None);
    }
}