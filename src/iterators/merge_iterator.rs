/*
This iterator is used for compaction process.

It will take multiple sstables and produce always valid key next in sorted order. (like lazy merge sort).
In this way to compact, we would only need to create new SSTables from the iterator.

We assume that the first iterator has the earliest data.
*/

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::comparator::KeyComparator;

// iterator is not a generic to be able to combine different iterators
pub struct MergeIterator<Item> {
    iterators: Vec<Box<dyn Iterator<Item=Item>>>,
    heap: BinaryHeap<HeapElem<Item>>,
    comparator: Arc<dyn KeyComparator<Item>>,
}

// todo: ugly that we cannot pass lambda to BinaryHeap, now we got overhead of tons of reference counters
struct HeapElem<T> {
    entry: T,
    comparator: Arc<dyn KeyComparator<T>>,
    iterator_index: usize,
}

impl<T> PartialEq<Self> for HeapElem<T> {
    fn eq(&self, other: &Self) -> bool {
        return self.iterator_index == other.iterator_index &&
            self.comparator.compare(&self.entry, &other.entry).is_eq();
    }
}

impl<T> Eq for HeapElem<T> {}

impl<T> PartialOrd for HeapElem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        return Some(self.cmp(other));
    }
}

impl<'a, T> Ord for HeapElem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.comparator.compare(&self.entry, &other.entry)
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
impl<Item> MergeIterator<Item> {
    pub fn new(iterators: Vec<Box<dyn Iterator<Item=Item>>>, comparator: Arc<dyn KeyComparator<Item>>) -> Self {
        let mut heap = BinaryHeap::new();
        let mut iterators = iterators;

        for i in 0..iterators.len() {
            if let Some(entry) = iterators[i].next() {
                heap.push(HeapElem {
                    entry,
                    comparator: comparator.clone(),
                    iterator_index: i,
                });
            }
        }

        MergeIterator {
            iterators,
            heap,
            comparator,
        }
    }

    fn get_next(&mut self, index: usize) -> Option<Item> {
        if let Some(iter) = self.iterators.get_mut(index) {
            if let Some(entry) = iter.next() {
                return Some(entry);
            }
        }

        return None;
    }

    fn pop_heap(&mut self) -> Option<Item> {
        if let Some(heap_popped) = self.heap.pop() {

            // compensation
            if let Some(nxt) = self.get_next(heap_popped.iterator_index) {
                self.heap.push(HeapElem {
                    entry: nxt,
                    comparator: self.comparator.clone(),
                    iterator_index: heap_popped.iterator_index,
                });
            }

            return Some(heap_popped.entry);
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

impl<Item> Iterator for MergeIterator<Item> {
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(first_pop) = self.pop_heap() {
            loop {
                if let Some(peek) = self.heap.peek() {
                    if self.comparator.compare(&peek.entry, &first_pop).is_eq() {
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
    use std::sync::Arc;

    use bytes::Bytes;

    use crate::comparator::BytesStringUtf8Comparator;
    use crate::entry::{Entry, EntryComparator, META_ADD, ValObj};

    use super::*;

    fn new_entry(key: u8, value: u8) -> Entry {
        Entry {
            key: Bytes::from(key.to_string()),
            val_obj: ValObj {
                value: Bytes::from(value.to_string()),
                meta: META_ADD,
                user_meta: key,
                version: value as u64,
            },
        }
    }

    #[test]
    fn single_merge_iterator() {
        let comparator = Arc::new(BytesStringUtf8Comparator {});
        let entry_comparator = Arc::new(EntryComparator::new(comparator));

        let e1 = new_entry(1, 1);
        let e2 = new_entry(2, 2);
        let e3 = new_entry(3, 3);
        let e4 = new_entry(4, 4);

        let vec = vec![e1.clone(), e2.clone(), e3.clone(), e4.clone()];
        let iter = Box::new(vec.into_iter());

        let mut merge_iter = MergeIterator::new(vec![iter], entry_comparator);

        assert_eq!(merge_iter.next(), Some(e1));
        assert_eq!(merge_iter.next(), Some(e2));
        assert_eq!(merge_iter.next(), Some(e3));
        assert_eq!(merge_iter.next(), Some(e4));
        assert_eq!(merge_iter.next(), None);
    }

    #[test]
    fn all_iterators_empty_test() {
        let comparator = Arc::new(BytesStringUtf8Comparator {});
        let entry_comparator = Arc::new(EntryComparator::new(comparator));

        let iter1 = Box::new(Vec::new().into_iter());
        let iter2 = Box::new(Vec::new().into_iter());

        let mut merge_iter = MergeIterator::new(vec![iter1, iter2], entry_comparator);
        assert_eq!(merge_iter.next(), None);
    }

    #[test]
    fn basic_merge_test() {
        let comparator = Arc::new(BytesStringUtf8Comparator {});
        let entry_comparator = Arc::new(EntryComparator::new(comparator));

        let e1 = new_entry(1, 1);
        let e2 = new_entry(2, 2);
        let e3 = new_entry(3, 3);
        let e4 = new_entry(4, 4);

        let vec1 = vec![e1.clone(), e3.clone()];
        let vec2 = vec![e2.clone(), e4.clone()];

        let iter1 = Box::new(vec1.into_iter());
        let iter2 = Box::new(vec2.into_iter());

        let mut merge_iter = MergeIterator::new(vec![iter1, iter2], entry_comparator);

        assert_eq!(merge_iter.next(), Some(e1));
        assert_eq!(merge_iter.next(), Some(e2));
        assert_eq!(merge_iter.next(), Some(e3));
        assert_eq!(merge_iter.next(), Some(e4));
        assert_eq!(merge_iter.next(), None);
    }

    #[test]
    fn duplicate_keys_test() {
        let comparator = Arc::new(BytesStringUtf8Comparator {});
        let entry_comparator = Arc::new(EntryComparator::new(comparator));

        let e1 = Entry::new(Bytes::from("1key"), Bytes::from("value1"), META_ADD);
        let e2 = Entry::new(Bytes::from("2key"), Bytes::from("value2"), META_ADD);
        let e3 = Entry::new(Bytes::from("2key"), Bytes::from("value3"), META_ADD);
        let e4 = Entry::new(Bytes::from("3key"), Bytes::from("value4"), META_ADD);

        let vec1 = vec![e1.clone(), e3.clone()];
        let vec2 = vec![e2.clone(), e4.clone()];

        let iter1 = Box::new(vec1.into_iter());
        let iter2 = Box::new(vec2.into_iter());

        let mut merge_iter = MergeIterator::new(vec![iter1, iter2], entry_comparator);

        assert_eq!(merge_iter.next(), Some(e1));
        assert_eq!(merge_iter.next(), Some(e3));
        assert_eq!(merge_iter.next(), Some(e4));
        assert_eq!(merge_iter.next(), None);
    }

    #[test]
    fn duplicate_keys_inside_single_test() {
        let comparator = Arc::new(BytesStringUtf8Comparator {});
        let entry_comparator = Arc::new(EntryComparator::new(comparator));

        let e1 = Entry::new(Bytes::from("1key"), Bytes::from("value1"), META_ADD);
        let e2 = Entry::new(Bytes::from("2key"), Bytes::from("value2"), META_ADD);
        let e21 = Entry::new(Bytes::from("2key"), Bytes::from("value21"), META_ADD);
        let e22 = Entry::new(Bytes::from("2key"), Bytes::from("value22"), META_ADD);
        let e23 = Entry::new(Bytes::from("2key"), Bytes::from("value23"), META_ADD);
        let e24 = Entry::new(Bytes::from("2key"), Bytes::from("value24"), META_ADD);
        let e4 = Entry::new(Bytes::from("3key"), Bytes::from("value4"), META_ADD);

        let vec1 = vec![e1.clone(), e21.clone(), e22.clone(), e23.clone()];
        let vec2 = vec![e2.clone(), e24.clone(), e4.clone()];

        let iter1 = Box::new(vec1.into_iter());
        let iter2 = Box::new(vec2.into_iter());

        let mut merge_iter = MergeIterator::new(vec![iter1, iter2], entry_comparator);

        assert_eq!(merge_iter.next(), Some(e1));
        // e2 is removed because is it from the iterator that is next in order
        assert_eq!(merge_iter.next(), Some(e21));
        assert_eq!(merge_iter.next(), Some(e4));
        assert_eq!(merge_iter.next(), None);
    }

    #[test]
    fn empty_iterators_test() {
        let comparator = Arc::new(BytesStringUtf8Comparator {});
        let entry_comparator = Arc::new(EntryComparator::new(comparator));

        let e1 = Entry::new(Bytes::from("1key"), Bytes::from("value1"), META_ADD);
        let e21 = Entry::new(Bytes::from("2key"), Bytes::from("value2"), META_ADD);
        let e22 = Entry::new(Bytes::from("2key"), Bytes::from("value2"), META_ADD);
        let e23 = Entry::new(Bytes::from("2key"), Bytes::from("value2"), META_ADD);
        let e24 = Entry::new(Bytes::from("2key"), Bytes::from("value2"), META_ADD);
        
        let vec1 = vec![e1.clone(),
                             e21.clone(), e22.clone(), e23.clone(), e24.clone()];
        let vec2 = vec![];

        let mut merge_iter = MergeIterator::new(
            vec![Box::new(vec1.into_iter()), Box::new(vec2.into_iter())], entry_comparator);

        assert_eq!(merge_iter.next(), Some(e1));
        assert_eq!(merge_iter.next(), Some(e24));
        assert_eq!(merge_iter.next(), None);
    }
}