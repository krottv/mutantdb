
/*
    Extremely hard to code without Atomic Skiplist. Tons of references 
    which I do not know how to handle in order to return Owned iterator...
    SkipIterator(list: AsRef<SkipList>) might be useful. So we can convert
    ReadGuard<MemtableInner> to AsRef<SkipList>.
 */

use std::sync::Arc;
use std::sync::RwLockReadGuard;
use bytes::Bytes;
use ouroboros::self_referencing;
use crate::entry::{Entry, EntryComparator, ValObj};
use crate::iterators::merge_iterator::MergeIterator;

use crate::memtables::{Memtables, MemtablesViewIterator};
use crate::memtables::memtable::Memtable;
use crate::memtables::memtable::MemtableInner;
use crate::skiplist::skipiterator::SkipIterator;

#[self_referencing]
pub struct MemtableIterator {
    mem: Arc<Memtable>,
    #[borrows(mem)]
    #[covariant]
    lock: RwLockReadGuard<'this, MemtableInner>,
    #[borrows(lock)]
    #[covariant]
    pub iter: SkipIterator<'this, Bytes, ValObj>,
}

impl MemtableIterator {

    pub fn new_from_mem(mem: Arc<Memtable>) -> Self {

        let builder = MemtableIteratorBuilder {
            mem,
            lock_builder: |x| {
                x.inner.read().unwrap()
            },
            iter_builder: |x| {
                x.skiplist.into_iter()
            }
        };
        builder.build()
    }
}

impl Iterator for MemtableIterator {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_iter_mut(|x| {
            x.next()
        }).map(|x| {
            Entry {
                key: x.key.clone(),
                val_obj: x.value.clone()
            }
        })
    }
}

impl Memtables {
    pub fn new_memtables_iterator(memtables: &Memtables) -> impl Iterator<Item=Entry> {
        let iterators: Vec<Box<dyn Iterator<Item=Entry>>> = MemtablesViewIterator::new(memtables)
            .map(|x| {
                Box::new(MemtableIterator::new_from_mem(x.clone())) as Box<dyn Iterator<Item=Entry>>
            }).collect();

        let entry_comparator = Arc::new(EntryComparator::new(memtables.opts.key_comparator.clone()));
        MergeIterator::new(iterators, entry_comparator)
    }
}

