use std::cmp::max;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use bytes::Bytes;
use crate::entry::{Entry, ValObj};
use crate::db_options::DbOptions;
use crate::skiplist::{AddResult, SkiplistRaw};
use crate::wal::{Wal, WalIterator};

// separate wal and skiplist so that they can be mutated independently
pub struct MemtableInner {
    pub skiplist: SkiplistRaw<Bytes, ValObj>,
    pub max_version: u64,
    pub max_memtable_size: u64,
    pub cur_size: AtomicU64,
}

impl MemtableInner {
    pub fn new(opts: Arc<DbOptions>) -> MemtableInner {
        return MemtableInner {
            skiplist: SkiplistRaw::new(opts.key_comparator.clone(), false),
            max_version: 0,
            max_memtable_size: opts.max_memtable_size,
            cur_size: AtomicU64::new(0)
        }
    }

    pub fn add_to_skip(&mut self, entry: Entry) {
        let add_size = entry.get_encoded_size_entry() as u64;
        let val_version = entry.val_obj.version;
        let add_res = self.skiplist.add(entry.key, entry.val_obj);
        match add_res {
            AddResult::Added => {}
            AddResult::Replaced(old_val) => {
                let encoded_size = old_val.key.len()
                    + Entry::get_header_size()
                    + old_val.value.get_encoded_size();
                self.cur_size.fetch_sub(encoded_size as u64, Ordering::Relaxed);
            }
        }
        self.cur_size.fetch_add(add_size, Ordering::Relaxed);

        self.max_version = max(self.max_version, val_version)
    }

    pub fn compute_max_entry_size(&self) -> usize {
        if let Some(max) = self.skiplist.into_iter().map(|x| {
            Entry::get_encoded_size(&x.key, &x.value)
        }).max() {
            max
        } else {
            0
        }
    }
}

pub struct Memtable {
    pub id: usize,
    pub wal: RwLock<Wal>,
    pub inner: RwLock<MemtableInner>
}

impl Memtable {
    pub fn new(id: usize,
               wal_path: PathBuf,
               opts: Arc<DbOptions>) -> crate::errors::Result<Memtable> {
        let wal = Wal::open(wal_path, &opts)?;
        return Ok(
            Memtable {
                id,
                wal: RwLock::new(wal),
                inner: RwLock::new(MemtableInner::new(opts))
            }
        );
    }

    pub fn restore_wal(&self) -> crate::errors::Result<()> {
        let mut inner = self.inner.write().unwrap();

        let wal = self.wal.read().unwrap();
        let mut wal_iterator = WalIterator::new(&wal);
        for entry in &mut wal_iterator {
            inner.add_to_skip(entry);
        }
        let restore_write_at = wal_iterator.restore_write_at.clone();
        drop(wal_iterator);
        drop(wal);

        if let Some(write_at) = restore_write_at {
            self.wal.write().unwrap()
                .write_at = write_at;
        }

        return Ok(());
    }

    pub fn size(&self) -> usize {
        let skip = &self.inner.read().unwrap().skiplist;
        return skip.size;
    }

    pub fn add(&self, entry: Entry) -> crate::errors::Result<()> {
        {
            self.wal.write().unwrap()
                .add(&entry)?
        }
        {
            self.inner.write().unwrap()
                .add_to_skip(entry);
        }

        return Ok(());
    }

    pub fn truncate(&self) -> crate::errors::Result<()> {
        self.wal.write().unwrap().truncate()
    }
}