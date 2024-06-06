use std::cmp::max;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::entry::{Entry, ValObj};
use crate::errors::Result;
use crate::opts::DbOptions;
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
               opts: Arc<DbOptions>) -> Result<Memtable> {
        let wal = Wal::open(wal_path, &opts)?;
        return Ok(
            Memtable {
                id,
                wal: RwLock::new(wal),
                inner: RwLock::new(MemtableInner::new(opts))
            }
        );
    }

    pub fn restore_wal(&self) -> Result<()> {
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
    
    pub fn add(&self, entry: Entry) -> Result<()> {
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
    
    pub fn truncate(&self) -> Result<()> {
        self.wal.write().unwrap().truncate()
    }
}

pub struct Memtables {
    pub mutable: Arc<Memtable>,
    pub immutables: VecDeque<Arc<Memtable>>,
    pub opts: Arc<DbOptions>
}

const WAL_FILE_EXT: &str = ".wal";

impl Memtables {
    
    // can be extracted to WAL manager like in Rocks db, but not too much code yet here.
    pub fn open(opts: Arc<DbOptions>) -> Result<Memtables> {
        std::fs::create_dir_all(&opts.wal_path)?;
        
        let wal_folder = std::fs::read_dir(&opts.wal_path)?;
        let mut memtables = VecDeque::new();
        
        for file_res in wal_folder {
            if let Ok(file) = file_res {
                let name = file.file_name();
                let num_name = &name.to_string_lossy()[0 .. (name.len() - WAL_FILE_EXT.len())];
                
                if let Ok(id) = num_name.parse() {
                    
                    let memtable = Arc::new(Memtable::new(id, file.path(), opts.clone())?);
                    memtables.push_back(memtable);
                }
            }
        }
        
        //todo: what if we create wals that exceed usize::MAX ?
        // illegal state will it be.
        memtables.make_contiguous().sort_unstable_by(| left, right | {
            return left.id.cmp(&right.id)
        });
        
        for memtable in memtables.iter_mut() {
            memtable.restore_wal()?;
        }
        
        let mutable: Arc<Memtable>;
        if let Some(mutable_tmp) = memtables.pop_back() {
            mutable = mutable_tmp;
        } else {
            mutable = Arc::new(Memtable::new(1, opts.wal_path.join(Self::id_to_name(1)), opts.clone())?);
        }
        
        return Ok(
            Memtables {
                mutable,
                immutables: memtables,
                opts
            }
        )
    }
    
    fn id_to_name(id: usize) -> String {
        return format!("{}{}", id, WAL_FILE_EXT);
    }
    
    fn next_path(&self) -> PathBuf {
        let next_id = self.mutable.id + 1;
        let next_path = Self::id_to_name(next_id);
        return self.opts.wal_path.join(next_path)
    }

    pub fn get(&self, key: &Bytes) -> Option<ValObj> {
        for memtable in MemtablesViewIterator::new(self) {
            let skip = &memtable.inner.read().unwrap().skiplist;
            let found = skip.search(key);
            if found.is_some() {
                return found.cloned()
            }
        }
        
        return None
    }

    pub fn is_need_to_freeze(&self, entry_size_bytes: usize) -> bool {
        let wal = self.mutable.wal.read().unwrap();
        let inner = self.mutable.inner.read().unwrap();
        
        let remaining_wal_space = self.opts.max_wal_size <= (wal.write_at + entry_size_bytes as u64);
        let remaining_skiplist_size = self.opts.max_memtable_size <= (&inner.cur_size.load(Ordering::Relaxed) + entry_size_bytes as u64);
        return inner.skiplist.size > 0 && (remaining_wal_space || remaining_skiplist_size)
    }

    pub fn add(&self, entry: Entry) -> Result<()> {
        return self.mutable.add(entry);
    }

    /**
    1. move mutable to immutable
    2. create new mutable
     */
    pub fn freeze_last(&mut self) -> Result<()> {
        if self.mutable.size() == 0 {
            panic!("can't freeze memtable of 0 size")
        }

        let new_id = self.mutable.id + 1;
        let new_memtable = Arc::new(Memtable::new(new_id, self.next_path(), self.opts.clone())?);

        let old_memtable = std::mem::replace(&mut self.mutable, new_memtable);
        self.immutables.push_front(old_memtable);

        return Ok(());
    }

    pub fn get_back(&self) -> Option<Arc<Memtable>> {
        return self.immutables.back().map(|x| {
            x.clone()
        })
    }

    pub fn pop_back(&mut self) -> Result<()> {
        if let Some(_popped) = self.immutables.pop_back() {
            
        } else {
            panic!("can't pop items of len 0")
        }

        return Ok(());
    }
    
    pub fn count(&self) -> usize {
        return 1 + &self.immutables.len();
    }
}

pub struct MemtablesViewIterator<'a> {
    index: usize,
    memtables: &'a Memtables
}

impl<'a> MemtablesViewIterator<'a> {
    pub fn new(memtables: &'a Memtables) -> MemtablesViewIterator<'a> {
        return MemtablesViewIterator {
            index: 0,
            memtables
        }
    }
}

impl<'a> Iterator for MemtablesViewIterator<'a> {
    type Item = Arc<Memtable>;

    fn next(&mut self) -> Option<Self::Item> {
        let size = self.memtables.immutables.len() + 1;
        let item = if self.index >= size {
            None
        } else if self.index == 0 {
            Some(&self.memtables.mutable)
        } else {
            Some(self.memtables.immutables.get(self.index - 1).unwrap())
        };
        
        self.index += 1;
        
        item.map(|x| {
            x.clone()
        })
    }
}

/**
Test cases:
1. write and read values. check if absent
2. check that write sometimes flushes files to disk. (new memtables created)
3. check that reopen works.
4. check deletion works
*/

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::comparator::BytesStringUtf8Comparator;
    use crate::entry;
    use crate::entry::Entry;
    use crate::iterators::memtable_iterator::MemtableIterator;
    use crate::memtable::Memtables;
    use crate::opts::DbOptions;

    #[test]
    fn create_write_read() {
        let e1 = Entry::new(Bytes::from("key1"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("key2"), Bytes::from("value2"), entry::META_ADD);
        
        let comparator = BytesStringUtf8Comparator { };
        let tmp_dir = tempdir().unwrap();
        let wal_path = tmp_dir.path().join("wals");
        fs::create_dir_all(&wal_path).unwrap();
        
        let opts = Arc::new(DbOptions {
            max_wal_size: 1000,
            max_memtable_size: 1000,
            key_comparator: Arc::new(comparator),
            wal_path,
            ..Default::default()
        });
        
        let memtables = Memtables::open(opts).unwrap();
        
        memtables.add(e1.clone()).unwrap();
        memtables.add(e2.clone()).unwrap();
        
        assert_eq!(memtables.get(&e1.key), Some(e1.val_obj.clone()));
        assert_eq!(memtables.get(&e2.key), Some(e2.val_obj.clone()));
        assert_eq!(memtables.get(&Bytes::from("key10")), None);
        assert_eq!(memtables.immutables.len(), 0);
        
        let memtable = memtables.mutable;
        let mut iterator = MemtableIterator::new_from_mem(memtable);
        assert_eq!(iterator.next(), Some(e1.clone()));
        assert_eq!(iterator.next(), Some(e2.clone()));
        assert_eq!(iterator.next(), None);
    }
    
    #[test]
    fn check_reopen() {
        let e1 = Entry::new(Bytes::from("key1"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("key2"), Bytes::from("value2"), entry::META_ADD);
        
        let comparator = BytesStringUtf8Comparator { };
        let tmp_dir = tempdir().unwrap();
        let wal_path = tmp_dir.path().join("wals");
        fs::create_dir_all(&wal_path).unwrap();
        
        let opts = Arc::new(DbOptions {
            max_wal_size: 1000,
            max_memtable_size: 1000,
            key_comparator: Arc::new(comparator),
            wal_path,
            ..Default::default()
        });
        
        let mut memtables = Memtables::open(opts.clone()).unwrap();
        
        memtables.add(e1.clone()).unwrap();
        memtables.add(e2.clone()).unwrap();
        
        drop(memtables);
        memtables = Memtables::open(opts).unwrap();
        
        assert_eq!(memtables.get(&e1.key), Some(e1.val_obj.clone()));
        assert_eq!(memtables.get(&e2.key), Some(e2.val_obj.clone()));
        assert_eq!(memtables.get(&Bytes::from("key10")), None);
    }
    
    #[test]
    fn check_flush_works() {
        let e1 = Entry::new(Bytes::from("key1"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("key2"), Bytes::from("value2"), entry::META_ADD);
        let e3 = Entry::new(Bytes::from("key2"), Bytes::from("value20"), entry::META_ADD);
        
        let comparator = BytesStringUtf8Comparator { };
        let tmp_dir = tempdir().unwrap();
        let wal_path = tmp_dir.path().join("wals");
        fs::create_dir_all(&wal_path).unwrap();
        
        // 0 size makes everything opening a few times.
        let opts = Arc::new(DbOptions {
            max_wal_size: 0,
            max_memtable_size: 0,
            key_comparator: Arc::new(comparator),
            wal_path,
            ..Default::default()
        });
        
        let mut memtables = Memtables::open(opts.clone()).unwrap();
        
        memtables.add(e1.clone()).unwrap();
        memtables.freeze_last().unwrap();
        memtables.add(e2.clone()).unwrap();
        memtables.freeze_last().unwrap();
        memtables.add(e3.clone()).unwrap();
        
        assert_eq!(memtables.immutables.len(), 2);
        assert_eq!(memtables.mutable.size(), 1);
        assert_eq!(memtables.immutables[0].size(), 1);
        assert_eq!(memtables.immutables[1].size(), 1);
        assert_eq!(memtables.mutable.id, 3);
        assert_eq!(memtables.immutables[1].id, 1);
        assert_eq!(memtables.immutables[0].id, 2);
        
        drop(memtables);
        memtables = Memtables::open(opts).unwrap();

        assert_eq!(memtables.get(&e1.key), Some(e1.val_obj.clone()));
        assert_eq!(memtables.get(&e2.key), Some(e3.val_obj.clone()));
        assert_eq!(memtables.get(&e3.key), Some(e3.val_obj.clone()));
        assert_eq!(memtables.immutables.len(), 2);
        assert_eq!(memtables.mutable.size(), 1);
        assert_eq!(memtables.get(&Bytes::from("key10")), None);
        
        // check iterator
        
        let mut iterator = Memtables::new_memtables_iterator(&memtables);

        assert_eq!(iterator.next(), Some(e1.clone()));
        assert_eq!(iterator.next(), Some(e3.clone()));
        assert_eq!(iterator.next(), None);
    }
}



