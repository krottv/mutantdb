use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;

use crate::entry::{Entry, ValObj};
use crate::errors::Result;
use crate::memtables::memtable::Memtable;
use crate::db_options::DbOptions;

pub mod memtable;

pub struct Memtables {
    pub mutable: Arc<Memtable>,
    pub immutables: VecDeque<Arc<Memtable>>,
    pub opts: Arc<DbOptions>
}

const WAL_FILE_EXT: &str = ".wal";

impl Memtables {
    
    // can be extracted to WAL manager like in Rocks db, but not too much code yet here.
    pub fn open(opts: Arc<DbOptions>) -> Result<Memtables> {
        std::fs::create_dir_all(&opts.wal_path())?;
        
        let wal_folder = std::fs::read_dir(&opts.wal_path())?;
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
            mutable = Arc::new(Memtable::new(1, opts.wal_path()
                .join(Self::id_to_name(1)), opts.clone())?);
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
        return self.opts.wal_path().join(next_path)
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
            _popped.wal.write().unwrap().mark_delete();
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
    use crate::memtables::Memtables;
    use crate::db_options::DbOptions;

    #[test]
    fn create_write_read() {
        let e1 = Entry::new(Bytes::from("key1"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("key2"), Bytes::from("value2"), entry::META_ADD);
        
        let comparator = BytesStringUtf8Comparator { };
        let tmp_dir = tempdir().unwrap();
        let path = tmp_dir.path().to_path_buf();
        
        let opts = Arc::new(DbOptions {
            max_wal_size: 1000,
            max_memtable_size: 1000,
            key_comparator: Arc::new(comparator),
            path,
            ..Default::default()
        });

        fs::create_dir_all(&opts.wal_path()).unwrap();
        
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
        let path = tmp_dir.path().to_path_buf();
        
        let opts = Arc::new(DbOptions {
            max_wal_size: 1000,
            max_memtable_size: 1000,
            key_comparator: Arc::new(comparator),
            path,
            ..Default::default()
        });
        fs::create_dir_all(&opts.wal_path()).unwrap();
        
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
        let path = tmp_dir.path().to_owned();
        
        // 0 size makes everything opening a few times.
        let opts = Arc::new(DbOptions {
            max_wal_size: 0,
            max_memtable_size: 0,
            key_comparator: Arc::new(comparator),
            path,
            ..Default::default()
        });
        fs::create_dir_all(&opts.wal_path()).unwrap();
        
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



