use std::cmp::max;
use std::collections::VecDeque;
use std::path::PathBuf;

use bytes::Bytes;

use crate::entry::{Entry, ValObj};
use crate::errors::Result;
use crate::opts::DbOptions;
use crate::skiplist::{AddResult, SkiplistRaw};
use crate::wal::Wal;

// separate wal and skiplist so that they can be mutated independently
pub struct MemtableInner<'a> {
    pub skiplist: SkiplistRaw<'a, Bytes, ValObj>,
    pub max_version: u64,
    pub max_size: u64,
    pub cur_size: u64,
}

impl<'a> MemtableInner<'a> {
    pub fn new(opts: &'a DbOptions) -> MemtableInner<'a> {
        return MemtableInner {
            skiplist: SkiplistRaw::new(opts.key_comparator.as_ref(), false),
            max_version: 0,
            max_size: opts.max_memtable_size,
            cur_size: 0
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
                self.cur_size -= encoded_size as u64;
            }
        }
        self.cur_size += add_size;
        self.max_version = max(self.max_version, val_version)
    }
}

pub struct Memtable<'a> {
    pub wal: Wal,
    pub id: usize,
    pub inner: MemtableInner<'a>
}

impl<'a> Memtable<'a> {
    pub fn new(id: usize,
               wal_path: PathBuf,
               opts: &'a DbOptions) -> Result<Memtable<'a>> {
        let wal = Wal::open(wal_path, opts)?;
        return Ok(
            Memtable {
                wal,
                id,
                inner: MemtableInner::new(opts)
            }
        );
    }

    pub fn restore_wal(&mut self) -> Result<()> {
        for entry in &mut self.wal {
            self.inner.add_to_skip(entry);
        }

        return Ok(());
    }

    pub fn flush_wal(&self) -> Result<()> {
        self.wal.flush()?;
        return Ok(());
    }
    
    pub fn size(&self) -> usize {
        return self.inner.skiplist.size;
    }
    
    pub fn add(&mut self, entry: Entry) -> Result<()> {
        self.wal.add(&entry)?;
        self.inner.add_to_skip(entry);
        return Ok(());
    }
}

pub struct Memtables<'a> {
    pub mutable: Memtable<'a>,
    pub immutables: VecDeque<Memtable<'a>>,
    pub opts: &'a DbOptions
}

const WAL_FILE_EXT: &str = ".wal";

impl<'a> Memtables<'a> {
    
    // can be extracted to WAL manager like in Rocks db, but not too much code yet here.
    pub fn open(opts: &DbOptions) -> Result<Memtables> {
        std::fs::create_dir_all(&opts.wal_path)?;
        
        let wal_folder = std::fs::read_dir(&opts.wal_path)?;
        let mut memtables: VecDeque<Memtable> = VecDeque::new();
        
        for file_res in wal_folder {
            if let Ok(file) = file_res {
                let name = file.file_name();
                let num_name = &name.to_string_lossy()[0 .. (name.len() - WAL_FILE_EXT.len())];
                
                if let Ok(id) = num_name.parse() {
                    
                    let memtable = Memtable::new(id, file.path(), opts)?;
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
        
        let mutable: Memtable;
        if let Some(mutable_tmp) = memtables.pop_back() {
            mutable = mutable_tmp;
        } else {
            mutable = Memtable::new(1, opts.wal_path.join(Self::id_to_name(1)), &opts)?;
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

    pub fn get(&self, key: &'a Bytes) -> Option<&ValObj> {
        for memtable in MemtablesViewIterator::new(self) {
            match memtable.inner.skiplist.search(key) {
                None => {}
                Some(val_obj) => {
                    return Some(val_obj)
                }
            }
        }
        
        return None
    }

    pub fn add(&mut self, entry: Entry) -> Result<()> {
        let encoded_size = entry.get_encoded_size_entry();
        let remaining_wal_space = self.opts.max_wal_size <= (self.mutable.wal.write_at + encoded_size as u64);
        let remaining_skiplist_size = self.opts.max_memtable_size <= (self.mutable.inner.cur_size + encoded_size as u64);

        // at least one entry will be written
        if self.mutable.inner.skiplist.size > 0 && (remaining_wal_space || remaining_skiplist_size) {
            self.flush()?;
        }
        self.mutable.add(entry)?;
        return Ok(());
    }

    /**
    1. move mutable to immutable
    2. flush its wal
    3. create new mutable
     */
    fn flush(&mut self) -> Result<()> {
        let new_id = self.mutable.id + 1;

        let new_memtable = Memtable::new(new_id, self.next_path(), self.opts)?;

        self.mutable.flush_wal()?;
        
        if self.mutable.size() == 0 {
            panic!("can't flush memtable of 0 size")
        }

        let mut old_memtable = std::mem::replace(&mut self.mutable, new_memtable);
        // remove extra size from the file
        old_memtable.wal.truncate()?;
        self.immutables.push_back(old_memtable);

        return Ok(());
    }

    pub fn pop_front(&mut self) -> Result<()> {
        if self.immutables.len() == 0 {
            panic!("can't pop items of len 0")
        }
        
        let popped = self.immutables.pop_front().unwrap();
        popped.wal.delete()?;

        return Ok(());
    }
}

pub struct MemtablesViewIterator<'a> {
    index: usize,
    memtables: &'a Memtables<'a>
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
    type Item = &'a Memtable<'a>;

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
        
        item
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
    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::comparator::BytesStringUtf8Comparator;
    use crate::entry;
    use crate::entry::Entry;
    use crate::memtable::Memtables;
    use crate::opts::DbOptions;

    #[test]
    fn create_write_read() {
        let e1 = Entry::new(Bytes::from("key1"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("key2"), Bytes::from("value2"), entry::META_ADD);
        
        let comparator = BytesStringUtf8Comparator { };
        let tmp_dir = tempdir().unwrap().path().join("wals");
        
        let opts = DbOptions {
            max_wal_size: 1000,
            max_memtable_size: 1000,
            key_comparator: Box::new(comparator),
            wal_path: tmp_dir,
            ..Default::default()
        };
        
        let mut memtables = Memtables::open(&opts).unwrap();
        
        memtables.add(e1.clone()).unwrap();
        memtables.add(e2.clone()).unwrap();
        
        assert_eq!(memtables.get(&e1.key), Some(&e1.val_obj));
        assert_eq!(memtables.get(&e2.key), Some(&e2.val_obj));
        assert_eq!(memtables.get(&Bytes::from("key10")), None);
        assert_eq!(memtables.immutables.len(), 0);
    }
    
    #[test]
    fn check_reopen() {
        let e1 = Entry::new(Bytes::from("key1"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("key2"), Bytes::from("value2"), entry::META_ADD);
        
        let comparator = BytesStringUtf8Comparator { };
        let tmp_dir = tempdir().unwrap().path().join("wals");
        
        let opts = DbOptions {
            max_wal_size: 1000,
            max_memtable_size: 1000,
            key_comparator: Box::new(comparator),
            wal_path: tmp_dir,
            ..Default::default()
        };
        
        let mut memtables = Memtables::open(&opts).unwrap();
        
        memtables.add(e1.clone()).unwrap();
        memtables.add(e2.clone()).unwrap();
        
        drop(memtables);
        memtables = Memtables::open(&opts).unwrap();
        
        assert_eq!(memtables.get(&e1.key), Some(&e1.val_obj));
        assert_eq!(memtables.get(&e2.key), Some(&e2.val_obj));
        assert_eq!(memtables.get(&Bytes::from("key10")), None);
    }
    
    #[test]
    fn check_flush_works() {
        let e1 = Entry::new(Bytes::from("key1"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("key2"), Bytes::from("value2"), entry::META_ADD);
        let e3 = Entry::new(Bytes::from("key3"), Bytes::from("value3"), entry::META_ADD);
        
        let comparator = BytesStringUtf8Comparator { };
        let tmp_dir = tempdir().unwrap().path().join("wals");
        
        // 0 size makes everything opening a few times.
        let opts = DbOptions {
            max_wal_size: 0,
            max_memtable_size: 0,
            key_comparator: Box::new(comparator),
            wal_path: tmp_dir,
            ..Default::default()
        };
        
        let mut memtables = Memtables::open(&opts).unwrap();
        
        memtables.add(e1.clone()).unwrap();
        memtables.add(e2.clone()).unwrap();
        memtables.add(e3.clone()).unwrap();
        
        assert_eq!(memtables.immutables.len(), 2);
        assert_eq!(memtables.mutable.size(), 1);
        assert_eq!(memtables.immutables[0].size(), 1);
        assert_eq!(memtables.immutables[1].size(), 1);
        assert_eq!(memtables.mutable.id, 3);
        assert_eq!(memtables.immutables[1].id, 2);
        assert_eq!(memtables.immutables[0].id, 1);
        
        drop(memtables);
        memtables = Memtables::open(&opts).unwrap();
        
        assert_eq!(memtables.get(&e3.key), Some(&e3.val_obj));
        assert_eq!(memtables.get(&e2.key), Some(&e2.val_obj));
        assert_eq!(memtables.get(&e1.key), Some(&e1.val_obj));
        assert_eq!(memtables.immutables.len(), 2);
        assert_eq!(memtables.mutable.size(), 1);
        assert_eq!(memtables.get(&Bytes::from("key10")), None);
    }
}



