use std::sync::{Arc, mpsc, RwLock};
use std::sync::mpsc::Sender;
use std::thread::{JoinHandle, spawn};

use bytes::Bytes;

use crate::builder::Builder;
use crate::entry::{Entry, Key, META_ADD, META_DELETE, ValObj};
use crate::errors::Error::IllegalState;
use crate::errors::Result;
use crate::levels::{create_controller, LevelsController};
use crate::memtable::Memtables;
use crate::opts::{DbOptions, LevelsOptions};
use crate::sstable::id_generator::SSTableIdGenerator;
use crate::sstable::SSTable;

pub struct Core {
    inner: Arc<InnerCore>,
    sx: Option<Sender<()>>,
}

pub struct InnerCore {
    db_opts: Arc<DbOptions>,
    lcontroller: Box<dyn LevelsController>,
    memtables: RwLock<Memtables>,
    id_generator: Arc<SSTableIdGenerator>,
}

impl Core {
    pub fn new(db_opts: Arc<DbOptions>, level_opts: Arc<LevelsOptions>) -> Result<Self> {
        let memtables = RwLock::new(Memtables::open(db_opts.clone())?);
        // todo: set right id after restoring the manifest.
        let id_generator = Arc::new(SSTableIdGenerator::new(1));

        let lcontroller = create_controller(id_generator.clone(), level_opts, db_opts.clone());

        let inner = InnerCore {
            db_opts,
            lcontroller,
            memtables,
            id_generator,
        };

        Ok(
            Core {
                inner: Arc::new(inner),
                sx: None,
            }
        )
    }

    pub fn start_compact_job(&mut self) -> JoinHandle<()> {
        let (sx, rx) = mpsc::channel();
        self.sx = Some(sx);

        let inner = self.inner.clone();

        spawn(move || {
            for _nothing in rx.iter() {
                inner.compaction_job();
            }
        })
    }

    pub fn notify_memtable_freeze(&self) {
        // send no matter receiver is hang up.
        if let Some(sx) = &self.sx {
            let _res = sx.send(());
        }
    }

    pub fn add(&self, key: Key, value: Bytes, user_meta: u8) -> Result<()> {
        let inner_self = &self.inner;
        let val_obj = ValObj {
            value,
            meta: META_ADD,
            user_meta,
            version: 0,
        };

        let encoded_size = Entry::get_encoded_size(&key, &val_obj);

        let mut memtables = inner_self.memtables.write().unwrap();

        if memtables.is_need_to_freeze(encoded_size) {
            memtables.freeze_last()?;
            self.notify_memtable_freeze();
        }

        memtables.add(Entry {
            key,
            val_obj,
        })
    }

    pub fn get(&self, key: &Key) -> Option<ValObj> {
        let inner_self = &self.inner;
        {
            if let Some(memtable_val) = inner_self.memtables.read().unwrap().get(key) {
                if memtable_val.meta == META_DELETE {
                    return None;
                }
                return Some(memtable_val.clone());
            }
        }

        let levels_val_res = inner_self.lcontroller.get(key);
        if levels_val_res.is_some() {
            let v = levels_val_res.as_ref().unwrap();
            if v.meta == META_DELETE {
                return None;
            }
        }

        return levels_val_res;
    }

    // todo: shouldn't obtain read lock for everything logically.
    // pub fn get_iter(&self) -> MergeIterator<Entry> {
    //     let levels_iter = self.inner.levels.iter();
    //     let memtable_iter = self.inner.memtables.read().unwrap().
    // }
}

impl InnerCore {
    // flushing is sequential for now
    fn try_flush_memtable(&self) -> Result<()> {
        {
            if let Some(memtable) = self.memtables.write().unwrap().get_first_mut() {
                memtable.wal.truncate()?;
            } else {
                return Err(IllegalState("memtable is absent in flush 1".to_string()));
            }
        }
        {
            if let Some(memtable) = self.memtables.read().unwrap().get_first() {
                let sstable_path = self.db_opts.sstables_path.join(SSTable::create_path(self.id_generator.get_new()));
                let sstable = Builder::build_from_memtable(memtable, sstable_path, self.db_opts.clone())?;
                self.lcontroller.add_to_l0(sstable)?;
            } else {
                return Err(IllegalState("memtable is absent in flush 2".to_string()));
            }
        }
        {
            self.memtables.write().unwrap().pop_front()?
        }

        return Ok(());
    }


    fn compaction_job(&self) {
        let res = self.try_flush_memtable();
        if let Some(err) = res.err() {
            log::warn!("compaction error {}", err.to_string());
        }
    }
}

/*
Test cases:
 - add values in another thread.
 - check that all values are added and only the latest version of them is available
 - check that compaction happened and smth is present in levels
 
 todo later:
 - check restoration (manifest)
 - check iterator
 */

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use bytes::Bytes;
    use tempfile::tempdir;
    use crate::comparator::BytesI32Comparator;
    use crate::core::Core;
    use crate::opts::{DbOptions, LevelsOptions};

    fn int_to_bytes(v: i32) -> Bytes {
        Bytes::from(v.to_be_bytes().to_vec())
    } 
    fn bytes_to_int(b: Bytes) -> i32 {
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&b);
        
        i32::from_be_bytes(arr)
    }

    #[test]
    fn add_many() {
        let tmp_dir = tempdir().unwrap();
        let comparator = Arc::new(BytesI32Comparator {});
        let db_opts = Arc::new(
            DbOptions {
                wal_path: tmp_dir.path().join("wal"),
                sstables_path: tmp_dir.path().join("mem"),
                key_comparator: comparator,
                max_memtable_size: 1,
                ..Default::default()
            }
        );
        let level_opts = Arc::new(
            LevelsOptions {
                level_max_size: 1,
                num_levels: 3,
                next_level_size_multiple: 1,
            }
        );

        let mut core = Core::new(db_opts, level_opts).unwrap();
        let _handle = core.start_compact_job();

        for i in 1..100 {
            core.add(int_to_bytes(i), int_to_bytes(i), 0).unwrap();
        }

        for i in 50..100 {
            core.add(int_to_bytes(i), int_to_bytes(i * 10), 0).unwrap();
        }

        // todo: wait for pending compactions to be finished. 
        // in another test

        for i in 1..50 {
            assert_eq!(bytes_to_int(core.get(&int_to_bytes(i)).unwrap().value), i);
        }
        for i in 50..100 {
            assert_eq!(bytes_to_int(core.get(&int_to_bytes(i)).unwrap().value), i * 10);
        }
    }
}