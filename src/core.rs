use std::sync::{Arc, RwLock};
use std::thread::{spawn};

use bytes::Bytes;
use crossbeam_channel::{select, Sender};

use crate::builder::Builder;
use crate::closer::Closer;
use crate::entry::{Entry, EntryComparator, Key, META_ADD, META_DELETE, ValObj};
use crate::errors::Error::IllegalState;
use crate::errors::Result;
use crate::iterators::merge_iterator::MergeIterator;
use crate::compact::{open_compactor, Compactor};
use crate::memtables::Memtables;
use crate::db_options::{DbOptions};
use crate::sstable::id_generator::SSTableIdGenerator;
use crate::sstable::SSTable;

pub struct Mutant {
    inner: Arc<InnerCore>,
    sx: Option<Sender<()>>,
    closer: Closer
}

pub struct InnerCore {
    db_opts: Arc<DbOptions>,
    compactor: Box<dyn Compactor>,
    memtables: RwLock<Memtables>,
    id_generator: Arc<SSTableIdGenerator>,
}

impl Mutant {
    pub fn open(db_opts: Arc<DbOptions>) -> Result<Self> {
        db_opts.create_dirs()?;
        
        let memtables = RwLock::new(Memtables::open(db_opts.clone())?);
        let id_generator = Arc::new(SSTableIdGenerator::new(0));

        let compactor = open_compactor(id_generator.clone(), db_opts.clone())?;

        let inner = InnerCore {
            db_opts,
            compactor,
            memtables,
            id_generator,
        };

        Ok(
            Mutant {
                inner: Arc::new(inner),
                sx: None,
                closer: Closer::new()
            }
        )
    }

    pub fn start_compact_job(&mut self) {
        let (sx, rx) = crossbeam_channel::unbounded();
        self.sx = Some(sx);

        let inner = self.inner.clone();
        let closer_rx = self.closer.receive.clone();

        let _handle = spawn(move || {
            loop {
                select! {
                    recv(rx) -> _msg => inner.do_compaction_no_fail(),
                    recv(closer_rx) -> _msg => break
                }
            }
        });
    }

    pub fn notify_memtable_freeze(&self) {
        // send no matter receiver is hang up.
        if let Some(sx) = &self.sx {
            let _res = sx.send(());
        }
    }

    pub fn add(&self, key: Key, value: Bytes, user_meta: u8) -> Result<()> {
        let val_obj = ValObj {
            value,
            meta: META_ADD,
            user_meta,
            version: 0,
        };

        self.add_inner(key, val_obj)
    }

    pub fn remove(&self, key: Bytes) -> Result<()> {
        let val_obj = ValObj {
            value: Bytes::new(),
            meta: META_DELETE,
            user_meta: 0,
            version: 0,
        };

        self.add_inner(key, val_obj)
    }

    fn add_inner(&self, key: Key, val_obj: ValObj) -> Result<()> {
        //todo: locks the whole memtables for check and freeze_last, which is not good
        // lock-free skiplist wouldn't bring any benefits in such case.
        let mut memtables = self.inner.memtables.write().unwrap();
        if memtables.is_need_to_freeze(Entry::get_encoded_size(&key, &val_obj)) {

            {
                memtables.freeze_last()?;

                self.notify_memtable_freeze();
            }
        }
        drop(memtables);

        self.inner.memtables.read().unwrap().add(Entry {
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
                return Some(memtable_val);
            }
        }

        let levels_val_res = inner_self.compactor.get_controller().get(key);
        if levels_val_res.is_some() {
            let v = levels_val_res.as_ref().unwrap();
            if v.meta == META_DELETE {
                return None;
            }
        }

        return levels_val_res;
    }
}

impl Drop for Mutant {
    fn drop(&mut self) {
        self.closer.close();
    }
}

impl IntoIterator for &Mutant {
    type Item = Entry;
    type IntoIter = MergeIterator<Entry>;

    fn into_iter(self) -> Self::IntoIter {
        let levels_iter = self.inner.compactor.get_controller().iter();
        let memtable_iter = Box::new(Memtables::new_memtables_iterator(&self.inner.memtables.read().unwrap()));
        let entry_comparator = Arc::new(EntryComparator::new(self.inner.db_opts.key_comparator.clone()));

        let iters = vec![levels_iter, memtable_iter];
        MergeIterator::new(iters, entry_comparator)
    }
}

impl InnerCore {
    // flushing is sequential for now
    fn do_compaction(&self) -> Result<()> {
        {
            if let Some(memtable) = self.memtables.read().unwrap().get_back() {
                memtable.truncate()?;
                let new_id = self.id_generator.get_new();
                let sstable_path = self.db_opts.sstables_path().join(SSTable::create_path(new_id));
                let sstable = Builder::build_from_memtable(memtable.clone(), sstable_path, self.db_opts.clone(), new_id)?;
                self.compactor.add_to_l0(sstable)?;
            } else {
                return Err(IllegalState("memtable is absent in do compaction".to_string()));
            }
        }
        {
            self.memtables.write().unwrap().pop_back()?
        }

        return Ok(());
    }


    fn do_compaction_no_fail(&self) {
        let res = self.do_compaction();
        if let Some(err) = res.err() {
            log::warn!("compaction error {}", err.to_string());
        }
    }
}

/*
Test cases:
 - add values in another thread.
 - check that all values are added and only the latest version of them is available
 - check that compaction happened and smth is present in compact
 - check iterator

 todo later:
 - check restoration (manifest)
 */

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;
    use bytes::Bytes;
    use tempfile::tempdir;
    use crate::compact::{CompactionOptions, SimpleLeveledOpts};
    use crate::comparator::BytesI32Comparator;
    use crate::core::Mutant;
    use crate::db_options::{DbOptions};

    pub fn int_to_bytes(v: i32) -> Bytes {
        Bytes::from(v.to_be_bytes().to_vec())
    }

    pub fn bytes_to_int(b: &Bytes) -> i32 {
        let mut arr = [0u8; 4];
        arr.copy_from_slice(b);

        i32::from_be_bytes(arr)
    }

    #[test]
    fn add_many_with_thread() {
        let tmp_dir = tempdir().unwrap();
        let comparator = Arc::new(BytesI32Comparator {});
        let db_opts = Arc::new(
            DbOptions {
                path: tmp_dir.path().to_path_buf(),
                key_comparator: comparator,
                max_memtable_size: 1,
                compaction: Arc::new(CompactionOptions::SimpleLeveled(SimpleLeveledOpts {
                    base_level_size: 10000,
                    num_levels: 3,
                    level_size_multiplier: 10,
                })),
                ..Default::default()
            }
        );
        std::fs::create_dir_all(&db_opts.wal_path()).unwrap();
        std::fs::create_dir_all(&db_opts.sstables_path()).unwrap();

        let mut core = Mutant::open(db_opts).unwrap();
        let _handle = core.start_compact_job();

        for i in 1..100 {
            core.add(int_to_bytes(i), int_to_bytes(i), 0).unwrap();
        }

        for i in 50..100 {
            core.add(int_to_bytes(i), int_to_bytes(i * 10), 0).unwrap();
        }

        //todo: wait for pending compactions to be finished instead of sleep.
        // 100ms is not enough
        sleep(Duration::from_millis(1000));

        assert_eq!(core.inner.memtables.read().unwrap().count(), 1);
        assert!(core.inner.compactor.get_controller().get_sstable_count_total() >= 1);

        for i in 1..50 {
            assert_eq!(bytes_to_int(&core.get(&int_to_bytes(i)).unwrap().value), i);
        }
        for i in 50..100 {
            assert_eq!(bytes_to_int(&core.get(&int_to_bytes(i)).unwrap().value), i * 10);
        }
    }

    #[test]
    fn add_many_manually_compact_one_big_level() {
        let tmp_dir = tempdir().unwrap();
        let comparator = Arc::new(BytesI32Comparator {});
        let db_opts = Arc::new(
            DbOptions {
                path: tmp_dir.path().to_path_buf(),
                key_comparator: comparator,
                max_memtable_size: 1,
                compaction: Arc::new(CompactionOptions::SimpleLeveled(SimpleLeveledOpts {
                    base_level_size: 10000,
                    num_levels: 3,
                    level_size_multiplier: 10,
                })),
                ..Default::default()
            }
        );
        std::fs::create_dir_all(&db_opts.wal_path()).unwrap();
        std::fs::create_dir_all(&db_opts.sstables_path()).unwrap();

        let core = Mutant::open(db_opts).unwrap();

        for i in 1..100 {
            core.add(int_to_bytes(i), int_to_bytes(i), 0).unwrap();
        }

        for i in 50..100 {
            core.add(int_to_bytes(i), int_to_bytes(i * 10), 0).unwrap();
        }

        let memtables_count = core.inner.memtables.read().unwrap().count();
        assert!(memtables_count > 1);
        assert_eq!(core.inner.compactor.get_controller().get_sstable_count_total(), 0);

        let mut iteration = 0usize;
        while memtables_count < iteration {
            iteration += 1;
            core.inner.do_compaction().unwrap();

            assert_eq!(core.inner.memtables.read().unwrap().count(), memtables_count - iteration);
            assert_eq!(core.inner.compactor.get_controller().get_sstable_count_total(), 1);

            for i in 1..50 {
                assert_eq!(bytes_to_int(&core.get(&int_to_bytes(i)).unwrap().value), i);
            }
            for i in 50..100 {
                assert_eq!(bytes_to_int(&core.get(&int_to_bytes(i)).unwrap().value), i * 10);
            }
        }

        for i in 50..100 {
            core.remove(int_to_bytes(i)).unwrap()
        }

        for i in 1..50 {
            assert_eq!(bytes_to_int(&core.get(&int_to_bytes(i)).unwrap().value), i);
        }
        for i in 50..100 {
            assert_eq!(core.get(&int_to_bytes(i)), None);
        }

        // check iterator
        let mut iter = core.into_iter();
        for i in 1..50 {
            assert_eq!(bytes_to_int(&iter.next().unwrap().val_obj.value), i);
        }
    }

    #[test]
    fn add_many_manually_small_levels() {
        let tmp_dir = tempdir().unwrap();
        let comparator = Arc::new(BytesI32Comparator {});
        let db_opts = Arc::new(
            DbOptions {
                path: tmp_dir.path().to_path_buf(),
                key_comparator: comparator,
                max_memtable_size: 1,
                compaction: Arc::new(CompactionOptions::SimpleLeveled(SimpleLeveledOpts {
                    base_level_size: 1,
                    num_levels: 3,
                    level_size_multiplier: 1,
                })),
                ..Default::default()
            }
        );
        std::fs::create_dir_all(&db_opts.wal_path()).unwrap();
        std::fs::create_dir_all(&db_opts.sstables_path()).unwrap();

        let core = Mutant::open(db_opts).unwrap();

        for i in 1..100 {
            core.add(int_to_bytes(i), int_to_bytes(i), 0).unwrap();
        }

        for i in 50..100 {
            core.add(int_to_bytes(i), int_to_bytes(i * 10), 0).unwrap();
        }

        let memtables_count = core.inner.memtables.read().unwrap().count();
        assert!(memtables_count > 1);
        assert_eq!(core.inner.compactor.get_controller().get_sstable_count_total(), 0);

        let mut iteration = 0usize;
        while memtables_count < iteration {
            iteration += 1;
            core.inner.do_compaction().unwrap();

            assert_eq!(core.inner.memtables.read().unwrap().count(), memtables_count - iteration);
            assert_eq!(core.inner.compactor.get_controller().get_sstable_count_total(), iteration);

            for i in 1..50 {
                assert_eq!(bytes_to_int(&core.get(&int_to_bytes(i)).unwrap().value), i);
            }
            for i in 50..100 {
                assert_eq!(bytes_to_int(&core.get(&int_to_bytes(i)).unwrap().value), i * 10);
            }
        }

        // check iterator
        let mut iter = core.into_iter();
        for i in 1..100 {
            let expect = if i >= 50 {
                i * 10
            } else {
                i
            };
            assert_eq!(bytes_to_int(&iter.next().unwrap().val_obj.value), expect);
        }
    }
}