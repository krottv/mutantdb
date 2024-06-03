use std::sync::{Arc, mpsc, RwLock};
use std::sync::mpsc::Sender;
use std::thread::{JoinHandle, spawn};

use bytes::Bytes;

use crate::builder::Builder;
use crate::entry::{Entry, Key, META_ADD, META_DELETE, ValObj};
use crate::errors::Error::IllegalState;
use crate::errors::Result;
use crate::levels::LevelsController;
use crate::logger::Logger;
use crate::memtable::Memtables;
use crate::opts::DbOptions;
use crate::sstable::id_generator::SSTableIdGenerator;
use crate::sstable::SSTable;

pub struct Core {
    inner: Arc<InnerCore>,
    sx: Option<Sender<()>>,
}

pub struct InnerCore {
    opts: Arc<DbOptions>,
    levels: Box<dyn LevelsController>,
    memtables: RwLock<Memtables>,
    logger: Box<dyn Logger>,
    id_generator: SSTableIdGenerator,
}

impl Core {
    
    pub fn start(&mut self) -> JoinHandle<()> {
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

        let levels_val_res = inner_self.levels.get(key);
        if levels_val_res.is_some() {
            let v = levels_val_res.as_ref().unwrap();
            if v.meta == META_DELETE {
                return None;
            }
        }

        return levels_val_res;
    }
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
                let sstable_path = SSTable::create_path(self.id_generator.get_new());
                let sstable = Builder::build_from_memtable(memtable, sstable_path, self.opts.clone())?;
                self.levels.add_to_l0(sstable)?;
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
            self.logger.on_compaction_error(err);
        }
    }
}
