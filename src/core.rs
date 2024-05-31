use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};
use std::thread;
use bytes::Bytes;
use crate::levels::LevelsController;
use crate::entry::{Entry, Key, META_ADD, META_DELETE, ValObj};
use crate::errors::Error::{AbsentKey, IllegalState};
use crate::memtable::Memtables;
use crate::opts::DbOptions;
use crate::errors::Result;
use crate::logger::Logger;
use crate::sstable::builder::Builder;
use crate::sstable::SSTable;

pub struct Core {
    opts: Arc<DbOptions>,
    levels: Box<dyn LevelsController>,
    memtables: RwLock<Memtables>,
    sx: Sender<()>,
    rx: Receiver<()>,
    logger: Box<dyn Logger>
}

impl Core {
    // pub fn start(&self) {
    //     let t = thread::spawn(|| {
    //          self.compaction_job();
    //     });
    // }
    
    fn compaction_job(&self) {
        for _nothing in self.rx.iter() {
            let res = self.try_flush_memtable();
            if let Some(err) = res.err() {
                self.logger.on_compaction_error(err);
            }
        }
    }
    
    // flushing is sequential for now
    fn try_flush_memtable(&self) -> Result<()> {
        {
            if let Some(memtable) = self.memtables.write().unwrap().get_first_mut() {
                memtable.wal.truncate()?;
            } else {
                return Err(IllegalState("memtable is absent in flush 1".to_string()))
            }
        }
        {
            if let Some(memtable) = self.memtables.read().unwrap().get_first() {
                let sstable_path = SSTable::create_path(memtable.id);
                let sstable = Builder::build_from_memtable(memtable, sstable_path, self.opts.clone())?;
                self.levels.add_to_l0(sstable)?;
                
            } else {
                return Err(IllegalState("memtable is absent in flush 2".to_string()))
            }
        }
        {
            self.memtables.write().unwrap().pop_front()?
        }
        
        return Ok(())
    }
    
    pub fn add(&self, key: Key, value: Bytes, user_meta: u8) -> Result<()> {
        let val_obj = ValObj {
            value,
            meta: META_ADD,
            user_meta, 
            version: 0
        };
        
        let encoded_size = Entry::get_encoded_size(&key, &val_obj);
        
        let mut memtables = self.memtables.write().unwrap();
        
        if memtables.is_need_to_freeze(encoded_size) { 
            memtables.freeze_last()?;
            self.notify_memtable_freeze();
        }
        
        memtables.add(Entry {
            key,
            val_obj
        })
    }
    
    pub fn notify_memtable_freeze(&self) {
        // send no matter receiver is hang up.
        let _res = self.sx.send(());
    }
    
    pub fn get(&self, key: &Key) -> Result<ValObj> {
        {
            if let Some(memtable_val) = self.memtables.read().unwrap().get(key) {
                if memtable_val.meta == META_DELETE {
                    return Err(AbsentKey)
                }
                return Ok(memtable_val.clone());
            }
        }
        
        let levels_val_res = self.levels.get(key);
        if levels_val_res.is_ok() {
            let v = levels_val_res.as_ref().unwrap();
            if v.meta == META_DELETE {
                return Err(AbsentKey)
            }
        }
        
        return levels_val_res;
    }
}