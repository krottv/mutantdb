use std::sync::{Arc, RwLock};
use crate::core::SSTableIdGenerator;
use crate::entry::{Entry, Key, ValObj};
use crate::levels::LevelsController;
use crate::sstable::SSTable;
use crate::errors::Result;
use crate::opts::LevelsOptions;
use crate::sstable::builder::Builder;


#[derive(Clone)]
struct Level {
    // sorted except first level. Size is at least 2 guaranteed
    run: Vec<Arc<SSTable>>,
    // id of the level
    id: u32,
    size_on_disk: u64,
}

impl Level {
    pub fn add(&mut self, sstable: Arc<SSTable>) {
        self.size_on_disk += sstable.size_on_disk;
        self.run.push(sstable)
    }

    pub fn new_empty(id: u32) -> Level {
        Level {
            run: Vec::new(),
            id,
            size_on_disk: 0,
        }
    }
}

struct SimpleLevelsController {
    id_generator: SSTableIdGenerator,
    opts: Arc<LevelsOptions>,
    levels: RwLock<Vec<Level>>,
}

impl SimpleLevelsController {
    pub fn new_empty(id_generator: SSTableIdGenerator, opts: Arc<LevelsOptions>) -> Self {
        let mut levels = Vec::with_capacity(opts.num_levels as usize);
        for i in 0..opts.num_levels {
            levels.push(Level::new_empty(i));
        }

        SimpleLevelsController {
            id_generator,
            opts,
            levels: RwLock::new(levels),
        }
    }
    
    // fn check_compact(&self, level_id: u32) -> Result<()> {
    //     let levels = self.levels.read().unwrap();
    // 
    //     // skip if it's the last level
    //     if level_id as usize >= (levels.len() - 1) {
    //         return Ok(());
    //     }
    // 
    //     let level_max_size = self.opts.level_max_size * (self.opts.next_level_size_multiple as u64)
    //         .pow(level_id);
    //     let cur_level = levels.get(level_id as usize).unwrap();
    // 
    //     if cur_level.size_on_disk < level_max_size {
    //         return Ok(());
    //     }
    // 
    //     // start compaction
    //     // merge current level and the next level; which is guaranteed to be present.
    // 
    //     // get total iterator
    //     let total_iterator = self.create_iterator(level_id, &levels);
    //     // new level
    //     let new_level = self.create_level(level_id, total_iterator)?;
    // 
    //     // if not dropped, then it would deadlock.
    //     drop(levels);
    // 
    //     {
    //         let mut levels = self.levels.write().unwrap();
    //         levels[level_id as usize] = Level::new_empty(level_id);
    //         levels[(level_id + 1) as usize] = new_level;
    //     }
    // 
    //     // check for the next level
    //     return self.check_compact(level_id + 1);
    // }
    // 
    // fn create_iterator(&self, level_id: u32, levels: &Vec<Level>) -> impl Iterator<Item=Entry> {}
    // 
    // fn new_builder(&self) -> Result<Builder> {
    //     let path = SSTable::create_path(self.id_generator.get_new());
    // }
    // 
    // fn create_level(&self, id: u32, iterator: impl Iterator<Item=Entry>) -> Result<Level> {
    //     let mut builder = self.new_builder()?;
    //     
    //     let mut level = Level::new_empty(id + 1);
    //     
    //     for entry in iterator {
    //         builder.add_entry(&entry.key, &entry.val_obj)?;
    //         
    //         if true {
    //             let table = builder.build()?;
    //             builder = self.new_builder()?;
    //             level.add(Arc::new(table));
    //         }
    //     }
    //     
    //     return Ok(level)
    // }
}

impl LevelsController for SimpleLevelsController {
    fn add_to_l0(&self, sstable: SSTable) -> Result<()> {
        let sstable_arc = Arc::new(sstable);
        {
            self.levels.write().unwrap().get_mut(0)
                .unwrap().add(sstable_arc.clone());
        }
        // self.check_compact(0)?;

        return Ok(());
    }

    fn get(&self, key: &Key) -> Result<ValObj> {
        todo!()
    }

    fn iter(&self) -> Box<dyn Iterator<Item=Entry>> {
        todo!()
    }
}