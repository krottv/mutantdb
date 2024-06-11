/*
When to compact:

- When numbers of memtables exceeds threshold.
- When other compact exceed some threshold
 */

use std::sync::Arc;
use crate::compact::levels_compactor::LevelsCompactor;

use crate::compact::levels_controller::LevelsController;
use crate::compact::simple_levels_compactor::SimpleLevelsCompactor;
use crate::db_options::DbOptions;
use crate::errors::Result;
use crate::sstable::id_generator::SSTableIdGenerator;
use crate::sstable::SSTable;

pub mod simple_levels_compactor;
pub mod level;
pub mod levels_controller;
pub mod levels_compactor;
pub mod targets;

pub trait Compactor: Send + Sync {
    // not mut because it should handle concurrency inside
    fn add_to_l0(&self, sstable: SSTable) -> Result<()>;
    fn get_controller(&self) -> &LevelsController;
}


pub(crate) fn create_compactor(id_generator: Arc<SSTableIdGenerator>,
                               db_opts: Arc<DbOptions>) -> Box<dyn Compactor> {
    db_opts.compaction.validate();
    
    match db_opts.compaction.as_ref() {
        CompactionOptions::SimpleLeveled(level_opts) => {
            Box::new(
                SimpleLevelsCompactor::new_empty(id_generator, level_opts.clone(), db_opts)
            )
        }
        CompactionOptions::Leveled(level_opts) => {
            Box::new(
                LevelsCompactor::new_empty(id_generator, level_opts.clone(), db_opts)
            )
        }
    }
}

pub enum CompactionOptions {
    SimpleLeveled(SimpleLeveledOpts),
    Leveled(LeveledOpts)
}

impl CompactionOptions {
    pub fn validate(&self) {
        match self {
            CompactionOptions::SimpleLeveled(opts) => {
                opts.validate()
            }
            CompactionOptions::Leveled(opts) => {
                opts.validate()
            }
        }
    }
}

#[derive(Clone)]
pub struct LeveledOpts {
    pub base_level_size: u64,

    // second_level_max_size = next_level_size_multiple * base_level_size
    pub level_size_multiplier: u32,

    pub num_levels: usize,
    
    pub level0_file_num_compaction_trigger: u32,
    
    // number of threads that can do parallel compaction of non-conflicting levels. Like 1 -> 2 and 3 -> 4
    pub num_parallel_compact: usize
}

impl LeveledOpts {
    pub fn validate(&self) {
        if self.num_levels < 2 {
            panic!("num_levels cannot be less then 2")
        }
    }
}

impl Default for LeveledOpts {
    fn default() -> Self {
        LeveledOpts {
            // 20 mb
            base_level_size: 200000,

            level_size_multiplier: 10,

            num_levels: 7,
            
            level0_file_num_compaction_trigger: 5,
            
            num_parallel_compact: 2
        }
    }
}

#[derive(Clone)]
pub struct SimpleLeveledOpts {
    pub base_level_size: u64,

    // second_level_max_size = next_level_size_multiple * base_level_size
    pub level_size_multiplier: u32,

    pub num_levels: usize
}

impl SimpleLeveledOpts {
    pub fn validate(&self) {
        if self.num_levels < 2 {
            panic!("num_levels cannot be less then 2")
        }
    }
}

impl Default for SimpleLeveledOpts {
    fn default() -> Self {
        SimpleLeveledOpts {
            // 20 mb
            base_level_size: 200000,

            level_size_multiplier: 10,

            num_levels: 7
        }
    }
}