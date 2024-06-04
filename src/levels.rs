/*
When to compact:

- When numbers of memtables exceeds threshold.
- When other levels exceed some threshold
 */

pub mod simple_levels_controller;

use std::sync::Arc;
use crate::entry::{Entry, Key, ValObj};
use crate::sstable::SSTable;
use crate::errors::Result;
use crate::levels::simple_levels_controller::SimpleLevelsController;
use crate::opts::{DbOptions, LevelsOptions};
use crate::sstable::id_generator::SSTableIdGenerator;

pub trait LevelsController: Send + Sync {
    // not mut because it should handle concurrency inside
    fn add_to_l0(&self, sstable: SSTable) -> Result<()>;
    fn get(&self, key: &Key) -> Option<ValObj>;
    fn iter(&self) -> Box<dyn Iterator<Item=Entry>>;
    fn get_sstable_count(&self, level_id: u32) -> Option<usize>;
    
    fn get_sstable_count_total(&self) -> usize;
}

pub enum CompactionStrategy {
    SimpleLeveled
}

pub(crate) fn create_controller(id_generator: Arc<SSTableIdGenerator>,
                                level_opts: Arc<LevelsOptions>,
                                db_opts: Arc<DbOptions>) -> Box<dyn LevelsController> {
    match db_opts.compaction_strategy {
        CompactionStrategy::SimpleLeveled => {
            Box::new(
                SimpleLevelsController::new_empty(id_generator, level_opts, db_opts)
            )
        }
    }
}