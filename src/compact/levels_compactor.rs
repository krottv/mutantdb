use std::sync::{Arc, RwLock};

use crate::compact::{Compactor, LeveledOpts};
use crate::compact::level::Level;
use crate::compact::levels_controller::LevelsController;
use crate::db_options::DbOptions;
use crate::errors::Result;
use crate::sstable::id_generator::SSTableIdGenerator;
use crate::sstable::SSTable;

pub struct LevelsCompactor {
    level_opts: LeveledOpts,
    controller: LevelsController,
}

impl LevelsCompactor {
    pub fn new_empty(id_generator: Arc<SSTableIdGenerator>, level_opts: LeveledOpts, db_opts: Arc<DbOptions>) -> Self {
        // todo: maybe don't create absent levels.
        let mut levels = Vec::with_capacity(level_opts.num_levels as usize);
        for i in 0..level_opts.num_levels {
            levels.push(Level::new_empty(i));
        }

        let controller = LevelsController {
            id_generator,
            db_opts,
            levels: RwLock::new(levels),
        };

        LevelsCompactor {
            level_opts,
            controller,
        }
    }
    
    pub fn check_compact(&self) -> Result<()> {
        todo!()
    }
}

impl Compactor for LevelsCompactor {
    fn add_to_l0(&self, sstable: SSTable) -> Result<()> {
        let sstable_arc = Arc::new(sstable);
        {
            self.controller.levels.write().unwrap().get_mut(0)
                .unwrap().add(sstable_arc);
        }
        
        self.check_compact()?;
        
        return Ok(());
    }

    fn get_controller(&self) -> &LevelsController {
        &self.controller
    }
}