use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use crate::compact::{Compactor, LeveledOpts};
use crate::compact::level::Level;
use crate::compact::levels_controller::LevelsController;
use crate::compact::targets::Targets;
use crate::db_options::DbOptions;
use crate::entry::{Entry, EntryComparator};
use crate::errors::Result;
use crate::iterators::merge_iterator::MergeIterator;
use crate::sstable::id_generator::SSTableIdGenerator;
use crate::sstable::SSTable;

pub struct LevelsCompactor {
    level_opts: LeveledOpts,
    controller: LevelsController,
}

struct LevelPriority {
    score: f64,
    level_id: usize,
}

impl LevelsCompactor {
    pub fn new_empty(id_generator: Arc<SSTableIdGenerator>, level_opts: LeveledOpts, db_opts: Arc<DbOptions>) -> Self {
        // todo: maybe don't create absent levels.
        let mut levels = Vec::with_capacity(level_opts.num_levels);
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

    // todo: refactor. too big method.
    pub fn do_compact(&self) -> Result<bool> {
        let targets = self.compute_targets();
        let mut priorities = self.compute_priorities(&targets);

        if let Some(last_priority) = priorities.pop() {
            let from_level_id = last_priority.level_id;
            let to_level_id = if from_level_id == 0 {
                targets.base_level
            } else {
                from_level_id + 1
            };

            let levels = self.controller.levels.read().unwrap();
            let from_level = &levels[last_priority.level_id];
            let to_level = &levels[to_level_id];

            let from_sstables: Vec<Arc<SSTable>>;

            if from_level_id == 0 {
                from_sstables = from_level.run.iter().cloned().collect();
            } else {
                from_sstables = vec![from_level.select_oldest_sstable().unwrap()]
            }

            let lowest_key = from_sstables.iter().min_by(|x, y| {
                self.controller.db_opts.key_comparator.compare(&x.first_key, &y.first_key)
            }).unwrap();

            let highest_key = from_sstables.iter().min_by(|x, y| {
                self.controller.db_opts.key_comparator.compare(&x.last_key, &y.last_key)
            }).unwrap();
            
            let to_sstables = to_level.select_sstables(&lowest_key.first_key, &highest_key.last_key, self.controller.db_opts.clone());

            drop(levels);
            
            // building new tables
            let entry_comparator = Arc::new(EntryComparator::new(self.controller.db_opts.key_comparator.clone()));
            let from_iterator = Level::create_iterator_for_tables(entry_comparator.clone(), from_level_id, &from_sstables);
            let to_iterator = Level::create_iterator_for_tables(entry_comparator.clone(), to_level_id, &to_sstables);
            let iterators: Vec<Box<dyn Iterator<Item=Entry>>> = vec![from_iterator, to_iterator];
            let total_iter = MergeIterator::new(iterators, entry_comparator);
            
            // get new tables
            let new_tables = self.controller.create_sstables(total_iter)?;


            // produce new levels
            let keys_to_remove_from: HashSet<usize> = from_sstables.iter().map(|x| {
                x.id
            }).collect();

            let levels = self.controller.levels.read().unwrap();
            let mut from_tables_complete = (&levels[from_level_id].run).clone();
            from_tables_complete.retain(|x| {
                !keys_to_remove_from.contains(&x.id)
            });
            let mut new_level_from = Level {
                run: from_tables_complete,
                id: from_level_id,
                size_on_disk: 0
            };
            new_level_from.recalc_run();

            let keys_to_remove_to: HashSet<usize> = to_sstables.iter().map(|x| {
                x.id
            }).collect();
            let mut to_tables_complete = (&levels[to_level_id].run).clone();
            to_tables_complete.retain(|x| {
                !keys_to_remove_to.contains(&x.id)
            });

            for new_table in new_tables {
                to_tables_complete.push_back(new_table);
            }

            to_tables_complete.make_contiguous().sort_unstable_by(|x, y| {
                // since keys are non-overlapping, order doesn't matter
                self.controller.db_opts.key_comparator.compare(&x.first_key, &y.first_key)
            });
            let mut new_level_to = Level {
                run: to_tables_complete,
                id: to_level_id,
                size_on_disk: 0
            };
            new_level_to.recalc_run();
            
            drop(levels);
            
            // write new levels
            {
                let mut levels = self.controller.levels.write().unwrap();
                levels[from_level_id] = new_level_from;
                levels[to_level_id] = new_level_to;
            }
            
            // delete old sstables
            for sstable in from_sstables {
                sstable.mark_delete()
            }
            for sstable in to_sstables {
                sstable.mark_delete()
            }

            return Ok(true);
        } else {
            return Ok(false)
        }
    }

    fn compute_targets(&self) -> Targets {
        let x = &self.controller.levels.read().unwrap()[1..];
        let current_sizes: Vec<u64> = x
            .iter().map(|x| {
            x.size_on_disk
        }).collect();

        Targets::compute(current_sizes, self.level_opts.base_level_size, self.level_opts.level_size_multiplier)
    }

    fn compute_priorities(&self, targets: &Targets) -> Vec<LevelPriority> {
        let l0_run_len = self.controller.levels.read().unwrap()[0].run.len();

        let mut priorities = Vec::with_capacity(targets.current_sizes.len() + 1);

        let first_score = if l0_run_len >= self.level_opts.level0_file_num_compaction_trigger as usize {
            f64::MAX
        } else {
            0f64
        };
        priorities.push(LevelPriority {
            score: first_score,
            level_id: 0,
        });

        // don't include last level.
        for i in 0..targets.target_sizes.len() - 1 {
            let score = targets.current_sizes[i] as f64 / targets.target_sizes[i] as f64;
            priorities.push(LevelPriority {
                score,
                level_id: i + 1,
            });
        }

        priorities.retain(|x| {
            x.score > 1f64
        });

        priorities.sort_unstable_by(|a, b| {
            a.score.total_cmp(&b.score)
        });

        priorities
    }
}


impl Compactor for LevelsCompactor {
    fn add_to_l0(&self, sstable: SSTable) -> Result<()> {
        let sstable_arc = Arc::new(sstable);
        {
            self.controller.levels.write().unwrap().get_mut(0)
                .unwrap().add(sstable_arc);
        }

        self.do_compact()?;

        return Ok(());
    }

    fn get_controller(&self) -> &LevelsController {
        &self.controller
    }
}

// todo: tests