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

    pub fn do_compact(&self) -> Result<bool> {
        let targets = self.compute_targets();
        let l0_run_len = self.controller.levels.read().unwrap()[0].run.len();
        let mut priorities = self.compute_priorities(&targets, l0_run_len);

        return if let Some(last_priority) = priorities.pop() {
            let from_level_id = last_priority.level_id;
            let to_level_id = if from_level_id == 0 {
                targets.base_level
            } else {
                from_level_id + 1
            };

            let (from_sstables, to_sstables) = self.select_tables_for_merge(from_level_id, to_level_id);

            // building new tables
            let total_iter = self.create_iterator_for_tables(from_level_id, to_level_id, &from_sstables, &to_sstables);
            let new_tables = self.controller.create_sstables(total_iter)?;

            let keys_to_remove_to = to_sstables.iter().map(|x| {
                x.id
            }).collect();

            let keys_to_remove_from: HashSet<usize> = from_sstables.iter().map(|x| {
                x.id
            }).collect();

            self.change_levels(from_level_id, to_level_id, keys_to_remove_from, keys_to_remove_to, new_tables)?;

            // delete old sstables
            for sstable in from_sstables {
                sstable.mark_delete()
            }
            for sstable in to_sstables {
                sstable.mark_delete()
            }

            Ok(true)
        } else {
            Ok(false)
        };
    }

    fn create_iterator_for_tables(&self,
                                  from_level_id: usize,
                                  to_level_id: usize,
                                  from_sstables: &Vec<Arc<SSTable>>,
                                  to_sstables: &Vec<Arc<SSTable>>) -> MergeIterator<Entry> {
        let entry_comparator = Arc::new(EntryComparator::new(self.controller.db_opts.key_comparator.clone()));
        let from_iterator = Level::create_iterator_for_tables(entry_comparator.clone(), from_level_id, from_sstables);
        let to_iterator = Level::create_iterator_for_tables(entry_comparator.clone(), to_level_id, to_sstables);
        let iterators: Vec<Box<dyn Iterator<Item=Entry>>> = vec![from_iterator, to_iterator];
        MergeIterator::new(iterators, entry_comparator)
    }

    fn select_tables_for_merge(&self,
                               from_level_id: usize,
                               to_level_id: usize) -> (Vec<Arc<SSTable>>, Vec<Arc<SSTable>>) {
        if to_level_id <= 0 || to_level_id <= from_level_id {
            panic!("wrong ids are provided")
        }

        let levels = self.controller.levels.read().unwrap();
        let from_level = &levels[from_level_id];
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

        let highest_key = from_sstables.iter().max_by(|x, y| {
            self.controller.db_opts.key_comparator.compare(&x.last_key, &y.last_key)
        }).unwrap();

        let lowest_key_ref = &lowest_key.first_key;
        let highest_key_ref = &highest_key.last_key;

        let to_sstables = to_level.select_sstables(lowest_key_ref, highest_key_ref, self.controller.db_opts.clone());

        return (from_sstables, to_sstables);
    }

    fn change_levels(&self,
                     from_level_id: usize,
                     to_level_id: usize,
                     keys_to_remove_from: HashSet<usize>,
                     keys_to_remove_to: HashSet<usize>,
                     new_sstables: Vec<Arc<SSTable>>,
    ) -> Result<()> {
        if to_level_id <= 0 || to_level_id <= from_level_id {
            panic!("wrong ids are provided")
        }

        // produce new levels
        // from level only delete

        let levels = self.controller.levels.read().unwrap();
        let mut from_tables_complete = (&levels[from_level_id].run).clone();
        from_tables_complete.retain(|x| {
            !keys_to_remove_from.contains(&x.id)
        });
        let mut new_level_from = Level {
            run: from_tables_complete,
            id: from_level_id,
            size_on_disk: 0,
        };
        new_level_from.calc_size_on_disk();

        // to level delete, add new, sort
        let mut to_tables_complete = (&levels[to_level_id].run).clone();

        drop(levels);

        to_tables_complete.retain(|x| {
            !keys_to_remove_to.contains(&x.id)
        });

        for new_table in new_sstables {
            to_tables_complete.push_back(new_table);
        }

        to_tables_complete.make_contiguous().sort_unstable_by(|x, y| {
            // since keys are non-overlapping, order doesn't matter
            self.controller.db_opts.key_comparator.compare(&x.first_key, &y.first_key)
        });
        let mut new_level_to = Level {
            run: to_tables_complete,
            id: to_level_id,
            size_on_disk: 0,
        };
        new_level_to.calc_size_on_disk();

        // write new levels
        let mut levels = self.controller.levels.write().unwrap();
        levels[from_level_id] = new_level_from;
        levels[to_level_id] = new_level_to;

        Ok(())
    }

    fn compute_targets(&self) -> Targets {
        let x = &self.controller.levels.read().unwrap()[1..];
        let current_sizes: Vec<u64> = x
            .iter().map(|x| {
            x.size_on_disk
        }).collect();

        Targets::compute(current_sizes, self.level_opts.base_level_size, self.level_opts.level_size_multiplier)
    }

    fn compute_priorities(&self, targets: &Targets, l0_run_len: usize) -> Vec<LevelPriority> {
        let mut priorities = Vec::with_capacity(targets.current_sizes.len() + 1);

        if l0_run_len >= self.level_opts.level0_file_num_compaction_trigger as usize {
            priorities.push(LevelPriority {
                score: f64::MAX,
                level_id: 0,
            });
        };

        // don't include last level.
        for i in 0..targets.target_sizes.len() - 1 {
            if targets.target_sizes[i] != 0 {
                let score = targets.current_sizes[i] as f64 / targets.target_sizes[i] as f64;

                if score > 1f64 {
                    priorities.push(LevelPriority {
                        score,
                        level_id: i + 1,
                    });
                }
            }
        }

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

        while self.do_compact()? {}

        return Ok(());
    }

    fn get_controller(&self) -> &LevelsController {
        &self.controller
    }
}

/*
 - change_levels. should delete from and to tables and add one's on the to_level_id with new
 - compute_priorities. should compute right priorities depending on options. Consider 0 and x levels.
 - select_tables_for_merge. correct tables for merge. Consider 0 and x levels.
 
 - Overall compaction. Consider many different cases.
 */
#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::mem::ManuallyDrop;
    use std::sync::{Arc, atomic};

    use bytes::Bytes;
    use memmap2::Mmap;
    use tempfile::{tempdir, tempfile};

    use proto::meta::TableIndex;

    use crate::compact::LeveledOpts;
    use crate::compact::levels_compactor::LevelsCompactor;
    use crate::compact::targets::Targets;
    use crate::comparator::BytesI32Comparator;
    use crate::core::tests::{bytes_to_int, int_to_bytes};
    use crate::db_options::DbOptions;
    use crate::sstable::id_generator::SSTableIdGenerator;
    use crate::sstable::SSTable;

    #[test]
    fn empty() {
        let level_opts = LeveledOpts::default();
        let db_opts = Arc::new(
            DbOptions::default()
        );
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let compactor = LevelsCompactor::new_empty(id_generator, level_opts, db_opts);

        let any_key = Bytes::from("key1");
        assert_eq!(compactor.controller.get(&any_key), None);

        let mut iter = compactor.controller.iter();
        assert_eq!(iter.next(), None)
    }

    fn empty_sstable(opts: Arc<DbOptions>, id: usize, first_key: i32, last_key: i32) -> SSTable {
        unsafe {
            let file = tempfile().unwrap();
            let mmap = ManuallyDrop::new(Mmap::map(&file).unwrap());

            let mut table = SSTable {
                index: TableIndex::default(),
                mmap,
                file_path: SSTable::create_path(id),
                opts,
                first_key: Bytes::new(),
                last_key: Bytes::new(),
                size_on_disk: 0,
                delete_on_drop: atomic::AtomicBool::new(false),
                id,
            };

            table.first_key = int_to_bytes(first_key);
            table.last_key = int_to_bytes(last_key);

            table
        }
    }

    #[test]
    fn priority() {
        let level_opts = LeveledOpts {
            num_levels: 5,
            level0_file_num_compaction_trigger: 1,
            ..Default::default()
        };
        let db_opts: Arc<DbOptions> = Arc::new(
            Default::default()
        );
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let compactor = LevelsCompactor::new_empty(id_generator.clone(), level_opts, db_opts.clone());
    
        // some level has priority
        let targets = Targets {
            base_level: 1,
            current_sizes: vec![10, 100, 0, 10000, 10000],
            target_sizes: vec![10, 0, 1000, 1000, 10000],
        };

        let priorities = compactor.compute_priorities(&targets, 0);
        assert!(!priorities.is_empty());
        assert_eq!(priorities.last().unwrap().level_id, 4);
        assert_eq!(priorities.last().unwrap().score, 10f64);

        // 0 level priority
        let priorities = compactor.compute_priorities(&targets, 1);
        assert!(!priorities.is_empty());
        assert_eq!(priorities.last().unwrap().level_id, 0);
        assert_eq!(priorities.last().unwrap().score, f64::MAX);
    }

    fn tables_to_ranges(tables: &[Arc<SSTable>]) -> Vec<(i32, i32)> {
        tables.iter().map(|x| {
            (bytes_to_int(&x.first_key), bytes_to_int(&x.last_key))
        }).collect()
    }

    #[test]
    fn select_tables_for_merge_lx() {
        /*
           l0: 1-3, 4-7
                ^
           lx: 1-2,3-4,5-6,7-8,9-10,11-12,13-14

           res = [1-3, 4-7], [1-2, 3-4, 5-6, 7-8]
       */
        /*
         lx: 1-3, 4-7, 8-12
                  ^
         lx: 1-2,3-4,5-6,7-8,9-10,11-12,13-14

         res = [4-7], [3-4,5-6,7-8]
         */
        let level_opts = LeveledOpts {
            num_levels: 3,
            ..Default::default()
        };
        let db_opts: Arc<DbOptions> = Arc::new(
            DbOptions {
                key_comparator: Arc::new(BytesI32Comparator {}),
                ..Default::default()
            }
        );
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let compactor = LevelsCompactor::new_empty(id_generator.clone(), level_opts, db_opts.clone());

        {
            let mut levels = compactor.controller.levels.write().unwrap();
            levels[1].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 4, 7)));
            levels[1].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 3)));
            levels[1].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 8, 12)));

            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 13, 14)));
            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 11, 12)));
            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 9, 10)));
            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 7, 8)));
            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 5, 6)));
            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 3, 4)));
            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 2)));
        }

        let (from_tables, to_tables) = compactor.select_tables_for_merge(1, 2);
        assert_eq!(tables_to_ranges(&from_tables), vec![(4, 7)]);
        assert_eq!(tables_to_ranges(&to_tables), vec![(3, 4), (5, 6), (7, 8)]);
    }


    #[test]
    fn select_tables_for_merge_l0() {
        /*
           l0: 1-3, 4-7
                ^
           lx: 1-2,3-4,5-6,7-8,9-10,11-12,13-14

           res = [1-3, 4-7], [1-2, 3-4, 5-6, 7-8]
       */
        let level_opts = LeveledOpts {
            num_levels: 2,
            ..Default::default()
        };
        let db_opts: Arc<DbOptions> = Arc::new(
            DbOptions {
                key_comparator: Arc::new(BytesI32Comparator {}),
                ..Default::default()
            }
        );
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let compactor = LevelsCompactor::new_empty(id_generator.clone(), level_opts, db_opts.clone());

        {
            let mut levels = compactor.controller.levels.write().unwrap();
            levels[0].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 3)));
            levels[0].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 4, 7)));

            levels[1].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 13, 14)));
            levels[1].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 11, 12)));
            levels[1].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 9, 10)));
            levels[1].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 7, 8)));
            levels[1].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 5, 6)));
            levels[1].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 3, 4)));
            levels[1].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 2)));
        }

        let (from_tables, to_tables) = compactor.select_tables_for_merge(0, 1);
        assert_eq!(tables_to_ranges(&from_tables), vec![(4, 7), (1, 3)]);
        assert_eq!(tables_to_ranges(&to_tables), vec![(1, 2), (3, 4), (5, 6), (7, 8)]);
    }

    #[test]
    fn change_levels() {
        /*
        l0: 1-3, 4-7
        l1: 1-2, 3-4, 5-6,7-8,9-10,11-12,13-14
        
        delete l0: 1
        delete l1: [8,7,6]
        add tables: [3-8]
         */


        let level_opts = LeveledOpts {
            num_levels: 3,
            ..Default::default()
        };
        let db_opts: Arc<DbOptions> = Arc::new(
            DbOptions {
                key_comparator: Arc::new(BytesI32Comparator {}),
                ..Default::default()
            }
        );
        let id_generator = Arc::new(SSTableIdGenerator::new(0));
        let compactor = LevelsCompactor::new_empty(id_generator.clone(), level_opts, db_opts.clone());

        {
            let mut levels = compactor.controller.levels.write().unwrap();
            levels[1].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 4, 7))); // 1
            levels[1].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 3))); // 2

            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 13, 14))); // 3
            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 11, 12))); // 4
            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 9, 10))); // 5
            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 7, 8))); // 6
            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 5, 6))); // 7
            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 3, 4))); // 8
            levels[2].add(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 2))); // 9
        }

        let new_tables = vec![Arc::new(empty_sstable(db_opts.clone(),
                                                     id_generator.get_new(), 3, 8))];

        compactor.change_levels(1, 2, HashSet::from([1]),
                                HashSet::from([6, 7, 8]),
                                new_tables).unwrap();

        {
            let mut levels = compactor.controller.levels.write().unwrap();
            assert_eq!(levels[1].run.len(), 1);
            assert_eq!(tables_to_ranges(levels[1].run.make_contiguous()), vec![(1,3)]);
            
            assert_eq!(levels[2].run.len(), 5);
            assert_eq!(tables_to_ranges(levels[2].run.make_contiguous()), vec![(1,2), (3, 8), (9, 10), (11,12), (13,14)]);
        }
    }
}
