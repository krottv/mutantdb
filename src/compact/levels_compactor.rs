use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, RwLock};

use tokio::runtime::Runtime;
use proto::meta::ManifestChange;

use crate::compact::{Compactor, LeveledOpts};
use crate::compact::level::Level;
use crate::compact::levels_controller::LevelsController;
use crate::compact::targets::Targets;
use crate::db_options::DbOptions;
use crate::entry::{Entry, EntryComparator};
use crate::errors::Result;
use crate::iterators::merge_iterator::MergeIterator;
use crate::manifest::Manifest;
use crate::sstable::id_generator::SSTableIdGenerator;
use crate::sstable::SSTable;

//todo: we need to compare keys also based on timestamp, 
// because it is not guaranteed the oldest keys are from l1. They might be put to l5 first.

struct LevelPriority {
    score: f64,
    from_level_id: usize,
    to_level_id: usize,
}

impl LevelPriority {
    pub fn new(score: f64, from_level_id: usize, base_level: usize) -> LevelPriority {
        let to_level_id = if from_level_id == 0 {
            // +1 because base_level is calculated from an array starting from 1 level.
            base_level + 1
        } else {
            from_level_id + 1
        };

        let priority = LevelPriority {
            score,
            from_level_id,
            to_level_id,
        };

        priority.validate();

        priority
    }

    fn validate(&self) {
        if self.to_level_id <= 0 || self.to_level_id <= self.from_level_id {
            panic!("wrong ids are provided")
        }
    }
}

pub struct LevelsCompactor {
    level_opts: Arc<LeveledOpts>,
    controller: LevelsController,
    runtime: Runtime,
}

pub struct CompactDef {
    priority: LevelPriority,
    from_tables: Vec<Arc<SSTable>>,
    to_tables: Vec<Arc<SSTable>>,
}

impl CompactDef {
    fn is_async(&self) -> bool {
        !self.to_tables.is_empty()
    }
}

pub struct CompactRes {
    changes: Vec<ManifestChange>,
}

impl LevelsCompactor {
    pub fn open(id_generator: Arc<SSTableIdGenerator>, level_opts: LeveledOpts, db_opts: Arc<DbOptions>) -> Result<Self> {
        let controller = LevelsController::open(
            db_opts.clone(),
            id_generator.clone(),
            level_opts.num_levels,
        )?;


        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .max_blocking_threads(level_opts.num_parallel_compact)
            .build()
            .unwrap();

        Ok(
            LevelsCompactor {
                level_opts: Arc::new(level_opts),
                controller,
                runtime,
            }
        )
    }

    pub fn do_compact(&self) -> Result<bool> {
        let targets = self.compute_targets();
        let l0_run_len = self.controller.levels.read().unwrap()[0].run.len();

        let priorities = Self::compute_priorities(&self.level_opts, &targets, l0_run_len);
        let is_empty = priorities.is_empty();

        if is_empty {
            return Ok(false);
        }

        let mut async_compacts = Vec::new();
        let mut total_changes = Vec::new();

        for priority in priorities {
            let (from_tables, to_tables) = self.select_tables_for_merge(&priority);
            let compact_def = CompactDef {
                priority,
                from_tables,
                to_tables,
            };

            if compact_def.is_async() {
                async_compacts.push(compact_def)
            } else {
                for change in self.subcompact_move(compact_def).changes {
                    total_changes.push(change)
                }
            }
        }

        for res in self.subcompact_async(async_compacts)?.into_iter() {
            for change in res.changes {
                total_changes.push(change)
            }
        }

        self.controller.manifest_writer.lock()
            .unwrap().write(total_changes)?;

        self.controller.sync_dir()?;

        Ok(true)
    }

    fn subcompact_move(&self, compact_def: CompactDef) -> CompactRes {
        let new_tables: Vec<Arc<SSTable>> = compact_def.from_tables.clone();

        let changes = Self::change_levels_inner(self.controller.levels.clone(),
                                                self.controller.db_opts.clone(), &compact_def, new_tables);

        CompactRes {
            changes,
        }
    }

    fn subcompact_async(&self, compacts: Vec<CompactDef>) -> Result<Vec<CompactRes>> {
        let mut futures = Vec::new();

        for x in compacts.into_iter() {
            futures.push(Self::subcompact_async_single(self.controller.levels.clone(),
                                                       self.controller.db_opts.clone(), 
                                                       self.controller.id_generator.clone(), x))
        }

        let results = self.runtime.block_on(futures::future::join_all(futures.into_iter()));

        let mut compacts = Vec::new();

        for res in results.into_iter() {
            compacts.push(res?);
        }

        Ok(
            compacts
        )
    }


    async fn subcompact_async_single(levels_lock: Arc<RwLock<Vec<Level>>>,
                                     db_opts: Arc<DbOptions>, id_generator: Arc<SSTableIdGenerator>,
                                     compact_def: CompactDef) -> Result<CompactRes> {
        let total_iter = Self::create_iterator_for_tables(db_opts.clone(),
                                                          &compact_def);
        let new_tables = LevelsController::create_sstables(db_opts.clone(), id_generator.clone(), total_iter)?;

        let changes = Self::change_levels_inner(levels_lock, db_opts, &compact_def, new_tables);

        for sstable in &compact_def.from_tables {
            sstable.mark_delete()
        }

        for sstable in &compact_def.to_tables {
            sstable.mark_delete()
        }

        Ok(
            CompactRes {
                changes,
            }
        )
    }


    fn create_iterator_for_tables(db_opts: Arc<DbOptions>,
                                  compact_def: &CompactDef) -> MergeIterator<Entry> {
        let entry_comparator = Arc::new(EntryComparator::new(db_opts.key_comparator.clone()));
        let from_iterator = Level::create_iterator_for_tables(entry_comparator.clone(), compact_def.priority.from_level_id, &compact_def.from_tables);
        let to_iterator = Level::create_iterator_for_tables(entry_comparator.clone(), compact_def.priority.to_level_id, &compact_def.to_tables);
        let iterators: Vec<Box<dyn Iterator<Item=Entry>>> = vec![from_iterator, to_iterator];
        MergeIterator::new(iterators, entry_comparator)
    }

    fn select_tables_for_merge(&self, priority: &LevelPriority) -> (Vec<Arc<SSTable>>, Vec<Arc<SSTable>>) {
        let levels = self.controller.levels.read().unwrap();
        let from_level = &levels[priority.from_level_id];
        let to_level = &levels[priority.to_level_id];

        let from_sstables: Vec<Arc<SSTable>>;

        if priority.from_level_id == 0 {
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

    fn change_levels_inner(levels_lock: Arc<RwLock<Vec<Level>>>,
                           db_opts: Arc<DbOptions>,
                           compact_def: &CompactDef,
                           new_sstables: Vec<Arc<SSTable>>,
    ) -> Vec<ManifestChange> {
        let keys_to_remove_to: HashSet<usize> = compact_def.to_tables.iter().map(|x| {
            x.id
        }).collect();

        let keys_to_remove_from: HashSet<usize> = compact_def.from_tables.iter().map(|x| {
            x.id
        }).collect();

        Self::change_levels(levels_lock, db_opts, &compact_def.priority,
                            keys_to_remove_from, keys_to_remove_to, new_sstables)
    }

    fn change_levels(levels_lock: Arc<RwLock<Vec<Level>>>,
                     db_opts: Arc<DbOptions>,
                     priority: &LevelPriority,
                     keys_to_remove_from: HashSet<usize>,
                     keys_to_remove_to: HashSet<usize>,
                     new_sstables: Vec<Arc<SSTable>>,
    ) -> Vec<ManifestChange> {
        // produce new levels
        // from level only delete

        let mut changes = Vec::with_capacity(keys_to_remove_to.len() + keys_to_remove_from.len() +
            new_sstables.len());

        for table_id in &keys_to_remove_from {
            changes.push(Manifest::new_change_delete(*table_id as u64));
        }
        for table_id in &keys_to_remove_to {
            changes.push(Manifest::new_change_delete(*table_id as u64));
        }

        let levels = levels_lock.read().unwrap();

        let from_tables_complete: VecDeque<Arc<SSTable>> = (&levels[priority.from_level_id].run)
            .iter().filter(|x| {
            !keys_to_remove_from.contains(&x.id)
        }).cloned().collect();

        let mut new_level_from = Level {
            run: from_tables_complete,
            id: priority.from_level_id,
            size_on_disk: 0,
        };
        new_level_from.calc_size_on_disk();
        new_level_from.validate(db_opts.key_comparator.as_ref());

        // to level delete, add new, sort
        let mut to_tables_complete: VecDeque<Arc<SSTable>> = (&levels[priority.to_level_id].run).iter()
            .filter(|x| {
                !keys_to_remove_to.contains(&x.id)
            }).cloned().collect();

        drop(levels);

        for new_table in new_sstables {
            changes.push(Manifest::new_change_add(new_table.id as u64, priority.to_level_id));
            to_tables_complete.push_back(new_table);
        }

        let mut new_level_to = Level {
            run: to_tables_complete,
            id: priority.to_level_id,
            size_on_disk: 0,
        };
        new_level_to.calc_size_on_disk();
        new_level_to.sort_tables(db_opts.key_comparator.as_ref());
        new_level_to.validate(db_opts.key_comparator.as_ref());

        // write new levels
        let mut levels = levels_lock.write().unwrap();
        levels[priority.from_level_id] = new_level_from;
        levels[priority.to_level_id] = new_level_to;

        changes
    }

    fn compute_targets(&self) -> Targets {
        let x = &self.controller.levels.read().unwrap()[1..];
        let current_sizes: Vec<u64> = x
            .iter().map(|x| {
            x.size_on_disk
        }).collect();

        Targets::compute(current_sizes, self.level_opts.base_level_size, self.level_opts.level_size_multiplier)
    }

    // score is guaranteed to be > 1.
    fn compute_priorities(level_opts: &LeveledOpts, targets: &Targets, l0_run_len: usize) -> Vec<LevelPriority> {
        let mut priorities = Vec::with_capacity(targets.current_sizes.len() + 1);

        if l0_run_len >= level_opts.level0_file_num_compaction_trigger as usize {
            priorities.push(LevelPriority::new(f64::MAX, 0, targets.base_level));
        };

        // don't include last level.
        for i in 0..targets.target_sizes.len() - 1 {
            if targets.target_sizes[i] != 0 {
                let score = targets.current_sizes[i] as f64 / targets.target_sizes[i] as f64;

                if score > 1f64 {
                    priorities.push(LevelPriority::new(score, i + 1, targets.base_level));
                }
            }
        }

        priorities.sort_unstable_by(|a, b| {
            b.score.total_cmp(&a.score)
        });

        Self::filter_conflicting_priorities(&mut priorities);

        priorities
    }

    fn filter_conflicting_priorities(priorities: &mut Vec<LevelPriority>) {
        let mut visited_levels: HashSet<usize> = HashSet::new();

        priorities.retain(|x| {
            if visited_levels.contains(&x.to_level_id) || visited_levels.contains(&x.from_level_id) {
                false
            } else {
                visited_levels.insert(x.from_level_id);
                visited_levels.insert(x.to_level_id);
                true
            }
        });
    }
}


impl Compactor for LevelsCompactor {
    fn add_to_l0(&self, sstable: SSTable) -> Result<()> {
        let sstable_arc = Arc::new(sstable);
        {
            let mut levels = self.controller.levels.write().unwrap();
            let level = levels.get_mut(0).unwrap();
            level.add_front(sstable_arc.clone());
            level.validate(sstable_arc.opts.key_comparator.as_ref());
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
    use log::LevelFilter;
    use memmap2::Mmap;
    use simplelog::{ColorChoice, Config, TerminalMode, TermLogger};
    use tempfile::{tempdir, tempfile};

    use proto::meta::TableIndex;

    use crate::compact::{CompactionOptions, Compactor, LeveledOpts};
    use crate::compact::levels_compactor::{LevelPriority, LevelsCompactor};
    use crate::compact::targets::Targets;
    use crate::comparator::BytesI32Comparator;
    use crate::core::tests::{bytes_to_int, int_to_bytes};
    use crate::db_options::DbOptions;
    use crate::sstable::id_generator::SSTableIdGenerator;
    use crate::sstable::SSTable;
    use crate::sstable::tests::create_sstable;

    #[test]
    fn empty() {
        let level_opts = LeveledOpts::default();
        let tmp_dir = tempdir().unwrap();
        let db_opts = Arc::new(
            DbOptions {
                path: tmp_dir.path().to_path_buf(),
                ..Default::default()
            }
        );
        db_opts.create_dirs().unwrap();
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let compactor = LevelsCompactor::open(id_generator, level_opts, db_opts).unwrap();

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
    fn priority_edge_cases() {
        let level_opts = LeveledOpts {
            num_levels: 5,
            level0_file_num_compaction_trigger: 1,
            ..Default::default()
        };

        // some level has priority
        let targets = Targets {
            base_level: 1,
            current_sizes: vec![10, 100, 0, 10000, 10000],
            target_sizes: vec![10, 0, 1000, 1000, 10000],
        };

        let priorities = LevelsCompactor::compute_priorities(&level_opts, &targets, 0);
        assert!(!priorities.is_empty());
        assert_eq!(priorities.first().unwrap().from_level_id, 4);
        assert_eq!(priorities.first().unwrap().score, 10f64);

        // 0 level priority
        let priorities = LevelsCompactor::compute_priorities(&level_opts, &targets, 1);
        assert!(!priorities.is_empty());
        assert_eq!(priorities.first().unwrap().from_level_id, 0);
        assert_eq!(priorities.first().unwrap().score, f64::MAX);
    }

    #[test]
    fn priority_non_conflicting() {
        let level_opts = LeveledOpts {
            num_levels: 5,
            level0_file_num_compaction_trigger: 1,
            ..Default::default()
        };

        // some level has priority
        let targets = Targets {
            base_level: 1,
            current_sizes: vec![10, 100, 0, 10000, 10000],
            target_sizes: vec![1, 1, 1, 1, 1],
        };

        let priorities = LevelsCompactor::compute_priorities(&level_opts, &targets, 0);
        assert_eq!(priorities.len(), 2);
        assert_eq!(priorities[0].from_level_id, 4);
        assert_eq!(priorities[0].to_level_id, 5);

        assert_eq!(priorities[1].from_level_id, 2);
        assert_eq!(priorities[1].to_level_id, 3);
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
        let tmp_dir = tempdir().unwrap();
        let db_opts = Arc::new(
            DbOptions {
                path: tmp_dir.path().to_path_buf(),
                key_comparator: Arc::new(BytesI32Comparator {}),
                ..Default::default()
            }
        );
        db_opts.create_dirs().unwrap();

        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let compactor = LevelsCompactor::open(id_generator.clone(), level_opts, db_opts.clone()).unwrap();

        {
            let mut levels = compactor.controller.levels.write().unwrap();
            levels[1].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 4, 7)));
            levels[1].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 3)));
            levels[1].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 8, 12)));

            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 13, 14)));
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 11, 12)));
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 9, 10)));
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 7, 8)));
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 5, 6)));
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 3, 4)));
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 2)));
        }

        let (from_tables, to_tables) = compactor.select_tables_for_merge(&LevelPriority {
            score: 0f64,
            from_level_id: 1,
            to_level_id: 2,
        });
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
        let tmp_dir = tempdir().unwrap();
        let db_opts = Arc::new(
            DbOptions {
                path: tmp_dir.path().to_path_buf(),
                key_comparator: Arc::new(BytesI32Comparator {}),
                ..Default::default()
            }
        );
        db_opts.create_dirs().unwrap();


        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let compactor = LevelsCompactor::open(id_generator.clone(), level_opts, db_opts.clone()).unwrap();

        {
            let mut levels = compactor.controller.levels.write().unwrap();
            levels[0].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 3)));
            levels[0].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 4, 7)));

            levels[1].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 13, 14)));
            levels[1].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 11, 12)));
            levels[1].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 9, 10)));
            levels[1].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 7, 8)));
            levels[1].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 5, 6)));
            levels[1].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 3, 4)));
            levels[1].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 2)));
        }

        let (from_tables, to_tables) = compactor.select_tables_for_merge(&LevelPriority {
            score: 0f64,
            from_level_id: 0,
            to_level_id: 1,
        });
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
        let tmp_dir = tempdir().unwrap();
        let db_opts = Arc::new(
            DbOptions {
                path: tmp_dir.path().to_path_buf(),
                key_comparator: Arc::new(BytesI32Comparator {}),
                ..Default::default()
            }
        );
        db_opts.create_dirs().unwrap();


        let id_generator = Arc::new(SSTableIdGenerator::new(0));
        let compactor = LevelsCompactor::open(id_generator.clone(), level_opts, db_opts.clone()).unwrap();

        {
            let mut levels = compactor.controller.levels.write().unwrap();
            levels[1].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 4, 7))); // 1
            levels[1].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 3))); // 2

            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 13, 14))); // 3
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 11, 12))); // 4
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 9, 10))); // 5
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 7, 8))); // 6
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 5, 6))); // 7
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 3, 4))); // 8
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 2))); // 9
        }

        let new_tables = vec![Arc::new(empty_sstable(db_opts.clone(),
                                                     id_generator.get_new(), 3, 8))];

        LevelsCompactor::change_levels(compactor.controller.levels.clone(), db_opts.clone(), &LevelPriority {
            score: 0f64,
            from_level_id: 1,
            to_level_id: 2,
        }, HashSet::from([1]),
                                       HashSet::from([6, 7, 8]),
                                       new_tables);

        {
            let mut levels = compactor.controller.levels.write().unwrap();
            assert_eq!(levels[1].run.len(), 1);
            assert_eq!(tables_to_ranges(levels[1].run.make_contiguous()), vec![(1, 3)]);

            assert_eq!(levels[2].run.len(), 5);
            assert_eq!(tables_to_ranges(levels[2].run.make_contiguous()), vec![(1, 2), (3, 8), (9, 10), (11, 12), (13, 14)]);
        }
    }

    pub fn init_log() {
        let _log = TermLogger::init(LevelFilter::Info, Config::default(),
                                    TerminalMode::Mixed, ColorChoice::Auto).unwrap();
    }

    #[test]
    fn add_overlapping_last_level() {
        let tmp_dir = tempdir().unwrap();
        let comparator = Arc::new(BytesI32Comparator {});
        let db_opts = Arc::new(
            DbOptions {
                key_comparator: comparator,
                path: tmp_dir.path().to_path_buf(),
                ..Default::default()
            }
        );
        db_opts.create_dirs().unwrap();

        let level_opts = LeveledOpts {
            base_level_size: 1,
            num_levels: 5,
            level_size_multiplier: 2,
            level0_file_num_compaction_trigger: 1,
            ..Default::default()
        };
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let compactor = LevelsCompactor::open(id_generator, level_opts, db_opts.clone()).unwrap();

        let e1 = crate::compact::simple_levels_compactor::tests::new_entry(1, 1);
        let e2 = crate::compact::simple_levels_compactor::tests::new_entry(2, 2);
        let e3 = crate::compact::simple_levels_compactor::tests::new_entry(3, 3);

        let sstable1 = create_sstable(db_opts.clone(), vec![e1.clone(), e2.clone(), e3.clone()],
                                      compactor.controller.id_generator.get_new());

        let e4 = crate::compact::simple_levels_compactor::tests::new_entry(4, 4);
        let e5 = crate::compact::simple_levels_compactor::tests::new_entry(2, 20);
        let e6 = crate::compact::simple_levels_compactor::tests::new_entry(5, 5);

        let sstable2 = create_sstable(db_opts.clone(), vec![e4.clone(), e5.clone(), e6.clone()],
                                      compactor.controller.id_generator.get_new());

        println!("{}", compactor.compute_targets());

        compactor.add_to_l0(sstable1).unwrap();
        compactor.controller.log_levels();
        println!("{}", compactor.compute_targets());

        compactor.add_to_l0(sstable2).unwrap();
        compactor.controller.log_levels();
        println!("{}", compactor.compute_targets());

        // should be moved and merged to last level
        let levels = compactor.controller.levels.read().unwrap();
        assert_eq!(levels.len(), 5);
        assert_eq!(levels[0].size_on_disk, 0);
        assert_eq!(levels[1].size_on_disk, 0);
        assert_eq!(levels[2].size_on_disk, 0);
        assert_eq!(levels[3].size_on_disk, 0);
        assert_ne!(levels[4].size_on_disk, 0);

        assert_eq!(levels[0].run.len(), 0);
        assert_eq!(levels[1].run.len(), 0);
        assert_eq!(levels[2].run.len(), 0);
        assert_eq!(levels[3].run.len(), 0);
        // 2 tables are overlapping and thus merged
        assert_eq!(levels[4].run.len(), 1);
        drop(levels);

        let mut iter = compactor.controller.get_iterator();
        assert_eq!(iter.next(), Some(e1.clone()));
        assert_eq!(iter.next(), Some(e5.clone()));
        assert_eq!(iter.next(), Some(e3.clone()));
        assert_eq!(iter.next(), Some(e4.clone()));
        assert_eq!(iter.next(), Some(e6.clone()));
        assert_eq!(iter.next(), None);
        drop(iter);

        assert_eq!(compactor.controller.get(&e1.key), Some(e1.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e3.key), Some(e3.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e4.key), Some(e4.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e5.key), Some(e5.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e6.key), Some(e6.val_obj.clone()));
    }

    #[test]
    fn add_non_overlapping_last_level() {
        init_log();

        let tmp_dir = tempdir().unwrap();
        let comparator = Arc::new(BytesI32Comparator {});
        let db_opts = Arc::new(
            DbOptions {
                key_comparator: comparator,
                path: tmp_dir.path().to_path_buf(),
                ..Default::default()
            }
        );
        db_opts.create_dirs().unwrap();

        let level_opts = LeveledOpts {
            base_level_size: 1,
            num_levels: 5,
            level_size_multiplier: 2,
            level0_file_num_compaction_trigger: 1,
            ..Default::default()
        };
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let mut compactor = LevelsCompactor::open(id_generator.clone(), level_opts.clone(), db_opts.clone()).unwrap();

        let e1 = crate::compact::simple_levels_compactor::tests::new_entry(1, 1);
        let e2 = crate::compact::simple_levels_compactor::tests::new_entry(2, 2);
        let e3 = crate::compact::simple_levels_compactor::tests::new_entry(3, 3);

        let sstable1 = create_sstable(db_opts.clone(), vec![e1.clone(), e2.clone(), e3.clone()],
                                      compactor.controller.id_generator.get_new());

        let e4 = crate::compact::simple_levels_compactor::tests::new_entry(4, 4);
        let e5 = crate::compact::simple_levels_compactor::tests::new_entry(5, 5);
        let e6 = crate::compact::simple_levels_compactor::tests::new_entry(6, 6);

        let sstable2 = create_sstable(db_opts.clone(), vec![e4.clone(), e5.clone(), e6.clone()],
                                      compactor.controller.id_generator.get_new());

        println!("{}", compactor.compute_targets());

        compactor.add_to_l0(sstable1).unwrap();
        compactor.controller.log_levels();
        println!("{}", compactor.compute_targets());

        compactor.add_to_l0(sstable2).unwrap();
        compactor.controller.log_levels();
        println!("{}", compactor.compute_targets());

        // should be moved and merged to last level
        let levels = compactor.controller.levels.read().unwrap();
        assert_eq!(levels.len(), 5);

        assert_eq!(levels[0].run.len(), 0);
        assert_eq!(levels[1].run.len(), 0);
        assert_eq!(levels[2].run.len(), 0);
        assert_eq!(levels[3].run.len(), 0);
        // 2 tables are non-overlapping and thus not merged
        assert_eq!(levels[4].run.len(), 2);
        drop(levels);

        // add to 4 level
        let e7 = crate::compact::simple_levels_compactor::tests::new_entry(8, 8);
        let sstable3 = create_sstable(db_opts.clone(),
                                      vec![e7.clone()],
                                      compactor.controller.id_generator.get_new());

        compactor.add_to_l0(sstable3).unwrap();
        compactor.controller.log_levels();
        println!("{}", compactor.compute_targets());

        let levels = compactor.controller.levels.read().unwrap();
        assert_eq!(levels.len(), 5);

        assert_eq!(levels[0].run.len(), 0);
        assert_eq!(levels[1].run.len(), 0);
        assert_eq!(levels[2].run.len(), 0);
        assert_eq!(levels[3].run.len(), 1);
        // 2 tables are non-overlapping and thus not merged
        assert_eq!(levels[4].run.len(), 2);

        compactor.controller.log_levels();

        drop(levels);
        drop(compactor);

        // reopen.
        compactor = LevelsCompactor::open(id_generator, level_opts, db_opts.clone()).unwrap();

        assert_eq!(compactor.controller.get(&e1.key), Some(e1.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e3.key), Some(e3.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e4.key), Some(e4.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e5.key), Some(e5.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e6.key), Some(e6.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e7.key), Some(e7.val_obj.clone()));
    }

    #[test]
    fn do_compaction() {
        let level_opts = LeveledOpts {
            num_levels: 3,
            level0_file_num_compaction_trigger: 1,
            ..Default::default()
        };
        let tmp_dir = tempdir().unwrap();
        let db_opts = Arc::new(
            DbOptions {
                path: tmp_dir.path().to_path_buf(),
                key_comparator: Arc::new(BytesI32Comparator {}),
                compaction: Arc::new(CompactionOptions::Leveled(level_opts.clone())),
                ..Default::default()
            }
        );
        db_opts.create_dirs().unwrap();

        let id_generator = Arc::new(SSTableIdGenerator::new(0));
        let compactor = LevelsCompactor::open(id_generator.clone(), level_opts, db_opts.clone()).unwrap();

        {
            let mut levels = compactor.controller.levels.write().unwrap();
            levels[0].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 3))); // 1
            levels[0].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 4, 7))); // 2

            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 1, 2))); // 3
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 3, 4))); // 4
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 5, 6))); // 5
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 7, 8))); // 6
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 9, 10))); // 7
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 11, 12))); // 8
            levels[2].add_front(Arc::new(empty_sstable(db_opts.clone(), id_generator.get_new(), 13, 14))); // 9
        }

        let compacted = compactor.do_compact();
        assert_eq!(compacted.unwrap(), true);

        let mut levels = compactor.controller.levels.write().unwrap();
        assert_eq!(levels[0].run.len(), 0);
        assert_eq!(levels[1].run.len(), 0);

        // (1, 8) is absent because sstables are empty
        assert_eq!(tables_to_ranges(levels[2].run.make_contiguous()), vec![(9, 10), (11, 12), (13, 14)]);
    }
}
