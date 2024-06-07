use std::sync::{Arc, RwLock};

use crate::compact::{Compactor, SimpleLeveledOpts};
use crate::compact::level::Level;
use crate::compact::levels_controller::LevelsController;
use crate::entry::{Entry, EntryComparator};
use crate::errors::Result;
use crate::iterators::merge_iterator::MergeIterator;
use crate::db_options::{DbOptions};
use crate::sstable::id_generator::SSTableIdGenerator;
use crate::sstable::SSTable;

pub struct SimpleLevelsCompactor {
    level_opts: SimpleLeveledOpts,
    controller: LevelsController,
}

impl SimpleLevelsCompactor {
    pub fn new_empty(id_generator: Arc<SSTableIdGenerator>, level_opts: SimpleLeveledOpts, db_opts: Arc<DbOptions>) -> Self {
        let mut levels = Vec::with_capacity(level_opts.num_levels as usize);
        for i in 0..level_opts.num_levels {
            levels.push(Level::new_empty(i));
        }

        let controller = LevelsController {
            id_generator,
            db_opts,
            levels: RwLock::new(levels),
        };

        SimpleLevelsCompactor {
            level_opts,
            controller,
        }
    }

    fn force_compact(&self, level_id: u32) -> Result<()> {
        let levels = self.controller.levels.read().unwrap();
        // merge current level and the next level; which is guaranteed to be present.
        let entry_comparator = Arc::new(EntryComparator::new(self.controller.db_opts.key_comparator.clone()));

        // get total iterator
        let level1 = levels.get(level_id as usize).unwrap();
        let level2 = levels.get((level_id + 1) as usize).unwrap();
        let iter1 = level1.create_iterator_for_level(entry_comparator.clone());
        let iter2 = level2.create_iterator_for_level(entry_comparator.clone());
        let iterators: Vec<Box<dyn Iterator<Item=Entry>>> = vec![iter1, iter2];
        let total_iter = MergeIterator::new(iterators, entry_comparator);
        // new level
        let new_level = self.controller.create_level(level_id, total_iter)?;

        // delete old sstables
        for sstable in level1.run.iter() {
            sstable.mark_delete()
        }
        for sstable in level2.run.iter() {
            sstable.mark_delete()
        }

        // if not dropped, then it would deadlock.
        drop(levels);

        {
            let mut levels = self.controller.levels.write().unwrap();
            levels[level_id as usize] = Level::new_empty(level_id);
            levels[(level_id + 1) as usize] = new_level;
        }

        // check for the next level
        return self.check_compact(level_id + 1);
    }

    fn check_compact(&self, level_id: u32) -> Result<()> {
        let levels = self.controller.levels.read().unwrap();

        // skip if it's the last level
        if level_id as usize >= (levels.len() - 1) {
            return Ok(());
        }

        let level_max_size = self.level_opts.base_level_size * (self.level_opts.level_size_multiplier as u64)
            .pow(level_id);
        let cur_level = levels.get(level_id as usize).unwrap();

        if cur_level.size_on_disk < level_max_size {
            return Ok(());
        }
        drop(levels);

        self.force_compact(level_id)
    }
}

impl Compactor for SimpleLevelsCompactor {
    fn add_to_l0(&self, sstable: SSTable) -> Result<()> {
        let sstable_arc = Arc::new(sstable);
        {
            self.controller.levels.write().unwrap().get_mut(0)
                .unwrap().add(sstable_arc);
        }
        self.check_compact(0)?;

        return Ok(());
    }

    fn get_controller(&self) -> &LevelsController {
        &self.controller
    }
}

/*
Test cases
1. Add to l0 and size of level small - don't move 
2. Add to l0 and size is big - move further
3. Add to l0 and size is huge - move 2 compact further
4. All items are present if level 1 and level 2 are filled for example.
5. Empty


- Check get and iter works correctly for each case.
- Check that duplicated keys are deleted.
*/
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::compact::{Compactor, SimpleLeveledOpts};
    use crate::compact::simple_levels_compactor::SimpleLevelsCompactor;
    use crate::comparator::BytesI32Comparator;
    use crate::entry::{Entry, META_ADD, ValObj};
    use crate::db_options::{DbOptions};
    use crate::sstable::id_generator::SSTableIdGenerator;
    use crate::sstable::tests::create_sstable;

    fn new_entry(key: i32, value: i32) -> Entry {
        Entry {
            key: Bytes::from(key.to_be_bytes().to_vec()),
            val_obj: ValObj {
                value: Bytes::from(value.to_be_bytes().to_vec()),
                meta: META_ADD,
                user_meta: key as u8,
                version: value as u64,
            },
        }
    }

    #[test]
    fn empty() {
        let level_opts = SimpleLeveledOpts::default();
        let db_opts = Arc::new(
            DbOptions::default()
        );
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let compactor = SimpleLevelsCompactor::new_empty(id_generator, level_opts, db_opts);

        let any_key = Bytes::from("key1");
        assert_eq!(compactor.controller.get(&any_key), None);

        let mut iter = compactor.controller.iter();
        assert_eq!(iter.next(), None)
    }

    #[test]
    fn compact_every_time() {
        let tmp_dir = tempdir().unwrap();
        let comparator = Arc::new(BytesI32Comparator {});
        let db_opts = Arc::new(
            DbOptions {
                key_comparator: comparator,
                sstables_path: tmp_dir.path().to_path_buf(),
                ..Default::default()
            }
        );
        let level_opts = SimpleLeveledOpts {
            base_level_size: 1,
            num_levels: 3,
            level_size_multiplier: 1,
        };
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let compactor = SimpleLevelsCompactor::new_empty(id_generator, level_opts, db_opts.clone());

        let e1 = new_entry(1, 1);
        let e2 = new_entry(2, 2);
        let e3 = new_entry(3, 3);

        let sstable1 = create_sstable(&tmp_dir, db_opts.clone(), vec![e1.clone(), e2.clone(), e3.clone()],
                                      compactor.controller.id_generator.get_new());

        let e4 = new_entry(4, 4);
        let e5 = new_entry(2, 20);
        let e6 = new_entry(5, 5);

        let sstable2 = create_sstable(&tmp_dir, db_opts.clone(), vec![e4.clone(), e5.clone(), e6.clone()],
                                      compactor.controller.id_generator.get_new());

        compactor.add_to_l0(sstable1).unwrap();
        compactor.add_to_l0(sstable2).unwrap();

        // should be moved and merged to 3rd level
        let levels = compactor.controller.levels.read().unwrap();
        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0].size_on_disk, 0);
        assert_eq!(levels[1].size_on_disk, 0);
        assert_ne!(levels[2].size_on_disk, 0);

        assert_eq!(levels[0].run.len(), 0);
        assert_eq!(levels[1].run.len(), 0);
        assert_eq!(levels[2].run.len(), 1);
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
    fn no_compact() {
        let tmp_dir = tempdir().unwrap();
        let comparator = Arc::new(BytesI32Comparator {});
        let db_opts = Arc::new(
            DbOptions {
                key_comparator: comparator,
                sstables_path: tmp_dir.path().to_path_buf(),
                ..Default::default()
            }
        );
        let level_opts = SimpleLeveledOpts {
            base_level_size: 1000000,
            num_levels: 3,
            level_size_multiplier: 1,
        };
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let compactor = SimpleLevelsCompactor::new_empty(id_generator, level_opts,
                                                         db_opts.clone());

        let e1 = new_entry(1, 1);
        let e2 = new_entry(2, 2);
        let e3 = new_entry(3, 3);

        let sstable1 = create_sstable(&tmp_dir, db_opts.clone(), vec![e1.clone(), e2.clone(), e3.clone()],
                                      compactor.controller.id_generator.get_new());

        let e4 = new_entry(4, 4);
        let e5 = new_entry(2, 20);
        let e6 = new_entry(5, 5);

        let sstable2 = create_sstable(&tmp_dir, db_opts.clone(), vec![e4.clone(), e5.clone(), e6.clone()],
                                      compactor.controller.id_generator.get_new());

        compactor.add_to_l0(sstable1).unwrap();
        compactor.add_to_l0(sstable2).unwrap();

        // should be at level 0
        let levels = compactor.controller.levels.read().unwrap();
        assert_eq!(levels.len(), 3);
        assert_ne!(levels[0].size_on_disk, 0);
        assert_eq!(levels[1].size_on_disk, 0);
        assert_eq!(levels[2].size_on_disk, 0);

        assert_eq!(levels[0].run.len(), 2);
        assert_eq!(levels[1].run.len(), 0);
        assert_eq!(levels[2].run.len(), 0);
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
    fn first_and_second_present() {
        let tmp_dir = tempdir().unwrap();
        let comparator = Arc::new(BytesI32Comparator {});
        let db_opts = Arc::new(
            DbOptions {
                key_comparator: comparator,
                sstables_path: tmp_dir.path().to_path_buf(),
                ..Default::default()
            }
        );
        let id_generator = Arc::new(SSTableIdGenerator::new(1));

        let e1 = new_entry(1, 1);
        let e2 = new_entry(2, 2);
        let e3 = new_entry(3, 3);
        let vec_entries_1 = vec![e1.clone(), e2.clone(), e3.clone()];
        let sstable1 = create_sstable(&tmp_dir, db_opts.clone(), vec_entries_1, id_generator.get_new());

        let e4 = new_entry(4, 4);
        let e5 = new_entry(2, 20);
        let e6 = new_entry(5, 5);
        let vec_entries_2 = vec![e4.clone(), e5.clone(), e6.clone()];
        let sstable2 = create_sstable(&tmp_dir, db_opts.clone(), vec_entries_2, id_generator.get_new());

        let e7 = new_entry(1, 10);
        let e8 = new_entry(2, 30);
        let e9 = new_entry(5, 50);
        let vec_entries_3 = vec![e7.clone(), e8.clone(), e9.clone()];
        let sstable3 = create_sstable(&tmp_dir, db_opts.clone(), vec_entries_3, id_generator.get_new());
        let size_on_disk_3 = sstable3.size_on_disk;

        let level_opts = SimpleLeveledOpts {
            base_level_size: sstable1.size_on_disk + sstable2.size_on_disk,
            num_levels: 2,
            level_size_multiplier: 1,
        };
        let compactor = SimpleLevelsCompactor::new_empty(id_generator, 
                                                         level_opts, db_opts.clone());


        compactor.add_to_l0(sstable1).unwrap();
        compactor.add_to_l0(sstable2).unwrap();
        compactor.add_to_l0(sstable3).unwrap();

        // should be at level 0 and 1
        let levels = compactor.controller.levels.read().unwrap();
        assert_eq!(levels.len(), 2);
        assert_eq!(&levels[0].size_on_disk, &size_on_disk_3);
        assert_ne!(levels[1].size_on_disk, 0);

        assert_eq!(levels[0].run.len(), 1);
        assert_eq!(levels[1].run.len(), 1);
        drop(levels);

        let mut iter = compactor.controller.get_iterator();
        assert_eq!(iter.next(), Some(e7.clone()));
        assert_eq!(iter.next(), Some(e8.clone()));
        assert_eq!(iter.next(), Some(e3.clone()));
        assert_eq!(iter.next(), Some(e4.clone()));
        assert_eq!(iter.next(), Some(e9.clone()));
        assert_eq!(iter.next(), None);
        drop(iter);

        assert_eq!(compactor.controller.get(&e7.key), Some(e7.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e8.key), Some(e8.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e3.key), Some(e3.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e4.key), Some(e4.val_obj.clone()));
        assert_eq!(compactor.controller.get(&e9.key), Some(e9.val_obj.clone()));
    }
}