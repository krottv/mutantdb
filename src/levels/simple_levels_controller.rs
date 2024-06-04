use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};

use crate::builder::Builder;
use crate::entry::{Entry, EntryComparator, Key, ValObj};
use crate::errors::Result;
use crate::iterators::concat_iterator::ConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::sstable_iterator::SSTableIterator;
use crate::levels::LevelsController;
use crate::opts::{DbOptions, LevelsOptions};
use crate::sstable::id_generator::SSTableIdGenerator;
use crate::sstable::SSTable;

#[derive(Clone)]
struct Level {
    // sorted except first level. Size is at least 2 guaranteed
    run: VecDeque<Arc<SSTable>>,
    // id of the level
    id: u32,
    size_on_disk: u64,
}

impl Level {
    pub fn add(&mut self, sstable: Arc<SSTable>) {
        self.size_on_disk += sstable.size_on_disk;
        self.run.push_front(sstable)
    }

    pub fn new_empty(id: u32) -> Level {
        Level {
            run: VecDeque::new(),
            id,
            size_on_disk: 0,
        }
    }
}

pub struct SimpleLevelsController {
    id_generator: Arc<SSTableIdGenerator>,
    level_opts: Arc<LevelsOptions>,
    db_opts: Arc<DbOptions>,
    levels: RwLock<Vec<Level>>,
}

impl SimpleLevelsController {
    pub fn new_empty(id_generator: Arc<SSTableIdGenerator>, level_opts: Arc<LevelsOptions>, db_opts: Arc<DbOptions>) -> Self {
        let mut levels = Vec::with_capacity(level_opts.num_levels as usize);
        for i in 0..level_opts.num_levels {
            levels.push(Level::new_empty(i));
        }

        SimpleLevelsController {
            id_generator,
            level_opts,
            db_opts,
            levels: RwLock::new(levels),
        }
    }

    fn force_compact(&self, level_id: u32) -> Result<()> {
        let levels = self.levels.read().unwrap();
        // merge current level and the next level; which is guaranteed to be present.

        // get total iterator
        let level1 = levels.get(level_id as usize).unwrap();
        let level2 = levels.get((level_id + 1) as usize).unwrap();
        let iter1 = self.create_iterator_for_level(level1);
        let iter2 = self.create_iterator_for_level(level2);
        let iterators: Vec<Box<dyn Iterator<Item=Entry>>> = vec![iter1, iter2];
        let entry_comparator = EntryComparator::new(self.db_opts.key_comparator.clone());
        let total_iter = MergeIterator::new(iterators, Arc::new(entry_comparator));
        // new level
        let new_level = self.create_level(level_id, total_iter)?;

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
            let mut levels = self.levels.write().unwrap();
            levels[level_id as usize] = Level::new_empty(level_id);
            levels[(level_id + 1) as usize] = new_level;
        }

        // check for the next level
        return self.check_compact(level_id + 1);
    }

    fn check_compact(&self, level_id: u32) -> Result<()> {
        let levels = self.levels.read().unwrap();

        // skip if it's the last level
        if level_id as usize >= (levels.len() - 1) {
            return Ok(());
        }

        let level_max_size = self.level_opts.level_max_size * (self.level_opts.next_level_size_multiple as u64)
            .pow(level_id);
        let cur_level = levels.get(level_id as usize).unwrap();

        if cur_level.size_on_disk < level_max_size {
            return Ok(());
        }
        drop(levels);

        self.force_compact(level_id)
    }

    fn get_iterator(&self) -> MergeIterator<Entry> {
        let levels = self.levels.read().unwrap();

        let mut iterators: Vec<Box<dyn Iterator<Item=Entry>>> = Vec::new();

        for level in levels.iter() {
            let iter = self.create_iterator_for_level(level);
            iterators.push(Box::new(iter));
        }

        let entry_comparator = EntryComparator::new(self.db_opts.key_comparator.clone());
        MergeIterator::new(iterators, Arc::new(entry_comparator))
    }

    fn create_iterator_for_level(&self, level: &Level) -> Box<dyn Iterator<Item=Entry>> {
        if level.id == 0 {
            Box::new(self.create_iterator_l0(level))
        } else {
            Box::new(self.create_iterator_lx(level))
        }
    }

    fn create_iterator_l0(&self, level: &Level) -> impl Iterator<Item=Entry> {
        let mut iterators: Vec<Box<dyn Iterator<Item=Entry>>> = Vec::new();

        for sstable in &level.run {
            let iter = SSTableIterator::new(sstable.clone());
            iterators.push(Box::new(iter));
        }

        let entry_comparator = EntryComparator::new(self.db_opts.key_comparator.clone());
        return MergeIterator::new(iterators, Arc::new(entry_comparator));
    }

    fn create_iterator_lx(&self, level: &Level) -> impl Iterator<Item=Entry> {
        let iterators: Vec<SSTableIterator> = level.run.iter().map(|x| {
            SSTableIterator::new(x.clone())
        }).collect();

        return ConcatIterator::new(iterators);
    }

    fn get_val_l0(&self, key: &Key, level: &Level) -> Option<ValObj> {
        for sstable in level.run.iter() {
            if let Some(entry) = sstable.find_entry(key) {
                return Some(entry.val_obj);
            }
        }
        None
    }

    fn get_val_lx(&self, key: &Key, level: &Level) -> Option<ValObj> {
        let bsearch_res = level.run.binary_search_by(|x| {
            let cmp_first = self.db_opts.key_comparator.compare(key, &x.first_key);
            if cmp_first.is_lt() {
                return Ordering::Less;
            }

            let cmp_last = self.db_opts.key_comparator.compare(key, &x.last_key);
            if cmp_last.is_gt() {
                return Ordering::Greater;
            }

            return Ordering::Equal;
        });

        if let Ok(index_sstable) = bsearch_res {
            if let Some(entry) = level.run[index_sstable].find_entry(key) {
                return Some(entry.val_obj);
            }
        }

        None
    }

    fn get_val(&self, key: &Key, level: &Level) -> Option<ValObj> {
        if level.id == 0 {
            self.get_val_l0(key, level)
        } else {
            self.get_val_lx(key, level)
        }
    }

    fn new_builder(&self) -> Result<Builder> {
        let path = self.db_opts.sstables_path.join(SSTable::create_path(self.id_generator.get_new()));
        Builder::new(path, self.db_opts.clone(), self.db_opts.block_max_size as usize)
    }

    fn create_level(&self, id: u32, iterator: MergeIterator<Entry>) -> Result<Level> {
        let mut builder = self.new_builder()?;
        let mut builder_entries_size: u64 = 0;
        let mut level = Level::new_empty(id + 1);

        for entry in iterator {
            let entry_size = entry.get_encoded_size_entry();

            if (builder_entries_size + entry_size as u64) > self.db_opts.max_memtable_size {
                let table = builder.build()?;
                level.add(Arc::new(table));
                builder = self.new_builder()?;
                builder_entries_size = 0;
            }

            builder_entries_size += entry_size as u64;
            builder.add_entry(&entry.key, &entry.val_obj)?;
        }

        if !builder.is_empty() {
            let table = builder.build()?;
            level.add(Arc::new(table));
        }

        return Ok(level);
    }
}

impl LevelsController for SimpleLevelsController {
    fn add_to_l0(&self, sstable: SSTable) -> Result<()> {
        let sstable_arc = Arc::new(sstable);
        {
            self.levels.write().unwrap().get_mut(0)
                .unwrap().add(sstable_arc);
        }
        self.check_compact(0)?;

        return Ok(());
    }

    fn get(&self, key: &Key) -> Option<ValObj> {
        let levels = self.levels.read().unwrap();
        for level in levels.iter() {
            let entry = self.get_val(key, level);
            if entry.is_some() {
                return entry;
            }
        }
        None
    }

    fn iter(&self) -> Box<dyn Iterator<Item=Entry>> {
        let iter = self.get_iterator();
        Box::new(iter)
    }

    fn get_sstable_count(&self, level_id: u32) -> Option<usize> {
        let i = level_id as usize;
        let levels = self.levels.read().unwrap();
        levels.get(i).map(|x| {
            x.run.len()
        })
    }

    fn get_sstable_count_total(&self) -> usize {
        self.levels.read().unwrap()
            .iter().fold(0usize, |x, y| {
            x + y.run.len()
        })
    }
}

/*
Test cases
1. Add to l0 and size of level small - don't move 
2. Add to l0 and size is big - move further
3. Add to l0 and size is huge - move 2 levels further
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

    use crate::comparator::BytesI32Comparator;
    use crate::entry::{Entry, META_ADD, ValObj};
    use crate::levels::LevelsController;
    use crate::levels::simple_levels_controller::SimpleLevelsController;
    use crate::opts::{DbOptions, LevelsOptions};
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
        let level_opts = Arc::new(
            LevelsOptions::default()
        );
        let db_opts = Arc::new(
            DbOptions::default()
        );
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let controller = SimpleLevelsController::new_empty(id_generator, level_opts, db_opts);

        let any_key = Bytes::from("key1");
        assert_eq!(controller.get(&any_key), None);

        let mut iter = controller.iter();
        assert_eq!(iter.next(), None)
    }

    #[test]
    fn compact_every_time() {
        let tmp_dir = tempdir().unwrap();
        let comparator = Arc::new(BytesI32Comparator {});
        let db_opts = Arc::new(
            DbOptions {
                key_comparator: comparator,
                ..Default::default()
            }
        );
        let level_opts = Arc::new(
            LevelsOptions {
                level_max_size: 1,
                num_levels: 3,
                next_level_size_multiple: 1,
            }
        );
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let controller = SimpleLevelsController::new_empty(id_generator, level_opts, db_opts.clone());

        let e1 = new_entry(1, 1);
        let e2 = new_entry(2, 2);
        let e3 = new_entry(3, 3);

        let sstable1 = create_sstable(&tmp_dir, db_opts.clone(), vec![e1.clone(), e2.clone(), e3.clone()], controller.id_generator.get_new());

        let e4 = new_entry(4, 4);
        let e5 = new_entry(2, 20);
        let e6 = new_entry(5, 5);

        let sstable2 = create_sstable(&tmp_dir, db_opts.clone(), vec![e4.clone(), e5.clone(), e6.clone()], controller.id_generator.get_new());

        controller.add_to_l0(sstable1).unwrap();
        controller.add_to_l0(sstable2).unwrap();

        // should be moved and merged to 3rd level
        let levels = controller.levels.read().unwrap();
        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0].size_on_disk, 0);
        assert_eq!(levels[1].size_on_disk, 0);
        assert_ne!(levels[2].size_on_disk, 0);

        assert_eq!(levels[0].run.len(), 0);
        assert_eq!(levels[1].run.len(), 0);
        assert_eq!(levels[2].run.len(), 1);
        drop(levels);

        let mut iter = controller.get_iterator();
        assert_eq!(iter.next(), Some(e1.clone()));
        assert_eq!(iter.next(), Some(e5.clone()));
        assert_eq!(iter.next(), Some(e3.clone()));
        assert_eq!(iter.next(), Some(e4.clone()));
        assert_eq!(iter.next(), Some(e6.clone()));
        assert_eq!(iter.next(), None);
        drop(iter);

        assert_eq!(controller.get(&e1.key), Some(e1.val_obj.clone()));
        assert_eq!(controller.get(&e3.key), Some(e3.val_obj.clone()));
        assert_eq!(controller.get(&e4.key), Some(e4.val_obj.clone()));
        assert_eq!(controller.get(&e5.key), Some(e5.val_obj.clone()));
        assert_eq!(controller.get(&e6.key), Some(e6.val_obj.clone()));
    }

    #[test]
    fn no_compact() {
        let tmp_dir = tempdir().unwrap();
        let comparator = Arc::new(BytesI32Comparator {});
        let db_opts = Arc::new(
            DbOptions {
                key_comparator: comparator,
                ..Default::default()
            }
        );
        let level_opts = Arc::new(
            LevelsOptions {
                level_max_size: 1000000,
                num_levels: 3,
                next_level_size_multiple: 1,
            }
        );
        let id_generator = Arc::new(SSTableIdGenerator::new(1));
        let controller = SimpleLevelsController::new_empty(id_generator, level_opts, db_opts.clone());

        let e1 = new_entry(1, 1);
        let e2 = new_entry(2, 2);
        let e3 = new_entry(3, 3);

        let sstable1 = create_sstable(&tmp_dir, db_opts.clone(), vec![e1.clone(), e2.clone(), e3.clone()], controller.id_generator.get_new());

        let e4 = new_entry(4, 4);
        let e5 = new_entry(2, 20);
        let e6 = new_entry(5, 5);

        let sstable2 = create_sstable(&tmp_dir, db_opts.clone(), vec![e4.clone(), e5.clone(), e6.clone()], controller.id_generator.get_new());

        controller.add_to_l0(sstable1).unwrap();
        controller.add_to_l0(sstable2).unwrap();

        // should be at level 0
        let levels = controller.levels.read().unwrap();
        assert_eq!(levels.len(), 3);
        assert_ne!(levels[0].size_on_disk, 0);
        assert_eq!(levels[1].size_on_disk, 0);
        assert_eq!(levels[2].size_on_disk, 0);

        assert_eq!(levels[0].run.len(), 2);
        assert_eq!(levels[1].run.len(), 0);
        assert_eq!(levels[2].run.len(), 0);
        drop(levels);

        let mut iter = controller.get_iterator();
        assert_eq!(iter.next(), Some(e1.clone()));
        assert_eq!(iter.next(), Some(e5.clone()));
        assert_eq!(iter.next(), Some(e3.clone()));
        assert_eq!(iter.next(), Some(e4.clone()));
        assert_eq!(iter.next(), Some(e6.clone()));
        assert_eq!(iter.next(), None);
        drop(iter);

        assert_eq!(controller.get(&e1.key), Some(e1.val_obj.clone()));
        assert_eq!(controller.get(&e3.key), Some(e3.val_obj.clone()));
        assert_eq!(controller.get(&e4.key), Some(e4.val_obj.clone()));
        assert_eq!(controller.get(&e5.key), Some(e5.val_obj.clone()));
        assert_eq!(controller.get(&e6.key), Some(e6.val_obj.clone()));
    }

    #[test]
    fn first_and_second_present() {
        let tmp_dir = tempdir().unwrap();
        let comparator = Arc::new(BytesI32Comparator {});
        let db_opts = Arc::new(
            DbOptions {
                key_comparator: comparator,
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

        let level_opts = Arc::new(
            LevelsOptions {
                level_max_size: sstable1.size_on_disk + sstable2.size_on_disk,
                num_levels: 2,
                next_level_size_multiple: 1,
            }
        );
        let controller = SimpleLevelsController::new_empty(id_generator, level_opts, db_opts.clone());


        controller.add_to_l0(sstable1).unwrap();
        controller.add_to_l0(sstable2).unwrap();
        controller.add_to_l0(sstable3).unwrap();

        // should be at level 0 and 1
        let levels = controller.levels.read().unwrap();
        assert_eq!(levels.len(), 2);
        assert_eq!(&levels[0].size_on_disk, &size_on_disk_3);
        assert_ne!(levels[1].size_on_disk, 0);

        assert_eq!(levels[0].run.len(), 1);
        assert_eq!(levels[1].run.len(), 1);
        drop(levels);

        let mut iter = controller.get_iterator();
        assert_eq!(iter.next(), Some(e7.clone()));
        assert_eq!(iter.next(), Some(e8.clone()));
        assert_eq!(iter.next(), Some(e3.clone()));
        assert_eq!(iter.next(), Some(e4.clone()));
        assert_eq!(iter.next(), Some(e9.clone()));
        assert_eq!(iter.next(), None);
        drop(iter);

        assert_eq!(controller.get(&e7.key), Some(e7.val_obj.clone()));
        assert_eq!(controller.get(&e8.key), Some(e8.val_obj.clone()));
        assert_eq!(controller.get(&e3.key), Some(e3.val_obj.clone()));
        assert_eq!(controller.get(&e4.key), Some(e4.val_obj.clone()));
        assert_eq!(controller.get(&e9.key), Some(e9.val_obj.clone()));
    }
}