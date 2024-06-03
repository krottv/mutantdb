use std::cmp::Ordering;
use std::sync::{Arc, RwLock};

use crate::builder::Builder;
use crate::core::SSTableIdGenerator;
use crate::entry::{Entry, EntryComparator, Key, ValObj};
use crate::errors::Result;
use crate::iterators::concat_iterator::ConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::sstable_iterator::SSTableIterator;
use crate::levels::LevelsController;
use crate::opts::{DbOptions, LevelsOptions};
use crate::sstable::SSTable;

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
    level_opts: Arc<LevelsOptions>,
    db_opts: Arc<DbOptions>,
    levels: RwLock<Vec<Level>>,
}

impl SimpleLevelsController {
    pub fn new_empty(id_generator: SSTableIdGenerator, level_opts: Arc<LevelsOptions>, db_opts: Arc<DbOptions>) -> Self {
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

        // start compaction
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
        
        return ConcatIterator::new(iterators)
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
            
            return Ordering::Equal
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
        let path = SSTable::create_path(self.id_generator.get_new());
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
}