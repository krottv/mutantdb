use std::sync::{Arc, RwLock};
use crate::builder::Builder;
use crate::core::SSTableIdGenerator;
use crate::entry::{Entry, EntryComparator, Key, ValObj};
use crate::levels::LevelsController;
use crate::sstable::SSTable;
use crate::errors::Result;
use crate::iterators::concat_iterator::ConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::sstable_iterator::SSTableIterator;
use crate::opts::{DbOptions, LevelsOptions};


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
        let total_iterator = self.create_iterator_for_level(level_id, &levels);
        // new level
        let new_level = self.create_level(level_id, total_iterator)?;

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
    
    fn get_iterator(&self) -> Box<dyn Iterator<Item=Entry>> {
        
        let levels = self.levels.read().unwrap();
        let iter1 = self.create_iterator_for_level(0, &levels);

        let iterators = vec![*iter1];
        let entry_comparator = EntryComparator::new(self.db_opts.key_comparator.clone());
        MergeIterator::new(iterators, entry_comparator)
    }

    fn create_iterator_for_level(&self, level_id: u32, levels: &Vec<Level>) -> Box<dyn Iterator<Item=Entry>> {
        if level_id == 0 {
            Box::new(self.create_iterator_l0(levels))
        } else {
            Box::new(self.create_iterator_lx(level_id, levels))
        }
    }

    fn create_iterator_l0(&self, levels: &Vec<Level>) -> impl Iterator<Item=Entry> {
        let mut iterators = Vec::new();

        for sstable in &levels[0].run {
            let iter = SSTableIterator::new(sstable.clone());
            iterators.push(iter);
        }

        let entry_comparator = EntryComparator::new(self.db_opts.key_comparator.clone());
        return MergeIterator::new(iterators, Arc::new(entry_comparator));
    }

    fn create_iterator_lx(&self, level_id: u32, levels: &Vec<Level>) -> impl Iterator<Item=Entry> {
        
        let level = levels.get(level_id as usize).unwrap();
        let iterators: Vec<SSTableIterator> = level.run.iter().map(|x| {
            SSTableIterator::new(x.clone())
        }).collect();
        
        return ConcatIterator::new(iterators)
    }

    fn new_builder(&self) -> Result<Builder> {
        let path = SSTable::create_path(self.id_generator.get_new());
        Builder::new(path, self.db_opts.clone(), self.db_opts.block_max_size as usize)
    }

    // todo: not finished.
    fn create_level(&self, id: u32, iterator: Box<dyn Iterator<Item=Entry>>) -> Result<Level> {
        let mut builder = self.new_builder()?;

        let mut level = Level::new_empty(id + 1);

        for entry in iterator {
            builder.add_entry(&entry.key, &entry.val_obj)?;

            if true {
                let table = builder.build()?;
                builder = self.new_builder()?;
                level.add(Arc::new(table));
            }
        }

        return Ok(level);
    }
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