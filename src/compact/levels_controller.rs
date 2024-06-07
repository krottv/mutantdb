use std::sync::{Arc, RwLock};
use crate::builder::Builder;
use crate::compact::level::Level;
use crate::entry::{Entry, EntryComparator, Key, ValObj};
use crate::iterators::merge_iterator::MergeIterator;
use crate::db_options::DbOptions;
use crate::sstable::id_generator::SSTableIdGenerator;
use crate::sstable::SSTable;
use crate::errors::Result;

pub struct LevelsController {
    pub id_generator: Arc<SSTableIdGenerator>,
    pub db_opts: Arc<DbOptions>,
    pub levels: RwLock<Vec<Level>>,
}

impl LevelsController {
    
    pub fn get(&self, key: &Key) -> Option<ValObj> {
        let levels = self.levels.read().unwrap();
        for level in levels.iter() {
            let entry = level.get_val(key, self.db_opts.key_comparator.clone());
            if entry.is_some() {
                return entry;
            }
        }
        None
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item=Entry>> {
        let iter = self.get_iterator();
        Box::new(iter)
    }

    pub fn get_sstable_count(&self, level_id: usize) -> Option<usize> {
        let levels = self.levels.read().unwrap();
        levels.get(level_id).map(|x| {
            x.run.len()
        })
    }

    pub fn get_sstable_count_total(&self) -> usize {
        self.levels.read().unwrap()
            .iter().fold(0usize, |x, y| {
            x + y.run.len()
        })
    }

    pub fn get_iterator(&self) -> MergeIterator<Entry> {
        let levels = self.levels.read().unwrap();

        let mut iterators: Vec<Box<dyn Iterator<Item=Entry>>> = Vec::new();
        let entry_comparator = Arc::new(EntryComparator::new(self.db_opts.key_comparator.clone()));

        for level in levels.iter() {
            let iter = level.create_iterator_for_level(entry_comparator.clone());
            iterators.push(Box::new(iter));
        }

        MergeIterator::new(iterators, entry_comparator)
    }


    fn new_builder(&self) -> Result<Builder> {
        let sstable_id = self.id_generator.get_new();
        let path = self.db_opts.sstables_path.join(SSTable::create_path(sstable_id));
        Builder::new(path, self.db_opts.clone(), self.db_opts.block_max_size as usize, sstable_id)
    }
    
    pub fn create_sstables(&self, iterator: MergeIterator<Entry>) -> Result<Vec<Arc<SSTable>>> {
        let mut builder = self.new_builder()?;
        let mut builder_entries_size: u64 = 0;
        let mut tables = Vec::new();

        for entry in iterator {
            let entry_size = entry.get_encoded_size_entry();

            if (builder_entries_size + entry_size as u64) > self.db_opts.max_memtable_size {
                let table = builder.build()?;
                tables.push(Arc::new(table));
                builder = self.new_builder()?;
                builder_entries_size = 0;
            }

            builder_entries_size += entry_size as u64;
            builder.add_entry(&entry.key, &entry.val_obj)?;
        }

        if !builder.is_empty() {
            let table = builder.build()?;
            tables.push(Arc::new(table));
        }

        return Ok(tables);
    }
}