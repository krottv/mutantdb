use std::sync::{Arc, Mutex, RwLock};
use crate::builder::Builder;
use crate::compact::level::Level;
use crate::entry::{Entry, EntryComparator, Key, ValObj};
use crate::iterators::merge_iterator::MergeIterator;
use crate::db_options::DbOptions;
use crate::sstable::id_generator::SSTableIdGenerator;
use crate::sstable::SSTable;
use crate::errors::Result;
use crate::manifest::ManifestWriter;
use crate::util::sync_dir;

#[allow(dead_code)]
pub struct LevelsController {
    pub id_generator: Arc<SSTableIdGenerator>,
    pub db_opts: Arc<DbOptions>,
    // since it is modified only in compaction thread.
    // compaction threads reads can be released as soon as possible without any races.
    // reads in Core should hold lock for the whole needed duration.
    pub levels: Arc<RwLock<Vec<Level>>>,
    // not necessary thou because write only from single thread
    pub manifest_writer: Mutex<ManifestWriter>
}

impl LevelsController {
    
    // restores manifest
    pub fn open(db_opts: Arc<DbOptions>, id_generator: Arc<SSTableIdGenerator>,
                num_levels: usize) -> Result<Self> {
        let (manifest_writer, manifest) = ManifestWriter::open(db_opts.clone())?;

        let mut levels = Vec::with_capacity(num_levels);
        for i in 0..num_levels {
            levels.push(Level::new_empty(i));
        }
        
        // todo: parallelize opening
        let mut max_table_id = 0u64;
        for (table_id, level_id) in &manifest.tables {
            if *level_id < levels.len() {
                let sstable_path = db_opts.sstables_path().join(SSTable::create_path(*table_id as usize));
                
                let open_res = SSTable::open(sstable_path, db_opts.clone(), *table_id as usize);
                if let Ok(sstable) = open_res {
                    levels[*level_id].add_front(Arc::new(sstable));
                } else {
                    log::log!(log::Level::Warn, "can't restore sstable with id {}, err {}", *table_id, open_res.err().unwrap());
                }
            }
            if *table_id > max_table_id {
                max_table_id = *table_id;
            }
        }
        
        for level in &mut levels {
            level.sort_tables(db_opts.key_comparator.as_ref());
            level.validate(db_opts.key_comparator.as_ref());
        }
        
        id_generator.set_new(max_table_id as usize);
        
        Ok(
            LevelsController {
                id_generator,
                db_opts,
                levels: Arc::new(RwLock::new(levels)),
                manifest_writer: Mutex::new(manifest_writer)
            }
        )
    }
    
    // todo: really all dirs?
    pub fn sync_dir(&self) -> Result<()> {
        sync_dir(&self.db_opts.path)?;
        sync_dir(&self.db_opts.sstables_path())?;
        sync_dir(&self.db_opts.wal_path())?;
        Ok(())
    }
    
    pub fn get(&self, key: &Key) -> Option<ValObj> {
        let levels = self.levels.read().unwrap();
        for level in levels.iter() {
            let entry = level.get_val(key, self.db_opts.key_comparator.as_ref());
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


    fn new_builder(db_opts: Arc<DbOptions>, id_generator: Arc<SSTableIdGenerator>) -> Result<Builder> {
        let sstable_id = id_generator.get_new();
        let path = db_opts.sstables_path().join(SSTable::create_path(sstable_id));
        Builder::new(path, db_opts.clone(), db_opts.block_max_size as usize, sstable_id)
    }

    pub fn create_sstables(db_opts: Arc<DbOptions>, 
                           id_generator: Arc<SSTableIdGenerator>,
                           iterator: MergeIterator<Entry>) -> Result<Vec<Arc<SSTable>>> {
        let mut builder = Self::new_builder(db_opts.clone(), id_generator.clone())?;
        let mut builder_entries_size: u64 = 0;
        let mut tables = Vec::new();

        for entry in iterator {
            let entry_size = entry.get_encoded_size_entry();

            if (builder_entries_size + entry_size as u64) > db_opts.max_memtable_size {
                let table = builder.build()?;
                tables.push(Arc::new(table));
                builder = Self::new_builder(db_opts.clone(), id_generator.clone())?;
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

    pub fn log_levels(&self) {
        let mut s = String::new();
        s.push_str("Levels visualized\n");

        for level in self.levels.read().unwrap().iter() {
            let tables_str = level.run.iter().map(|x| {
                x.id.to_string()
            }).collect::<Vec<String>>().join(", ");

            let level_str = format!("level id:{}, size:{}mb. tables [{}]\n", level.id, level.size_on_disk / 1024, tables_str);
            s.push_str(level_str.as_str());
        }

        log::log!(target: "compaction", log::Level::Info, "{}" ,s);
    }
}