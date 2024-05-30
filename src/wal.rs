use std::{fs};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use bytes::{BytesMut};
use memmap2::MmapMut;

use crate::entry::{Entry, ZERO_ENTRY_SIZE};
use crate::opts::DbOptions;
use crate::errors::Result;

//todo: checksum to verify if WAL corrupted
//todo: think about flushing on drop call, because the whole DB can be dropped and it would be good to flush/trunk in that case

// Is it worth using memory map?
// Traditional file writing involves copying data from user space to kernel space buffers and then to disk.
// With memory mapping, the data is directly mapped to the process's address space, avoiding the extra copy to kernel buffers.

// But, at the same time here memory caching is not needed at all.
pub struct Wal {
    mmap: MmapMut,
    path: PathBuf,
    file: File,
    // in bytes
    pub write_at: u64,
    // in bytes
    len: u64,
    entry_buf: BytesMut,
}

impl Wal {
    pub fn open(path: PathBuf, opts: &DbOptions) -> Result<Wal> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let len = opts.max_wal_size;
        let original_len = file.metadata()?.len();

        // if don't take max, then changing wal options can remove some data,
        // which is bad.
        let new_len = std::cmp::max(len, original_len);

        if new_len != original_len {
            file.set_len(new_len)?;
            file.sync_all()?;
        }

        unsafe {
            let mmap = MmapMut::map_mut(&file)?;
            return Ok(
                Wal {
                    mmap,
                    path,
                    file,
                    write_at: 0,
                    len: new_len,
                    entry_buf: BytesMut::new(),
                }
            );
        }
    }

    // only for case when single entry is bigger then the limit
    pub fn ensure_len(&mut self, entry: &Entry) -> Result<()> {
        let entry_size_bytes = entry.get_encoded_size_entry();
        let size_required = entry_size_bytes as u64 + self.write_at;
        if size_required >= self.len {
            self.set_len(size_required)?;
        }

        return Ok(());
    }

    /**
    The clone_from_slice operation is a single,
    optimized memory copy operation, which can be more efficient than a loop.
     */
    pub fn add(&mut self, entry: &Entry) -> Result<()> {
        self.ensure_len(entry)?;
        
        self.entry_buf.clear();
        self.entry_buf.reserve(entry.get_encoded_size_entry());
        entry.encode_entry(&mut self.entry_buf);

        self.mmap[self.write_at as usize..self.write_at as usize + self.entry_buf.len()]
            .clone_from_slice(&self.entry_buf);

        self.write_at += self.entry_buf.len() as u64;
        
        self.zero_next_entry()?;

        return Ok(());
    }

    fn set_len(&mut self, new_len: u64) -> Result<()> {
        if self.len > new_len {
            panic!("provided len is smaller then before");
        } else if self.len < new_len {
            self.len = new_len;
            self.file.set_len(new_len)?;
            self.file.sync_all()?;
            unsafe {
                // reopen to reflect changed length.
                self.mmap = MmapMut::map_mut(&self.file)?
            }
        }

        return Ok(());
    }

    // zero next entry is a safety measure 
    // if opening wal which has some zero entry in between. 
    // because in this case if we fill that empty gap, the next opening of wal,
    // will attempt to read next entries also, which creates inconsistent situation.
    pub fn zero_next_entry(&mut self) -> Result<()> {
        
        if self.write_at + ZERO_ENTRY_SIZE as u64 <= self.len {
            let range =
                &mut self.mmap[self.write_at as usize..self.write_at as usize + ZERO_ENTRY_SIZE];
            
            unsafe {
                std::ptr::write_bytes(range.as_mut_ptr(), 0, range.len());
            }
        }
       
        Ok(())
    }
    
    pub fn truncate(&mut self) -> Result<()> {
        self.flush()?;
        if self.write_at < self.len {
            self.file.set_len(self.write_at)?;
            self.file.sync_all()?;
            self.len = self.write_at;
        }
        
        return Ok(());
    }

    pub fn flush(&self) -> Result<()> {
        self.mmap.flush()?;
        return Ok(());
    }

    pub fn delete(&self) -> Result<()> {
        fs::remove_file(&self.path)?;
        return Ok(());
    }
}

impl<'a> IntoIterator for &'a mut Wal {
    type Item = Entry;

    type IntoIter = WalIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        WalIterator::new(self)
    }
}

pub struct WalIterator<'a> {
    wal: &'a mut Wal,
    index_bytes: u64,
}

impl<'a> WalIterator<'a> {
    pub fn new(wal: &mut Wal) -> WalIterator {
        WalIterator {
            wal,
            index_bytes: 0,
        }
    }
}

impl<'a> Iterator for WalIterator<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        return if self.index_bytes >= self.wal.len {
            None
        } else {
            let item = Entry::read_mmap(self.index_bytes, &self.wal.mmap);

            // todo: might not be the best place. Do we need it in all cases of iteration?
            if item.is_absent() {
                // restore write position
                self.wal.write_at = self.index_bytes;
                None
            } else {
                self.index_bytes += item.get_encoded_size_entry() as u64;
                Some(item)
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tempfile::tempdir;
    use crate::entry;

    use crate::entry::{Entry, ValObj};
    use crate::opts::DbOptions;
    use crate::wal::Wal;

    #[test]
    fn iterator() {
        let e1 = Entry::new(Bytes::from("key1"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("key2"), Bytes::from("value2"), entry::META_ADD);
        let e3 = Entry {
            key: Bytes::from("key3"),
            val_obj: ValObj {
                value: Bytes::from("value3"),
                meta: 10,
                user_meta: 15,
                version: 1000,
            },
        };
        let tmp_dir = tempdir().unwrap();
        let wal_path = tmp_dir.path().join("1.wal");
        let encoded_size = e1.get_encoded_size_entry() as u64
            + e2.get_encoded_size_entry() as u64
            + e3.get_encoded_size_entry() as u64;

        let opts = DbOptions {
            max_wal_size: encoded_size,
            ..Default::default()
        };
        
        let mut wal = Wal::open(wal_path, &opts).unwrap();


        wal.add(&e1).unwrap();
        wal.add(&e2).unwrap();
        wal.add(&e3).unwrap();

        let mut iter = wal.into_iter();
        assert_eq!(iter.next(), Some(e1));
        assert_eq!(iter.next(), Some(e2));
        assert_eq!(iter.next(), Some(e3));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn flush_and_reopen() {
        let e1 = Entry::new(Bytes::from("key1"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("key2"), Bytes::from("value2"), entry::META_ADD);
        let e3 = Entry {
            key: Bytes::from("key3"),
            val_obj: ValObj {
                value: Bytes::from("value3"),
                meta: 10,
                user_meta: 15,
                version: 1000,
            },
        };

        let tmp_dir = tempdir().unwrap();
        let wal_path = tmp_dir.path().join("2.wal");

        let encoded_size = e1.get_encoded_size_entry() as u64
            + e2.get_encoded_size_entry() as u64
            + e3.get_encoded_size_entry() as u64;
        
        let opts1 = DbOptions {
            max_wal_size: 10000,
            ..Default::default()
        };
        let mut wal = Wal::open(wal_path.clone(), &opts1).unwrap();
        wal.add(&e1).unwrap();
        wal.add(&e2).unwrap();
        wal.add(&e3).unwrap();

        wal.truncate().unwrap();

        assert_eq!(wal.file.metadata().unwrap().len(), encoded_size);

        drop(wal);
        
        // small size, but file shouldn't be trimmed
        let opts2 = DbOptions {
            max_wal_size: 1,
            ..Default::default()
        };
        wal = Wal::open(wal_path, &opts2).unwrap();

        let mut iter = wal.into_iter();
        assert_eq!(iter.next(), Some(e1));
        assert_eq!(iter.next(), Some(e2));
        assert_eq!(iter.next(), Some(e3));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }
}