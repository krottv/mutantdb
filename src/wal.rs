use std::fs;
use std::fs::{File, OpenOptions};
use std::mem::ManuallyDrop;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::{Buf, BufMut, BytesMut};
use memmap2::{Advice, MmapMut};

use crate::db_options::DbOptions;
use crate::entry::{Entry, ZERO_ENTRY_SIZE};
use crate::errors::Error::{AbsentKey, CorruptedFileError};
use crate::errors::{Result};
use crate::util::no_fail;

pub struct Wal {
    // manually drop because we need to drop it before deleting file to prevent errors
    mmap: ManuallyDrop<MmapMut>,
    path: PathBuf,
    file: File,
    // in bytes
    pub write_at: usize,
    // in bytes
    len: usize,
    entry_buf: BytesMut,
    delete_on_close: AtomicBool,
}

impl Wal {
    pub fn open(path: PathBuf, opts: &DbOptions) -> Result<Wal> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let len = opts.max_wal_size;
        let original_len = file.metadata()?.len() as usize;

        // if max hasn't taken, then changing wal options can remove some data,
        // which is bad.
        let new_len = std::cmp::max(len, original_len);

        if new_len != original_len {
            file.set_len(new_len as u64)?;
            file.sync_all()?;
        }

        unsafe {
            let mmap = ManuallyDrop::new(MmapMut::map_mut(&file)?);
            mmap.advise(Advice::Sequential)?;

            return Ok(
                Wal {
                    mmap,
                    path,
                    file,
                    write_at: 0,
                    len: new_len,
                    entry_buf: BytesMut::new(),
                    delete_on_close: AtomicBool::new(false),
                }
            );
        }
    }

    // only for case when single entry is bigger then the limit
    pub fn ensure_len(&mut self, entry: &Entry) -> Result<()> {
        let entry_size_bytes = entry.get_encoded_size_entry();
        let size_required = entry_size_bytes + self.write_at + Self::HEADER_LEN;
        if size_required >= self.len {
            self.set_len(size_required)?;
        }

        return Ok(());
    }

    /**
    The clone_from_slice operation is a single,
    optimized memory copy operation, which can be more efficient than a loop.
     */
    pub fn add(&mut self, entry: &Entry) -> Result<usize> {
        self.ensure_len(entry)?;

        let entry_size = entry.get_encoded_size_entry();
        self.entry_buf.clear();
        self.entry_buf.reserve(entry_size);
        entry.encode_entry(&mut self.entry_buf);

        let crc = crc32fast::hash(&self.entry_buf);
        let mut buf_header = BytesMut::with_capacity(Self::HEADER_LEN);

        buf_header.put_u32(crc);
        buf_header.put_u32(self.entry_buf.len() as u32);

        self.mmap[self.write_at..self.write_at + Self::HEADER_LEN]
            .clone_from_slice(&buf_header);
        self.write_at += Self::HEADER_LEN;

        self.mmap[self.write_at..self.write_at + self.entry_buf.len()]
            .clone_from_slice(&self.entry_buf);
        self.write_at += self.entry_buf.len();

        let len = self.entry_buf.len() + buf_header.len();

        self.zero_next_entry()?;

        return Ok(len);
    }

    /**
    Wal has its own header of 8 bytes.
    checksum + size in bytes
     */
    const HEADER_LEN: usize = 8;


    pub fn read_entry(&self, mut offset: usize) -> Result<(Entry, usize)> {
        let mut buf_header = BytesMut::with_capacity(Self::HEADER_LEN);
        buf_header.resize(Self::HEADER_LEN, 0);
        if self.mmap.len() - offset < Self::HEADER_LEN {
            return Err(AbsentKey);
        }
        Entry::check_range_mmap(self.mmap.len(), offset, Self::HEADER_LEN)?;
        buf_header.clone_from_slice(&self.mmap[offset..offset + Self::HEADER_LEN]);

        let crc = buf_header.get_u32();
        let entry_size = buf_header.get_u32();

        if crc == 0 || entry_size == 0 {
            return Err(AbsentKey);
        }

        offset += Self::HEADER_LEN;

        let mut buf_entry = BytesMut::with_capacity(entry_size as usize);
        buf_entry.resize(entry_size as usize, 0);
        Entry::check_range_mmap(self.mmap.len(), offset, entry_size as usize)?;
        buf_entry.copy_from_slice(&self.mmap[offset..offset + entry_size as usize]);

        let actual_crc = crc32fast::hash(&buf_entry);

        if crc != actual_crc {
            return Err(CorruptedFileError);
        }

        return Ok((Entry::decode(&mut buf_entry.freeze())?, entry_size as usize + Self::HEADER_LEN));
    }

    fn set_len(&mut self, new_len: usize) -> Result<()> {
        if self.len > new_len {
            panic!("provided len is smaller then before");
        } else if self.len < new_len {
            self.len = new_len;
            self.file.set_len(new_len as u64)?;
            self.file.sync_all()?;
            unsafe {
                // reopen to reflect changed length.
                ManuallyDrop::drop(&mut self.mmap);
                self.mmap = ManuallyDrop::new(MmapMut::map_mut(&self.file)?);
            }
        }

        return Ok(());
    }

    // zero next entry is a safety measure 
    // if opening wal which has some zero entry in between. 
    // because in this case if we fill that empty gap, the next opening of wal,
    // will attempt to read next entries also, which creates inconsistent situation.
    pub fn zero_next_entry(&mut self) -> Result<()> {
        if self.write_at + ZERO_ENTRY_SIZE <= self.len {
            let range =
                &mut self.mmap[self.write_at..self.write_at + ZERO_ENTRY_SIZE];

            unsafe {
                std::ptr::write_bytes(range.as_mut_ptr(), 0, range.len());
            }
        }

        Ok(())
    }

    pub fn truncate(&mut self) -> Result<()> {
        self.flush()?;
        if self.write_at < self.len {
            self.file.set_len(self.write_at as u64)?;
            self.file.sync_all()?;
            self.len = self.write_at;
        }

        return Ok(());
    }

    pub fn flush(&self) -> Result<()> {
        self.mmap.flush()?;
        return Ok(());
    }

    pub fn mark_delete(&self) {
        self.delete_on_close.store(true, Ordering::Relaxed);
    }

    pub fn drop_no_fail(&mut self) -> Result<()> {
        unsafe {
            ManuallyDrop::drop(&mut self.mmap);
        }
        if self.delete_on_close.load(Ordering::Relaxed) {
            fs::remove_file(&self.path)?;
        }

        Ok(())
    }

    pub fn create_path(id: usize) -> String {
        return format!("{}{}", id, WAL_FILE_EXT);
    }
}

pub const WAL_FILE_EXT: &str = ".wal";

impl Drop for Wal {
    fn drop(&mut self) {
        no_fail(self.drop_no_fail(), "drop wal");
    }
}

pub struct WalIterator<'a> {
    wal: &'a Wal,
    index_bytes: usize,
    pub restore_write_at: Option<usize>,
    is_valid: bool,
}

impl<'a> WalIterator<'a> {
    pub fn new(wal: &Wal) -> WalIterator {
        WalIterator {
            wal,
            index_bytes: 0,
            restore_write_at: None,
            is_valid: true,
        }
    }

    pub fn is_valid(&self) -> bool {
        return self.is_valid;
    }
}

impl<'a> Iterator for &mut WalIterator<'a> {
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        return if self.index_bytes >= self.wal.len {
            None
        } else {
            let item_res = self.wal.read_entry(self.index_bytes);

            if let Ok((item, len)) = item_res {
                self.index_bytes += len;
                Some(item)
            } else {
                let err = item_res.err().unwrap();
                match err {
                    AbsentKey => {
                        // restore write position
                        self.restore_write_at = Some(self.index_bytes);
                        None
                    }

                    _ => {
                        self.is_valid = false;
                        log::log!(log::Level::Warn, "can't restore wal entry. {}", err);
                        None
                    }
                }
            }
        };
    }
}

#[cfg(test)]
pub mod tests {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::PathBuf;

    use bytes::{Bytes, BytesMut};
    use tempfile::{tempdir, TempDir};

    use crate::db_options::DbOptions;
    use crate::entry;
    use crate::entry::{Entry, ValObj};
    use crate::wal::{Wal, WalIterator};

    #[test]
    fn iterator() {
        let e1 = Entry::new(Bytes::from("key1"), Bytes::from("value1"), entry::META_ADD);
        let e2 = Entry::new(Bytes::from("key2"), Bytes::from("value2"), entry::META_ADD);
        let e3 = Entry {
            key: Bytes::from("key3"),
            val_obj: ValObj {
                value: Bytes::from("value3"),
                meta: 10,
                version: 1000,
            },
        };
        let tmp_dir = tempdir().unwrap();
        let wal_path = tmp_dir.path().join("1.wal");
        let encoded_size = e1.get_encoded_size_entry()
            + e2.get_encoded_size_entry()
            + e3.get_encoded_size_entry();

        let opts = DbOptions {
            max_wal_size: encoded_size,
            ..Default::default()
        };

        let mut wal = Wal::open(wal_path, &opts).unwrap();

        wal.add(&e1).unwrap();
        wal.add(&e2).unwrap();
        wal.add(&e3).unwrap();

        let mut iter = &mut WalIterator::new(&wal);
        assert_eq!(iter.next(), Some(e1));
        assert_eq!(iter.next(), Some(e2));
        assert_eq!(iter.next(), Some(e3));
        assert_eq!(iter.next(), None);
        assert!(iter.is_valid());
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
                version: 1000,
            },
        };

        let tmp_dir = tempdir().unwrap();
        let wal_path = tmp_dir.path().join("2.wal");

        let encoded_size = e1.get_encoded_size_entry()
            + e2.get_encoded_size_entry()
            + e3.get_encoded_size_entry() + Wal::HEADER_LEN * 3;

        let opts1 = DbOptions {
            max_wal_size: 10000,
            ..Default::default()
        };
        let mut wal = Wal::open(wal_path.clone(), &opts1).unwrap();
        wal.add(&e1).unwrap();
        wal.add(&e2).unwrap();
        wal.add(&e3).unwrap();

        wal.truncate().unwrap();

        assert_eq!(wal.file.metadata().unwrap().len(), encoded_size as u64);

        drop(wal);

        // small size, but file shouldn't be trimmed
        let opts2 = DbOptions {
            max_wal_size: 1,
            ..Default::default()
        };
        wal = Wal::open(wal_path, &opts2).unwrap();

        let mut iter = &mut WalIterator::new(&wal);
        assert_eq!(iter.next(), Some(e1));
        assert_eq!(iter.next(), Some(e2));
        assert_eq!(iter.next(), Some(e3));
        assert_eq!(iter.next(), None);
        assert!(iter.is_valid());
    }

    pub fn create_corrupted_file(dir: &TempDir, content: &[u8], truncate_bytes: usize) -> std::io::Result<PathBuf> {
        let file_path = dir.path().join("corrupted.fl");
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file_path)?;
        file.write_all(&content[..content.len() - truncate_bytes])?;
        file.sync_all()?;
        Ok(file_path)
    }

    #[test]
    fn test_read_corrupted_wal() {
        let temp_dir = tempdir().unwrap();

        let garbage = b"asduejfbsoeufejakjfndaskjfnidakdasojdoasjdoasjdoia";
        // Create a corrupted WAL file by truncating the encoded data
        let path = create_corrupted_file(&temp_dir, garbage, 10).unwrap();

        // Attempt to read the corrupted WAL file
        let wal = Wal::open(path, &DbOptions::default()).unwrap();
        let mut iter = WalIterator::new(&wal);

        // Collect entries and ensure no errors are produced
        let entries: Vec<_> = iter.collect();
        assert!(entries.is_empty());
        assert!(!iter.is_valid());
    }
    
    pub fn corrupt_byte(path: PathBuf, byte_pos: usize) {
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(false)
            .open(path.clone())
            .unwrap();

        let file_size = file.metadata().unwrap().len() as usize;

        // Initialize a BytesMut buffer with the file size
        let mut buffer = BytesMut::with_capacity(file_size);
        buffer.resize(file_size, 0);

        // Read the file contents into the buffer
        file.read_exact(&mut buffer).unwrap();

        // corrupt record, enough to change crc
        buffer[byte_pos] = buffer[byte_pos].saturating_add(1);

        // Ensure the file write position is at the beginning
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&buffer).unwrap();
        file.sync_all().unwrap();
    }

    #[test]
    fn test_read_corrupted_wal_wrong_encoding() {
        let tmp_dir = tempdir().unwrap();
        let wal_path = tmp_dir.path().join("some.wal");

        let e1 = Entry::new(Bytes::from("key1"), Bytes::from("value1"), entry::META_ADD);

        let opts1 = DbOptions {
            max_wal_size: 10000,
            ..Default::default()
        };
        let mut wal = Wal::open(wal_path.clone(), &opts1).unwrap();
        let _len1 = wal.add(&e1).unwrap();
        wal.truncate().unwrap();
        drop(wal);

        corrupt_byte(wal_path.clone(), 9);

        // Attempt to read the corrupted WAL file
        let wal = Wal::open(wal_path, &DbOptions::default()).unwrap();
        let mut iter = WalIterator::new(&wal);

        // Collect entries and ensure no errors are produced
        let entries: Vec<_> = iter.collect();
        assert!(!iter.is_valid());
        assert!(entries.is_empty()); // Expecting zero entries due to corruption
    }
}