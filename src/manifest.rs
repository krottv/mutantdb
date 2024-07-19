use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use prost::Message;

use proto::meta::{ManifestChange, ManifestChangeSet, ManifestOperation};

use crate::db_options::DbOptions;
use crate::errors::{Error, Result};

const MANIFEST_FORMAT_VERSION: u32 = 1;

pub struct Manifest {
    // table_id -> level_id 
    pub tables: HashMap<u64, usize>,
    add_count: usize,
    delete_count: usize      
}

impl Default for Manifest {
    fn default() -> Self {
        Manifest {
            tables: HashMap::new(),
            add_count: 0,
            delete_count: 0
        }
    }
}

impl Manifest {

    // file must exist.
    // return manifest and the last offset for make truncation.
    fn read_manifest(file: &mut File) -> Result<(Manifest, usize)> {

        let mut buf_header = BytesMut::with_capacity(4); // Create an empty vector as the buffer
        buf_header.resize(4, 0);
        let read_res = file.read_exact(&mut buf_header); 
        if let Err(error) = &read_res {
            if error.kind() == ErrorKind::UnexpectedEof {
                return Ok((Manifest::default(), 0));
            }
        }
        read_res?;

        let mut byte_arr = [0u8; 4];
        byte_arr.copy_from_slice(&buf_header[0..4]);
        let version = u32::from_be_bytes(byte_arr);
        if version != MANIFEST_FORMAT_VERSION {
            return Err(Error::ManifestFormatVersionErr(MANIFEST_FORMAT_VERSION));
        }
        
        buf_header.resize(8, 0);
        let mut manifest = Manifest::default();

        let mut buf_changes = BytesMut::new();
        let mut offset = 4;
        loop {
            
            let read_res = file.read_exact(&mut buf_header);
            if let Err(error) = &read_res {
                if error.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
            }
            read_res?;
            offset += 8;

            byte_arr = [0u8; 4];
            byte_arr.copy_from_slice(&buf_header[0..4]);
            let changes_len_bytes = u32::from_be_bytes(byte_arr) as usize;

            byte_arr = [0u8; 4];
            byte_arr.copy_from_slice(&buf_header[4..8]);
            let changes_crc = u32::from_be_bytes(byte_arr);
            
            buf_changes.resize(changes_len_bytes, 0);
            
            // break if eof
            let read_res = file.read_exact(&mut buf_changes);
            if let Err(error) = &read_res {
                if error.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
            }
            read_res?;
            offset += changes_len_bytes;
            
            // hashsum validation. Just ignore if doesn't match.
            let crc_new = crc32fast::hash(&buf_changes);
            if changes_crc == crc_new {
                let change_set = ManifestChangeSet::decode(&buf_changes[..])?;
                manifest.add_changes(change_set)?;
            }
        }

        Ok((manifest, offset))
    }
    
    pub fn to_changes(&self) -> Vec<ManifestChange> {
        let mut changes = Vec::with_capacity(self.tables.len());
        for (table_id, level_id) in &self.tables {
            changes.push(Self::new_change_add(*table_id, *level_id));
        }
        changes
    }
    
    pub fn add_changes(&mut self, changes: ManifestChangeSet) -> Result<()> {
        for change in changes.changes {
            match ManifestOperation::try_from(change.operation)? {
                ManifestOperation::Add => {
                    self.tables.insert(change.table_id, change.level_id as usize);
                    self.add_count += 1;
                }
                ManifestOperation::Delete => {
                    self.tables.remove(&change.table_id);
                    self.delete_count += 1;
                }
            }
        }
        
        Ok(())
    }
    
    pub fn new_change_add(table_id: u64, level_id: usize) -> ManifestChange {
        ManifestChange {
            table_id,
            level_id: level_id as u32,
            operation: ManifestOperation::Add as i32
        }
    }
    
    pub fn new_change_delete(table_id: u64) -> ManifestChange {
        ManifestChange {
            table_id,
            level_id: 0,
            operation: ManifestOperation::Delete as i32
        }
    }
}


/**
Manifest compression should be atomic.
We write to a different file and then replace current.
*/

pub struct ManifestWriter {
    db_opts: Arc<DbOptions>,
    file: File,
    add_count: usize,
    delete_count: usize
}

impl ManifestWriter {

    pub fn open(db_opts: Arc<DbOptions>) -> Result<(Self, Manifest)> {
        let exists = db_opts.manifest_path().exists();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_opts.manifest_path())?;

        if !exists  {
            return Ok((ManifestWriter {
                db_opts,
                file,
                add_count: 0,
                delete_count: 0
            }, Manifest::default()));
        }

        let (manifest, offset) = Manifest::read_manifest(&mut file)?;
        
        file.set_len(offset as u64)?;
        file.seek(SeekFrom::End(0))?;

        Ok((ManifestWriter {
            db_opts,
            file,
            add_count: manifest.add_count,
            delete_count: manifest.delete_count
        }, manifest))
    }

    // may read and compress data when necessary
    pub fn write(&mut self, changes: Vec<ManifestChange>) -> Result<()> {
        let (add_count, delete_count) = Self::write_to_file(&mut self.file, changes)?;
        
        self.add_count += add_count;
        self.delete_count += delete_count;

        if self.threshold_compress() {
            let res_compress = self.compress();
            self.file.seek(SeekFrom::End(0))?;
            res_compress?;
        }
        
        Ok(())
    }
    
    fn write_to_file(file: &mut File, changes: Vec<ManifestChange>) -> Result<(usize, usize)> {

        if changes.is_empty() {
            return Ok((0, 0))
        }

        // write version if was absent
        if file.stream_position()? == 0 {
            let mut buf_version = BytesMut::with_capacity(4);
            buf_version.put_u32(MANIFEST_FORMAT_VERSION);
            file.write_all(&buf_version)?;
        }

        let mut add_count = 0;
        let mut delete_count = 0;
        for change in &changes {
            match ManifestOperation::try_from(change.operation)? {
                ManifestOperation::Add => {
                    add_count += 1;
                }
                ManifestOperation::Delete => {
                    delete_count += 1;
                }
            }
        }

        let changes_proto = ManifestChangeSet {
            changes
        };
        let mut buf_changes = BytesMut::new();
        changes_proto.encode(&mut buf_changes)?;

        let crc = crc32fast::hash(&buf_changes);
        let mut buf_header = BytesMut::with_capacity(8);
        buf_header.put_u32(buf_changes.len() as u32);
        buf_header.put_u32(crc);

        file.write_all(&buf_header)?;
        file.write_all(&buf_changes)?;

        Ok((add_count, delete_count))
    }
    
    fn threshold_compress(&self) -> bool {
        self.delete_count >= self.db_opts.manifest_deletion_threshold || 
            self.add_count >= (self.db_opts.manifest_deletion_threshold * 5)
    }

    // atomic compress
    fn compress(&mut self) -> Result<()> {
        self.file.rewind()?;
        
        let (manifest, _offset) = Manifest::read_manifest(&mut self.file)?;
        
        let changes = manifest.to_changes();

        let mut rewrite_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.db_opts.manifest_rewrite_path())?;
        
        let (add_count, delete_count) = Self::write_to_file(&mut rewrite_file, changes)?;
        if delete_count > 0 {
            panic!("delete count should be 0")
        }
        drop(rewrite_file);
        
        fs::rename(self.db_opts.manifest_rewrite_path(), self.db_opts.manifest_path())?;
        self.add_count = add_count;
        self.delete_count = 0;
        
        self.file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(self.db_opts.manifest_path())?;
        
        Ok(())
    }
}

/*
Test cases
1. adding file changes and reopen returns the same
2. compression makes delete_count = 0 and reduces file len
3. no panic in case of corrupted file, just Err
4. wrong checksum doesn't return the changes
 */
#[cfg(test)]
mod tests {
    use std::io::{Seek, SeekFrom, Write};
    use std::sync::Arc;

    use tempfile::tempdir;

    use proto::meta::ManifestChangeSet;

    use crate::db_options::DbOptions;
    use crate::manifest::{Manifest, ManifestWriter};

    #[test]
    fn add_and_reopen() {
        let tempdir = tempdir().unwrap();
        let opts = Arc::new(DbOptions {
            path: tempdir.path().to_path_buf(),
            manifest_deletion_threshold: 10,
            ..DbOptions::default()
        });
        
        let (mut writer, mut manifest) = ManifestWriter::open(opts.clone()).unwrap();
        // Add changes
        let changes = vec![
            Manifest::new_change_add(1, 1),
            Manifest::new_change_add(2, 2),
        ];
        manifest.add_changes(ManifestChangeSet {
            changes: changes.clone()
        }).unwrap();
        
        writer.write(changes.clone()).unwrap();

        // Reopen and verify
        let (_new_writer, new_manifest) = ManifestWriter::open(opts.clone()).unwrap();
        
        assert_eq!(manifest.tables, new_manifest.tables);
        assert_eq!(manifest.tables.len(), 2);
        assert_eq!(manifest.tables[&1], 1);
        assert_eq!(manifest.tables[&2], 2);
    }

    #[test]
    fn compression_reduces_file_len() {
        let tempdir = tempdir().unwrap();
        let opts = Arc::new(DbOptions {
            path: tempdir.path().to_path_buf(),
            manifest_deletion_threshold: 10,
            ..DbOptions::default()
        });
        
        let (mut writer, _manifest) = ManifestWriter::open(opts.clone()).unwrap();

        // Add and delete changes to trigger compression
        let changes = vec![
            Manifest::new_change_add(1, 1),
            Manifest::new_change_delete(1),
            Manifest::new_change_add(3, 3),
        ];
        writer.write(changes).unwrap();

        // Check file length before compression
        let initial_len = writer.file.metadata().unwrap().len();

        // Trigger compression
        writer.compress().unwrap();

        // Check file length after compression
        let final_len = writer.file.metadata().unwrap().len();
        assert!(final_len < initial_len);
        assert_eq!(writer.delete_count, 0);
        
        writer.file.rewind().unwrap();
        let (manifest, _offset) = Manifest::read_manifest(&mut writer.file).unwrap();
        assert_eq!(manifest.tables.len(), 1);
    }

    #[test]
    fn no_panic_on_corrupted_file() {
        let tempdir = tempdir().unwrap();
        let opts = Arc::new(DbOptions {
            path: tempdir.path().to_path_buf(),
            manifest_deletion_threshold: 10,
            ..DbOptions::default()
        });
        
        let (mut writer, _manifest) = ManifestWriter::open(opts.clone()).unwrap();

        // Corrupt the file by writing invalid data
        writer.file.write_all(b"corrupt data").unwrap();
        writer.file.seek(SeekFrom::Start(0)).unwrap();

        // Attempt to read the manifest
        let result = Manifest::read_manifest(&mut writer.file);
        assert!(result.is_err());
    }

    #[test]
    fn corrupted_header_length() {
        let tempdir = tempdir().unwrap();
        let opts = Arc::new(DbOptions {
            path: tempdir.path().to_path_buf(),
            manifest_deletion_threshold: 10,
            ..DbOptions::default()
        });
        
        let (mut writer, _manifest) = ManifestWriter::open(opts.clone()).unwrap();

        // Add some changes
        let changes = vec![
            Manifest::new_change_add(1, 1),
            Manifest::new_change_add(2, 2),
        ];
        writer.write(changes).unwrap();

        // Corrupt the header length
        writer.file.seek(SeekFrom::Start(8)).unwrap(); // Move cursor to the header length position
        writer.file.write_all(&[0xff, 0xff, 0xff, 0xff]).unwrap(); // Write an invalid length

        writer.file.seek(SeekFrom::Start(0)).unwrap();

        // Attempt to read the manifest
        let result = Manifest::read_manifest(&mut writer.file).unwrap();
        // because corrupted
        assert_eq!(result.0.tables.len(), 0);
        // expect no panic.
    }

    #[test]
    fn wrong_checksum_does_not_return_changes() {
        let tempdir = tempdir().unwrap();
        let opts = Arc::new(DbOptions {
            path: tempdir.path().to_path_buf(),
            manifest_deletion_threshold: 10,
            ..DbOptions::default()
        });
        
        let (mut writer, _manifest) = ManifestWriter::open(opts.clone()).unwrap();

        // Add changes
        let changes = vec![Manifest::new_change_add(1, 1)];
        writer.write(changes).unwrap();

        // Corrupt the checksum
        writer.file.seek(SeekFrom::Start(4)).unwrap();
        writer.file.write_all(&[0, 0, 0, 0]).unwrap();
        writer.file.seek(SeekFrom::Start(0)).unwrap();

        // Attempt to read the manifest
        let (new_manifest, _) = Manifest::read_manifest(&mut writer.file).unwrap();
        assert!(new_manifest.tables.is_empty());
    }
}