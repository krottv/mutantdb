use std::fs::File;
use std::path::Path;
use crate::errors::Result;

pub fn no_fail<T>(result: Result<T>, id: &str) {
    if let Err(err) = result {
        log::warn!("{}, {:?}", id, err);
    }
}

pub fn sync_dir(path: &impl AsRef<Path>) -> Result<()> {
    File::open(path.as_ref())?.sync_all()?;
    Ok(())
}