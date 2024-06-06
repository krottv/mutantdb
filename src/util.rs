use crate::errors::Result;

pub fn no_fail<T>(result: Result<T>, id: &str) {
    if let Err(err) = result {
        log::warn!("WARN: {}, {:?}", id, err);
    }
}