use crate::errors::Error;

pub trait Logger {
    fn on_compaction_error(&self, error: Error);
}