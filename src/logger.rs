use crate::errors::Error;

pub trait Logger: Send + Sync {
    fn on_compaction_error(&self, error: Error);
}