/*
When to compact:

- When numbers of memtables exceeds threshold.
- When other levels exceed some threshold
 */

use crate::entry::{Entry, Key, ValObj};
use crate::sstable::SSTable;
use crate::errors::Result;

pub trait LevelsController {
    // not mut because it should handle concurrency inside
    fn add_to_l0(&self, sstable: SSTable) -> Result<()>;
    fn get(&self, key: &Key) -> Result<ValObj>;
    fn iter(&self) -> dyn Iterator<Item = Entry>;
}