pub mod concat_iterator;
pub mod merge_iterator;
pub mod sstable_iterator;
pub mod memtable_iterator;

/*
#[enum_dispatch]
pub trait AgateIterator {
    fn next(&mut self);
    fn rewind(&mut self);
    fn seek(&mut self, key: &Bytes);
    fn key(&self) -> &[u8];
    fn value(&self) -> Value;
    fn valid(&self) -> bool;
}

key returns &[u8] ref. How in the hell is this ref valid if
we call next? (invalidated). Does it mean that calling key will create immutable ref,
which will prevent all possible next calls? So destroying immutable ref is necessary for further
calls of next?

Invalid code:
        let mut skip_iter = mem_tables.mutable.skl.iter();
        skip_iter.next();
        
        let key = skip_iter.key();
        
        skip_iter.next();
        
        assert_eq!(key, &Bytes::new());
        
 */