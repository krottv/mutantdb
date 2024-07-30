use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub struct SSTableIdGenerator {
    last_id: Arc<AtomicUsize>
}

impl SSTableIdGenerator {
    pub fn new(id: usize) -> Self {
        SSTableIdGenerator {
            last_id: Arc::new(AtomicUsize::new(id))
        }
    }

    pub fn get_new(&self) -> usize {
        let prev = self.last_id.fetch_add(1, Ordering::Relaxed);
        return prev + 1;
    }
    
    pub fn set_new(&self, val: usize) {
        self.last_id.store(val, Ordering::Relaxed);
    }
}