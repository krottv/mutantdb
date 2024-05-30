use std::ptr::null_mut;

use crate::skiplist::skipvalue::SkipData;

pub struct SkipNode<T> {
    // if null, then it is a dummy node
    pub data: SkipData<T>,
    pub next: *mut SkipNode<T>,
    pub prev: *mut SkipNode<T>,
    pub below: *mut SkipNode<T>,
    pub above: *mut SkipNode<T>,
}

impl<T> SkipNode<T> {
    pub fn new(data: SkipData<T>) -> Self {
        return SkipNode {
            data,
            next: null_mut(),
            prev: null_mut(),
            below: null_mut(),
            above: null_mut(),
        };
    }
    
    pub fn nullify_pointers(&mut self) {
        self.next = null_mut();
        self.prev = null_mut();
        self.below = null_mut();
        self.above = null_mut();
    }
}