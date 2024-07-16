use std::ptr::null_mut;

use crate::skiplist::skipdata::SkipData;

pub struct SkipNode<T> {
    // if null, then it is a dummy node
    pub data: SkipData<T>,
    pub next: *mut SkipNode<T>,
    pub prev: *mut SkipNode<T>,
    pub below: *mut SkipNode<T>,
    pub above: *mut SkipNode<T>,
}
unsafe impl<T> Send for SkipNode<T> where T: Send {}
unsafe impl<T> Sync for SkipNode<T> where T: Sync {}

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

pub enum Direction {
    Next,
    Prev,
    Below,
    Above,
}

pub struct SkipNodeIterator<T> {
    current: *mut SkipNode<T>,
    direction: Direction,
}

impl<T> SkipNodeIterator<T> {
    pub fn new(current: *mut SkipNode<T>, direction: Direction) -> SkipNodeIterator<T> {
        return SkipNodeIterator {
            current,
            direction
        }
    }
}

impl<T> Iterator for SkipNodeIterator<T> {
    type Item = *mut SkipNode<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            return None;
        }

        let current_ptr = self.current;
        let current = unsafe { &*self.current };
        let next_ptr = match self.direction {
            Direction::Next => current.next,
            Direction::Prev => current.prev,
            Direction::Below => current.below,
            Direction::Above => current.above,
        };

        let result = Some(current_ptr);
        self.current = next_ptr;
        result
    }
}