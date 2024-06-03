use std::cmp::Ordering;
use std::ptr::null_mut;
use std::sync::Arc;

use crate::comparator::KeyComparator;
use crate::skiplist::coinflipper::{CoinFlipper, CoinFlipperRand};
use crate::skiplist::skipnode::SkipNode;
use crate::skiplist::skipvalue::SkipData;

mod coinflipper;
mod skipnode;
mod skipvalue;
mod skipiterator;
mod skiplist_test;

type NodePtr<KEY, VALUE> = *mut SkipNode<SkipEntry<KEY, VALUE>>;
type Comparator<KEY> = Arc<dyn KeyComparator<KEY>>;

pub enum AddResult<KEY, VALUE> where
    KEY: Default,
    VALUE: Default {
    Added,
    Replaced(Box<SkipEntry<KEY, VALUE>>),
}

pub struct SkipEntry<KEY, VALUE> where
    KEY: Default,
    VALUE: Default {
    pub key: KEY,
    pub value: VALUE,
}

impl<KEY, VALUE> Default for SkipEntry<KEY, VALUE> where
    KEY: Default,
    VALUE: Default {
    fn default() -> Self {
        SkipEntry {
            key: KEY::default(),
            value: VALUE::default(),
        }
    }
}

impl<KEY, VALUE> SkipEntry<KEY, VALUE> where
    KEY: Default,
    VALUE: Default {
    pub fn to_tuple(self) -> (KEY, VALUE) {
        (self.key, self.value)
    }
}

pub struct SkiplistRaw<KEY, VALUE> where
    KEY: Default,
    VALUE: Default {
    pub size: usize,
    pub height: usize,
    head: NodePtr<KEY, VALUE>,
    head_bottom: NodePtr<KEY, VALUE>,
    coin_flipper: Box<dyn CoinFlipper>,
    key_comparator: Comparator<KEY>,
    allow_duplicates: bool,
}

impl<KEY, VALUE> SkiplistRaw<KEY, VALUE> where
    KEY: Default,
    VALUE: Default {
    pub fn new(key_comparator: Comparator<KEY>, allow_duplicates: bool) -> Self {
        let dummy = Box::into_raw(Box::new(SkipNode::new(SkipData::Dummy())));
        return SkiplistRaw {
            size: 0,
            height: 1,
            head: dummy,
            head_bottom: dummy,
            coin_flipper: Box::new(CoinFlipperRand {}),
            key_comparator,
            allow_duplicates,
        };
    }
    //  1 -> 2 -> 3 -> 4 -> 5, target = 3
    fn search_prev(&self, target: &KEY) -> NodePtr<KEY, VALUE> {
        unsafe {
            let mut cur = self.head;

            loop {
                while !(*cur).next.is_null() {
                    let ord = self.key_comparator.compare(target, &(*(*cur).next).data.get_ref().key);
                    if ord.is_le() {
                        break;
                    }
                    cur = (*cur).next
                }

                if (*cur).below.is_null() {
                    break;
                } else {
                    cur = (*cur).below
                }
            }

            return cur;
        }
    }

    fn flip_coin(&self) -> bool {
        return self.coin_flipper.flip();
    }

    pub fn search(&self, target: &KEY) -> Option<&VALUE> {
        unsafe {
            let prev = self.search_prev(&target);
            return if self.next_equals(prev, target) {
                Some(&(*(*prev).next).data.get_ref().value)
            } else {
                None
            };
        }
    }

    pub fn contains(&self, target: &KEY) -> bool {
        return self.search(target).is_some();
    }

    fn next_equals(&self, node: NodePtr<KEY, VALUE>, target: &KEY) -> bool {
        unsafe {
            let found = &*node;
            return !found.next.is_null() && self.key_comparator.compare(&(&*found.next).data.get_ref().key, target) == Ordering::Equal;
        }
    }

    fn insert(after: NodePtr<KEY, VALUE>, data: SkipData<SkipEntry<KEY, VALUE>>) -> NodePtr<KEY, VALUE> {
        unsafe {
            let link = Box::into_raw(Box::new(SkipNode::new(data)));
            (*link).prev = after;
            if !(*after).next.is_null() {
                (*link).next = (*after).next;
                (*(*after).next).prev = link;
            }
            (*after).next = link;
            return link;
        }
    }

    pub fn add(&mut self, key: KEY, value: VALUE) -> AddResult<KEY, VALUE> {
        self.check_state();
        let mut erased = None;

        unsafe {
            let data_pointer = Box::into_raw(Box::new(SkipEntry { key, value }));
            let mut cur = self.search_prev(&(*data_pointer).key);

            if !self.allow_duplicates {
                erased = self.may_erase_if_next_eq(&(*data_pointer).key, cur);
            }

            let mut h = 1;

            let node_data = SkipData::Owned(Box::from_raw(data_pointer), data_pointer);

            let mut inserted = Self::insert(cur, node_data);

            while self.size > 0 && self.flip_coin() {
                if h >= self.height {
                    self.create_new_level();
                    // only one level at a time
                    break;
                } else {
                    while (*cur).above.is_null() {
                        cur = (*cur).prev;
                    }
                    cur = (*cur).above;

                    let inserted_temp = Self::insert(cur, SkipData::Pointer(data_pointer));

                    (*inserted).above = inserted_temp;
                    (*inserted_temp).below = inserted;
                    inserted = inserted_temp;

                    h += 1;
                }
            }

            self.size += 1;
        }
        self.check_state();

        return match erased {
            None => {
                AddResult::Added
            }
            Some(value) => {
                AddResult::Replaced(value)
            }
        };
    }

    fn check_state(&self) {
        unsafe {
            if !(*self.head).above.is_null() {
                panic!("head.above is assigned")
            }
            if !(*self.head).prev.is_null() {
                panic!("head.prev is assigned")
            }
            if !(*self.head_bottom).below.is_null() {
                panic!("head_bottom.below is assigned")
            }
            if !(*self.head_bottom).prev.is_null() {
                panic!("head_bottom.prev is assigned")
            }
        }
    }

    //todo: a level with nothing except dummy node can be
    // produced, which doesn't serve any purpose.
    fn create_new_level(&mut self) {
        self.check_state();

        unsafe {
            self.height += 1;

            let mut cur_above = Box::into_raw(Box::new(SkipNode::new(SkipData::Dummy())));
            (*cur_above).below = self.head;

            (*self.head).above = cur_above;
            let mut cur_below = (*self.head).next;
            self.head = cur_above;

            while !cur_below.is_null() {
                if self.flip_coin() {
                    let new_node = Box::into_raw(Box::new(
                        SkipNode::new(SkipData::Pointer((*cur_below).data.get_pointer()))));
                    (*new_node).prev = cur_above;
                    (*new_node).below = cur_below;

                    (*cur_above).next = new_node;
                    (*cur_below).above = new_node;

                    cur_above = new_node;
                }

                cur_below = (*cur_below).next
            }
        }
        self.check_state();
    }

    fn remove(node: NodePtr<KEY, VALUE>) {
        unsafe {
            let r = &mut *node;

            if !r.prev.is_null() {
                (*r.prev).next = r.next;
            }
            if !r.next.is_null() {
                (*r.next).prev = r.prev;
            }
            r.next = null_mut();
            r.prev = null_mut();
            r.below = null_mut();
            r.above = null_mut();

            let _ = Box::from_raw(node);
        }
    }

    pub fn erase_return(&mut self, key: &KEY) -> Option<Box<SkipEntry<KEY, VALUE>>> {
        let found = self.search_prev(key);
        self.may_erase_if_next_eq(key, found)
    }

    pub fn erase(&mut self, key: &KEY) -> bool {
        return self.erase_return(key).is_some();
    }

    pub fn erase_node(&mut self, node: NodePtr<KEY, VALUE>) -> Box<SkipEntry<KEY, VALUE>> {
        self.size -= 1;

        let mut to_delete = node;

        let removed_value: Box<SkipEntry<KEY, VALUE>>;

        unsafe {
            match &mut (*to_delete).data {
                SkipData::Dummy() => { panic!("node that is deleted should contain owned value") }
                SkipData::Owned(owned, _) => {
                    removed_value = std::mem::replace(owned, Box::new(SkipEntry::default()));
                }
                SkipData::Pointer(_) => { panic!("node that is deleted should contain owned value") }
            }


            let mut level_removed = false;
            let mut cur_level = 0;

            while !to_delete.is_null() {
                let tmp = to_delete;
                to_delete = (*to_delete).above;


                // erase the whole level. Except if it is the first level (first level can't be removed)
                if cur_level > 0 && (*tmp).next.is_null() && (*(*tmp).prev).data.is_none() && self.height > 1 {
                    self.height -= 1;

                    // first removal of the whole level
                    if !level_removed {
                        self.head = (*(*tmp).prev).below;

                        (*self.head).above = null_mut();

                        if self.head.is_null() {
                            panic!("head is null")
                        }
                    }

                    Self::remove((*tmp).prev);

                    level_removed = true;
                } else if level_removed {
                    panic!("should enter level removed clause")
                }

                Self::remove(tmp);
                cur_level += 1;
            }

            return removed_value;
        }
    }

    fn may_erase_if_next_eq(&mut self, key: &KEY, found: NodePtr<KEY, VALUE>) -> Option<Box<SkipEntry<KEY, VALUE>>> {
        unsafe {
            return if self.next_equals(found, key) {
                let nxt = (*found).next;
                Some(self.erase_node(nxt))
            } else {
                None
            };
        }
    }
    pub(crate) fn release_all_levels(node: NodePtr<KEY, VALUE>) {
        unsafe {
            let mut cur = node;
            while !cur.is_null() {
                let _ = Box::from_raw(cur);
                cur = (*cur).above;
            }
        }
    }

    pub fn release_pointers(&self) {
        unsafe {
            let mut cur = (*self.head_bottom).next;
            while !cur.is_null() {
                let next = (*cur).next;
                Self::release_all_levels(cur);
                cur = next;
            }
        }
    }

    pub fn clear(&mut self) {
        self.release_pointers();
        self.size = 0;
        self.height = 1;
        unsafe {
            (*self.head).nullify_pointers();
            (*self.head_bottom).nullify_pointers();
        }
    }
}

impl<KEY, VALUE> Drop for SkiplistRaw<KEY, VALUE> where
    KEY: Default,
    VALUE: Default {
    fn drop(&mut self) {
        self.release_pointers();
        // release dummy nodes also
        Self::release_all_levels(self.head_bottom);
    }
}