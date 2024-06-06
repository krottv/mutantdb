use std::marker::PhantomData;
use crate::skiplist::{SkipEntry, SkiplistRaw};
use crate::skiplist::skipnode::SkipNode;

pub struct SkipIterator<'a, KEY, VALUE> where
    KEY: Default,
    VALUE: Default {
    head_bottom: *mut SkipNode<SkipEntry<KEY, VALUE>>,
    phantom_data: PhantomData<&'a SkipEntry<KEY, VALUE>>,
}

impl<'a, KEY, VALUE> SkipIterator<'a, KEY, VALUE> where
    KEY: Default,
    VALUE: Default {
    pub fn new(skiplist_raw: &'a SkiplistRaw<KEY, VALUE>) -> Self {
        SkipIterator {
            head_bottom: unsafe { (*skiplist_raw.head_bottom).next },
            phantom_data: PhantomData {},
        }
    }
}

/**
By adding these lifetime annotations,
the Rust compiler can statically verify the memory safety of the code 
and ensure that the SkipIterator does not outlive the SkiplistRaw instance from which it was created.
 */
impl<'a, KEY, VALUE> IntoIterator for &'a SkiplistRaw<KEY, VALUE> where
    KEY: Default,
    VALUE: Default {
    type Item = &'a SkipEntry<KEY, VALUE>;
    type IntoIter = SkipIterator<'a, KEY, VALUE>;

    fn into_iter(self) -> Self::IntoIter {
        SkipIterator::new(self)
    }
}

impl<'a, KEY, VALUE> Iterator for SkipIterator<'a, KEY, VALUE> where
    KEY: 'a + Default,
    VALUE: 'a + Default {
    type Item = &'a SkipEntry<KEY, VALUE>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.head_bottom.is_null() {
            return None;
        }

        unsafe {
            let cur = &*self.head_bottom;
            let data = cur.data.get_ref();
            self.head_bottom = cur.next;
            return Some(data);
        }
    }
}


// drain version of iterator
// todo: WTF we need 2 lifetimes? Think about where lifetime for skiplist key-value

/*
The lifetime 'b, on the other hand, is associated with the 
mutable reference &'b mut SkiplistRaw<'a, KEY, VALUE> held by the SkipDrain struct. 
This lifetime is separate from 'a because the mutable reference could potentially
have a shorter lifetime than the KEY and VALUE instances.
 
Using a single lifetime 'a would not be sufficient because it would imply 
that the mutable reference &'a mut SkiplistRaw<'a, KEY, VALUE> has the 
same lifetime as the KEY and VALUE instances, which may not always be the case. 
This could lead to potential memory safety issues, such as dangling references or use-after-free bugs.
 */
pub struct SkipDrain<'b, KEY, VALUE> where
    KEY: Default,
    VALUE: Default, {
    head_bottom: *mut SkipNode<SkipEntry<KEY, VALUE>>,
    skiplist_raw: &'b mut SkiplistRaw<KEY, VALUE>,
}

impl<'b, KEY, VALUE> SkiplistRaw<KEY, VALUE> where
    KEY: Default,
    VALUE: Default {
    pub fn drain(&'b mut self) -> SkipDrain<'b, KEY, VALUE> {
        SkipDrain {
            head_bottom: unsafe { (*self.head_bottom).next },
            skiplist_raw: self,
        }
    }
}

impl<'b, KEY, VALUE> Iterator for SkipDrain<'b, KEY, VALUE> where
    KEY: Default,
    VALUE: Default {
    type Item = Box<SkipEntry<KEY, VALUE>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.head_bottom.is_null() {
            return None;
        }

        unsafe {
            let to_remove = self.head_bottom;
            let cur = &*self.head_bottom;
            self.head_bottom = cur.next;

            return Some(self.skiplist_raw.erase_node(to_remove));
        }
    }
}