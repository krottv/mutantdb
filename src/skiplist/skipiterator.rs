use std::marker::PhantomData;
use crate::skiplist::{SkipEntry, SkiplistRaw};
use crate::skiplist::skipnode::SkipNode;

pub struct SkipIterator<'a, KEY, VALUE> where
    KEY: 'a + Default,
    VALUE: 'a + Default {
    head_bottom: *mut SkipNode<SkipEntry<KEY, VALUE>>,
    phantom_data: PhantomData<&'a SkipEntry<KEY, VALUE>>,
}

/**
By adding these lifetime annotations,
the Rust compiler can statically verify the memory safety of the code 
and ensure that the SkipIterator does not outlive the SkiplistRaw instance from which it was created.
 */
impl<'a, KEY, VALUE> IntoIterator for &'a SkiplistRaw<'a, KEY, VALUE> where
    KEY: 'a + Default,
    VALUE: 'a + Default {
    type Item = &'a SkipEntry<KEY, VALUE>;
    type IntoIter = SkipIterator<'a, KEY, VALUE>;

    fn into_iter(self) -> Self::IntoIter {
        SkipIterator {
            head_bottom: unsafe { (*self.head_bottom).next },
            phantom_data: PhantomData {},
        }
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
pub struct SkipDrain<'a, 'b, KEY, VALUE> where
    KEY: 'a + Default,
    VALUE: 'a + Default, {
    head_bottom: *mut SkipNode<SkipEntry<KEY, VALUE>>,
    skiplist_raw: &'b mut SkiplistRaw<'a, KEY, VALUE>,
}

impl<'a, 'b, KEY, VALUE> SkiplistRaw<'a, KEY, VALUE> where
    KEY: 'a + Default,
    VALUE: 'a + Default {
    pub fn drain(&'b mut self) -> SkipDrain<'a, 'b, KEY, VALUE> {
        SkipDrain {
            head_bottom: unsafe { (*self.head_bottom).next },
            skiplist_raw: self,
        }
    }
}

impl<'a, 'b, KEY, VALUE> Iterator for SkipDrain<'a, 'b, KEY, VALUE> where
    KEY: 'a + Default,
    VALUE: 'a + Default {
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