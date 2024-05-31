#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::comparator::I32Comparator;
    use crate::skiplist::{AddResult, SkipEntry, SkiplistRaw};
    use crate::skiplist::skipiterator::{SkipDrain, SkipIterator};

    #[test]
    fn basic_search_erase() {
        let comparator = Arc::new(I32Comparator {});
        let mut list = SkiplistRaw::new(comparator, true);
        list.add(1, 0);
        list.add(2, 0);
        list.add(3, 0);
        assert!(!list.contains(&0));
        assert!(list.contains(&2));
        assert!(list.erase(&2));
        assert!(!list.contains(&2));
    }

    #[test]
    fn basic_duplicates() {
        let comparator = Arc::new(I32Comparator {});
        let mut list = SkiplistRaw::new(comparator, true);
        
        match list.add(1, 1) {
            AddResult::Added => {}
            AddResult::Replaced(_) => {
                panic!("wrong operation")
            }
        }
        match list.add(1, 2) {
            AddResult::Added => {}
            AddResult::Replaced(_) => {
                panic!("wrong operation")
            }
        }
        match list.add(1, 3) {
            AddResult::Added => {}
            AddResult::Replaced(_) => {
                panic!("wrong operation")
            }
        }
        match list.add(1, 4) {
            AddResult::Added => {}
            AddResult::Replaced(_) => {
                panic!("wrong operation")
            }
        }
        
        assert_eq!(list.size, 4);
        
        assert!(list.erase(&1));
        assert!(list.contains(&1));
        assert_eq!(list.size, 3);
    
        assert!(list.erase(&1));
        assert!(list.erase(&1));
        assert!(list.erase(&1));
        assert_eq!(list.height, 1);
    
        assert!(!list.contains(&1));
        assert_eq!(list.size, 0);
    }  
    
    #[test]
    fn basic_unique() {
        let comparator = Arc::new(I32Comparator {});
        let mut list = SkiplistRaw::new(comparator, false);
        match list.add(1, 0) {
            AddResult::Added => {}
            AddResult::Replaced(_) => {
                panic!("wrong operation")
            }
        }
        match list.add(2, 0) {
            AddResult::Added => {}
            AddResult::Replaced(_) => {
                panic!("wrong operation")
            }
        }
        match list.add(3, 0) {
            AddResult::Added => {}
            AddResult::Replaced(_) => {
                panic!("wrong operation")
            }
        }match list.add(4, 0) {
            AddResult::Added => {}
            AddResult::Replaced(_) => {
                panic!("wrong operation")
            }
        }
        
        assert_eq!(list.size, 4);
        
        match list.add(3, 100) {
            AddResult::Added => {
                panic!("wrong operation")
            }
            AddResult::Replaced(_) => {}
        }
        assert_eq!(list.size, 4);

        match list.add(1, 100) {
            AddResult::Added => {
                panic!("wrong operation")
            }
            AddResult::Replaced(_) => {}
        }
        assert_eq!(list.size, 4);
        
        match list.add(4, 100) {
            AddResult::Added => {
                panic!("wrong operation")
            }
            AddResult::Replaced(_) => {}
        }
        assert_eq!(list.size, 4);
    }
    
    #[test]
    fn test_clear() {
        let comparator = Arc::new(I32Comparator {});
        let mut list = SkiplistRaw::new(comparator, true);
        list.add(1,0);
        list.add(2,0);
        list.add(3,0);
        assert_eq!(list.size, 3);
        list.clear();
        assert_eq!(list.size, 0);
        assert!(!list.contains(&1));
        assert!(!list.contains(&2));
        assert!(!list.contains(&3));
    }

    #[test]
    fn test_iterator() {
        let comparator = Arc::new(I32Comparator {});
        let mut list = SkiplistRaw::new(comparator, true);
        list.add(1, 101);
        list.add(1, 102);
        list.add(1, 103);
        list.add(1, 104);
        list.add(2, 201);
        list.add(3, 301);
        list.add(4, 401);
        list.add(5, 501);
        
        assert_eq!(list.size, 8);

        let mut iter = list.into_iter();
        // todo: fix skiplist inserts duplicates in reverse order.
        assert_eq!(next_map(&mut iter), Some(MySkipEntry { key: 1, value: 104}));
        assert_eq!(next_map(&mut iter), Some(MySkipEntry { key: 1, value: 103}));
        assert_eq!(next_map(&mut iter), Some(MySkipEntry { key: 1, value: 102}));
        assert_eq!(next_map(&mut iter), Some(MySkipEntry { key: 1, value: 101}));
        assert_eq!(next_map(&mut iter), Some(MySkipEntry { key: 2, value: 201}));
        assert_eq!(next_map(&mut iter), Some(MySkipEntry { key: 3, value: 301}));
        assert_eq!(next_map(&mut iter), Some(MySkipEntry { key: 4, value: 401}));
        assert_eq!(next_map(&mut iter), Some(MySkipEntry { key: 5, value: 501}));
        assert_eq!(next_map(&mut iter), None);
        assert_eq!(next_map(&mut iter), None);
        assert_eq!(next_map(&mut iter), None);
        
        assert_eq!(list.size, 8);
    }

    #[test]
    fn test_drain_iterator() {
        let comparator = Arc::new(I32Comparator {});
        let mut list = SkiplistRaw::new(comparator, true);
        list.add(1, 101);
        list.add(1, 102);
        list.add(1, 103);
        list.add(1, 104);
        list.add(2, 201);
        list.add(3, 301);
        list.add(4, 401);
        list.add(5, 501);

        // lifetime guarantees!
        // let vl = list.search(&3).unwrap();
        // list.erase(&3);
        // println!("removed! {}", vl);
        
        assert_eq!(list.size, 8);
        {
            let mut iter = list.drain();
            assert_eq!(next_map_drain(&mut iter), Some(MySkipEntry { key: 1, value: 104}));
            assert_eq!(next_map_drain(&mut iter), Some(MySkipEntry { key: 1, value: 103}));
            assert_eq!(next_map_drain(&mut iter), Some(MySkipEntry { key: 1, value: 102}));
            assert_eq!(next_map_drain(&mut iter), Some(MySkipEntry { key: 1, value: 101}));
            assert_eq!(next_map_drain(&mut iter), Some(MySkipEntry { key: 2, value: 201}));
            assert_eq!(next_map_drain(&mut iter), Some(MySkipEntry { key: 3, value: 301}));
            assert_eq!(next_map_drain(&mut iter), Some(MySkipEntry { key: 4, value: 401}));
            assert_eq!(next_map_drain(&mut iter), Some(MySkipEntry { key: 5, value: 501}));
            assert_eq!(next_map_drain(&mut iter), None);
            assert_eq!(next_map_drain(&mut iter), None);
            assert_eq!(next_map_drain(&mut iter), None);
        }

        assert_eq!(list.size, 0);
        // map can work find with only one lifetime annotation
        
        
        // let mut map: HashMap<i32, i32> = HashMap::new();
        // map.insert(1, 1);
        // map.insert(2, 2);
        // 
        // let mut iter = map.iter_mut();
        // iter.next();
        // iter.next();
        // 
        // map.len();
    }
    
    #[derive(PartialEq, Eq, Debug)]
    struct MySkipEntry {
        key: i32, 
        value: i32 
    }
    
    impl MySkipEntry {
        fn new(entry: &SkipEntry<i32, i32>) -> MySkipEntry {
            MySkipEntry {
                key: entry.key,
                value: entry.value
            }
        }
    }
    
    fn next_map(iter: &mut SkipIterator<i32, i32>) -> Option<MySkipEntry> {
        iter.next().map(| x | {
            MySkipEntry::new(x)
        })
    }
    
    fn next_map_drain(iter: &mut SkipDrain<i32, i32>) -> Option<MySkipEntry> {
        iter.next().map(| x | {
            MySkipEntry::new(&x)
        })
    }
}