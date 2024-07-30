pub struct ConcatIterator<Item, Iter: Iterator<Item = Item>> {
    iterators: Vec<Iter>,
    iterator_pos: usize
}

impl<Item, Iter: Iterator<Item = Item>> ConcatIterator<Item, Iter> {
    pub fn new(iterators: Vec<Iter>) -> Self {
        ConcatIterator {
            iterators, 
            iterator_pos: 0
        }
    }
}

impl<Item, Iter: Iterator<Item = Item>> Iterator for ConcatIterator<Item, Iter> {
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        
        while self.iterator_pos < self.iterators.len() {
            let item = self.iterators[self.iterator_pos].next();
            if item.is_some() {
                return item
            } else {
                self.iterator_pos += 1;
            }
        }
        
        return None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concat_iterator_empty() {
        let mut concat_iter: ConcatIterator<i32, std::vec::IntoIter<i32>> = ConcatIterator::new(vec![]);
        assert_eq!(concat_iter.next(), None);
    }

    #[test]
    fn test_concat_iterator_single() {
        let mut concat_iter: ConcatIterator<i32, std::vec::IntoIter<i32>> = ConcatIterator::new(vec![vec![1, 2, 3].into_iter()]);
        assert_eq!(concat_iter.next(), Some(1));
        assert_eq!(concat_iter.next(), Some(2));
        assert_eq!(concat_iter.next(), Some(3));
        assert_eq!(concat_iter.next(), None);
    }

    #[test]
    fn test_concat_iterator_multiple() {
        let mut concat_iter: ConcatIterator<i32, std::vec::IntoIter<i32>> = ConcatIterator::new(vec![
            vec![1, 2].into_iter(),
            vec![3, 4, 5].into_iter(),
            vec![].into_iter(),
            vec![6].into_iter(),
        ]);
        assert_eq!(concat_iter.next(), Some(1));
        assert_eq!(concat_iter.next(), Some(2));
        assert_eq!(concat_iter.next(), Some(3));
        assert_eq!(concat_iter.next(), Some(4));
        assert_eq!(concat_iter.next(), Some(5));
        assert_eq!(concat_iter.next(), Some(6));
        assert_eq!(concat_iter.next(), None);
    }

    #[test]
    fn test_concat_iterator_empty_iterators() {
        let mut concat_iter: ConcatIterator<i32, std::vec::IntoIter<i32>> = ConcatIterator::new(vec![
            vec![].into_iter(),
            vec![].into_iter(),
            vec![].into_iter(),
        ]);
        assert_eq!(concat_iter.next(), None);
    }
}