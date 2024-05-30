use std::cmp::Ordering;
use bytes::Bytes;

pub trait KeyComparator<T> {
    fn compare(&self, compare: &T, another: &T) -> Ordering;
}

pub struct BytesStringUtf8Comparator {}
pub struct VecStringUtf8Comparator {}

impl KeyComparator<Bytes> for BytesStringUtf8Comparator {
    fn compare(&self, compare: &Bytes, another: &Bytes) -> Ordering {
        let compare_res = std::str::from_utf8(compare.as_ref());
        let another_res = std::str::from_utf8(another.as_ref());

        return if compare_res.is_err() && another_res.is_err() {
            Ordering::Equal
        } else if compare_res.is_err() {
            Ordering::Less
        } else if another_res.is_err() {
            Ordering::Greater
        } else {
            compare_res.unwrap().cmp(&another_res.unwrap())
        }
    }
}

pub struct BytesI32Comparator {}

impl KeyComparator<Bytes> for BytesI32Comparator {
    fn compare(&self, compare: &Bytes, another: &Bytes) -> Ordering {
        let compare_res = compare.as_ref().try_into().map(|x| {
            i32::from_be_bytes(x)
        });
        let another_res = another.as_ref().try_into().map(|x| {
            i32::from_be_bytes(x)
        });

        return if compare_res.is_err() && another_res.is_err() {
            Ordering::Equal
        } else if compare_res.is_err() {
            Ordering::Less
        } else if another_res.is_err() {
            Ordering::Greater
        } else {
            compare_res.unwrap().cmp(&another_res.unwrap())
        }
    }
}

pub struct I32Comparator {}

impl KeyComparator<i32> for I32Comparator {
    fn compare(&self, compare: &i32, another: &i32) -> Ordering {
        return compare.cmp(another)
    }
}