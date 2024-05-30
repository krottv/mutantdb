use std::cmp::Ordering;
use bytes::Bytes;

pub trait KeyComparator<T> {
    fn compare(&self, compare: &T, another: &T) -> Ordering;
}

pub struct BytesStringUtf8Comparator {}
pub struct VecStringUtf8Comparator {}

impl KeyComparator<Bytes> for BytesStringUtf8Comparator {
    fn compare(&self, compare: &Bytes, another: &Bytes) -> Ordering {
        let compare_str_res = String::from_utf8(compare.to_vec());
        let another_str_res = String::from_utf8(another.to_vec());

        if compare_str_res.is_err() && another_str_res.is_err() {
            return Ordering::Equal
        } else if compare_str_res.is_err() {
            return Ordering::Less
        } else if another_str_res.is_err() {
            return Ordering::Greater
        } else {
            return compare_str_res.unwrap().cmp(&another_str_res.unwrap());
        }
    }
}

// impl KeyComparator<&[u8]> for BytesStringUtf8Comparator {
//     fn compare(&self, compare: &[u8], another: &[u8]) -> Ordering {
//         let compare_str_res = str::from_utf8(compare);
//         let another_str_res = str::from_utf8(another);
// 
//         return if compare_str_res.is_err() && another_str_res.is_err() {
//             Ordering::Equal
//         } else if compare_str_res.is_err() {
//             Ordering::Less
//         } else if another_str_res.is_err() {
//             Ordering::Greater
//         } else {
//             compare_str_res.unwrap().cmp(&another_str_res.unwrap())
//         }
//     }
// }

pub struct BytesI32Comparator {}

impl KeyComparator<Bytes> for BytesI32Comparator {
    fn compare(&self, compare: &Bytes, another: &Bytes) -> Ordering {
        // todo: handle errors
        let compare_i32_res = i32::from_be_bytes(compare.to_vec().try_into().unwrap());
        let another_i32_res = i32::from_be_bytes(another.to_vec().try_into().unwrap());

        compare_i32_res.cmp(&another_i32_res)
    }
}

pub struct I32Comparator {}

impl KeyComparator<i32> for I32Comparator {
    fn compare(&self, compare: &i32, another: &i32) -> Ordering {
        return compare.cmp(another)
    }
}