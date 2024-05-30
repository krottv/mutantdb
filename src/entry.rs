use std::cmp::Ordering;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use memmap2::MmapMut;
use prost::EncodeError;
use crate::comparator::KeyComparator;
use crate::errors::Error;

/**
## Encoding format for Entry
-key length (4 bytes)
-value length (4 bytes)

-meta (1byte fixed)
-user_meta (1byte fixed)
-version (8 bytes fixed)

-key (length from header in bytes)
-value (length from header in bytes)

TODO: check max size of key or value is 4GB.
TODO: which byte order to use for decode? Does it matter for all platforms? Currently BE is used.
 */

// if meta is 0, then it is invalid record or absent.
pub const META_DELETE: u8 = 1 << 0;
pub const META_ADD: u8 = 1 << 1;

pub const ZERO_ENTRY_SIZE: usize = 18;

pub type Key = Bytes;

#[derive(PartialEq, Eq, Debug, Default, Clone)]
pub struct Entry {
    pub key: Key,
    pub val_obj: ValObj,
}

#[derive(PartialEq, Eq, Debug, Default, Clone)]
pub struct ValObj {
    pub value: Bytes,
    pub(crate) meta: u8,
    pub user_meta: u8,
    pub(crate) version: u64,
}

impl ValObj {
    pub fn get_encoded_size(&self) -> usize {
        return self.value.len() + 1 + 1 + 8;
    }
}

impl Entry {
    pub fn new(key: Key, value: Bytes, meta: u8) -> Entry {
        Entry {
            key,
            val_obj: ValObj {
                value,
                meta,
                user_meta: 0,
                version: 0,
            },
        }
    }

    pub fn mark_delete(&mut self) {
        self.val_obj.meta |= META_DELETE;
    }

    pub fn mark_add(&mut self) {
        self.val_obj.meta |= META_ADD;
    }

    pub fn get_encoded_size(key: &Key, val_obj: &ValObj) -> usize {
        return Self::get_header_size() + key.len() + val_obj.get_encoded_size();
    }

    pub fn get_encoded_size_entry(&self) -> usize {
        return Self::get_encoded_size(&self.key, &self.val_obj);
    }
    
    pub fn to_tuple(self) -> (Key, ValObj) {
        (self.key, self.val_obj)
    }

    pub fn get_header_size() -> usize {
        return 4 + 4;
    }

    pub fn is_absent(&self) -> bool {
        return self.val_obj.meta == 0;
    }

    pub fn encode(key: &Bytes, val_obj: &ValObj, buf: &mut BytesMut) {
        buf.put_u32(key.len() as u32);
        buf.put_u32(val_obj.value.len() as u32);
        buf.put_u8(val_obj.meta);
        buf.put_u8(val_obj.user_meta);
        buf.put_u64(val_obj.version);
        buf.extend_from_slice(key);
        buf.extend_from_slice(&val_obj.value);
    }

    pub fn encode_entry(&self, buf: &mut BytesMut) {
        Self::encode(&self.key, &self.val_obj, buf)
    }

    pub fn decode(buf: &mut Bytes) -> Entry {
        let key_len = buf.get_u32();
        let value_len = buf.get_u32();
        let meta = buf.get_u8();
        let user_meta = buf.get_u8();
        let version = buf.get_u64();

        let key = buf.slice(0..key_len as usize);
        buf.advance(key_len as usize);
        let value = buf.slice(0..value_len as usize);
        buf.advance(value_len as usize);

        return Entry {
            key,
            val_obj: ValObj {
                value,
                meta,
                user_meta,
                version,
            },
        };
    }

    pub fn decode_mut(buf: &mut BytesMut, key_len: u32, value_len: u32) -> Entry {
        let meta = buf.get_u8();
        let user_meta = buf.get_u8();
        let version = buf.get_u64();

        let key = &buf[0..key_len as usize];
        let start = key_len as usize;
        let value = &buf[start..start + value_len as usize];

        return Entry {
            key: Bytes::copy_from_slice(key),
            val_obj: ValObj {
                value: Bytes::copy_from_slice(value),
                meta,
                user_meta,
                version,
            },
        };
    }

    pub fn read_mmap(index: u64, mmap: &MmapMut) -> Entry {
        let mut offset = index as usize;
        let key_len_bytes: [u8; 4] = mmap[offset..offset + 4].try_into().unwrap();
        offset += 4;
        let key_len = u32::from_be_bytes(key_len_bytes) as usize;

        let value_len_bytes: [u8; 4] = mmap[offset..offset + 4].try_into().unwrap();
        offset += 4;
        let value_len = u32::from_be_bytes(value_len_bytes) as usize;

        let meta = mmap[offset];
        offset += 1;

        let user_meta = mmap[offset];
        offset += 1;

        let version_bytes: [u8; 8] = mmap[offset..offset + 8].try_into().unwrap();
        offset += 8;
        let version = u64::from_be_bytes(version_bytes);

        let key = Bytes::copy_from_slice(&mmap[offset..offset + key_len]);
        offset += key_len;

        let value = Bytes::copy_from_slice(&mmap[offset..offset + value_len]);

        return Entry {
            key,
            val_obj: ValObj {
                value,
                meta,
                user_meta,
                version,
            },
        };
    }
}

pub struct EntryComparator<'a> {
    inner: &'a dyn KeyComparator<Bytes>
}

impl<'a> EntryComparator<'a> {
    pub fn new(inner: &'a dyn KeyComparator<Bytes>) -> Self {
        EntryComparator {
            inner
        }
    }
}

impl<'a> KeyComparator<Entry> for EntryComparator<'a> {
    fn compare(&self, compare: &Entry, another: &Entry) -> Ordering {
        self.inner.compare(&compare.key, &another.key)
    }
}


#[cfg(test)]
mod tests {
    use crate::entry::{Entry, ValObj};
    use bytes::{Buf, Bytes, BytesMut};

    #[test]
    fn encode_decode() {
        let entry_original = Entry {
            key: Bytes::from("key3"),
            val_obj: ValObj {
                value: Bytes::from("value3"),
                meta: 10,
                user_meta: 15,
                version: 1000,
            },
        };

        let mut encode_buf = BytesMut::new();
        entry_original.encode_entry(&mut encode_buf);

        let mut bytes = encode_buf.copy_to_bytes(encode_buf.len());
        let entry_decoded = Entry::decode(&mut bytes);

        assert_eq!(entry_original, entry_decoded);
    }
}
