use std::cmp::Ordering;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::comparator::KeyComparator;
use crate::errors::Error::ReadInvalidRange;
use crate::errors::Result;

/**
## Encoding format for Entry
-key length (4 bytes)
-value length (4 bytes)

-meta (1byte fixed)
-version (8 bytes fixed)

-key (length from header in bytes)
-value (length from header in bytes)

TODO: check max size of key or value is 4GB.
 */

// if meta is 0, then it is invalid record or absent.
// We use big endian order everywhere for encoding of primitives.

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
    pub(crate) version: u64,
}

impl ValObj {
    pub fn get_encoded_size(&self) -> usize {
        return self.value.len() + 1 + 8;
    }
}

impl Entry {
    pub fn new(key: Key, value: Bytes, meta: u8) -> Entry {
        Entry {
            key,
            val_obj: ValObj {
                value,
                meta,
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

    pub fn encode(key: &Bytes, val_obj: &ValObj, buf: &mut BytesMut) {
        buf.put_u32(key.len() as u32);
        buf.put_u32(val_obj.value.len() as u32);
        buf.put_u8(val_obj.meta);
        buf.put_u64(val_obj.version);
        buf.extend_from_slice(key);
        buf.extend_from_slice(&val_obj.value);
    }

    pub fn encode_entry(&self, buf: &mut BytesMut) {
        Self::encode(&self.key, &self.val_obj, buf)
    }

    pub(crate) fn check_range_mmap(mmap_len: usize, offset: usize, required: usize) -> Result<()> {
        if mmap_len - offset < required {
            return Err(ReadInvalidRange(format!(
                "invalid range expected len {}, actual len {}",
                required, mmap_len - offset
            )));
        }
        Ok(())
    }

    pub(crate) fn check_range(buf: &Bytes, required: usize) -> Result<()> {
        if buf.remaining() < required {
            return Err(ReadInvalidRange(format!(
                "invalid range expected len {}, actual len {}",
                required, buf.remaining()
            )));
        }
        Ok(())
    }

    pub fn decode(buf: &mut Bytes) -> Result<Entry> {
        Self::check_range(buf, 4)?;
        let key_len = buf.get_u32() as usize;

        Self::check_range(buf, 4)?;
        let value_len = buf.get_u32() as usize;

        Self::check_range(buf, 1)?;
        let meta = buf.get_u8();

        Self::check_range(buf, 8)?;
        let version = buf.get_u64();

        Self::check_range(buf, key_len)?;
        let key = buf.slice(0..key_len);
        buf.advance(key_len);

        Self::check_range(buf, value_len)?;
        let value = buf.slice(0..value_len);
        buf.advance(value_len);

        Ok(Entry {
            key,
            val_obj: ValObj {
                value,
                meta,
                version,
            },
        })
    }
}

pub struct EntryComparator {
    inner: Arc<dyn KeyComparator<Bytes>>
}

impl EntryComparator {
    pub fn new(inner: Arc<dyn KeyComparator<Bytes>>) -> Self {
        EntryComparator {
            inner
        }
    }
}

impl KeyComparator<Entry> for EntryComparator {
    fn compare(&self, compare: &Entry, another: &Entry) -> Ordering {
        self.inner.compare(&compare.key, &another.key)
    }
}


#[cfg(test)]
mod tests {
    use bytes::{Buf, Bytes, BytesMut};

    use crate::entry::{Entry, ValObj};

    #[test]
    fn encode_decode() {
        let entry_original = Entry {
            key: Bytes::from("key3"),
            val_obj: ValObj {
                value: Bytes::from("value3"),
                meta: 10,
                version: 1000,
            },
        };

        let mut encode_buf = BytesMut::new();
        entry_original.encode_entry(&mut encode_buf);

        let mut bytes = encode_buf.copy_to_bytes(encode_buf.len());
        let entry_decoded = Entry::decode(&mut bytes);

        assert_eq!(entry_original, entry_decoded.unwrap());
    }
}
