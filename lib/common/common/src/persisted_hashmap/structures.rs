//! On-disk structures and their parsers: header, key types, entry.

use std::fmt::Debug;
use std::hash::Hash;
use std::io::Write;
use std::{io, str};

use zerocopy::{ConvertError, FromBytes, Immutable, IntoBytes, KnownLayout};

use super::read_err;

#[repr(C)]
#[derive(Copy, Clone, Debug, FromBytes, Immutable, IntoBytes, KnownLayout)]
pub(super) struct Header {
    pub key_type: [u8; 8],
    pub buckets_pos: u64,
    pub buckets_count: u64,
}

pub(super) type ValuesLen = u32;
pub(super) type BucketOffset = u64;

pub enum ReadResult<T> {
    Ok(T),
    Incomplete,
    Invalid(io::Error),
}

/// A key that can be stored in the hash map.
pub trait Key: Debug + PartialEq + Sync + Hash {
    const ALIGN: usize;

    /// Stored in the file header.
    const NAME: [u8; 8];

    /// Reasonable guess size of the key when performing random read.
    /// Exact size for fixed-size keys, some arbitrary length for string keys.
    const KEY_SIZE_EST: usize;

    /// Returns number of bytes which `write` will write.
    fn write_bytes(&self) -> usize;

    /// Write the key to `buf`.
    fn write(&self, buf: &mut impl Write) -> io::Result<()>;

    /// Check whether the first [`Key::write_bytes()`] of `buf` match the key.
    fn matches(&self, buf: &[u8]) -> bool;

    /// Try to read the key from `buf`.
    fn from_bytes(buf: &[u8]) -> ReadResult<&Self>;
}

impl Key for str {
    const ALIGN: usize = align_of::<u8>();

    const KEY_SIZE_EST: usize = 512;

    const NAME: [u8; 8] = *b"str\0\0\0\0\0";

    fn write_bytes(&self) -> usize {
        self.len() + 1
    }

    fn write(&self, buf: &mut impl Write) -> io::Result<()> {
        buf.write_all(self.as_bytes())?;
        buf.write_all(&[0xFF])?; // 0xFF is not a valid leading byte of a UTF-8 sequence.
        Ok(())
    }

    fn matches(&self, buf: &[u8]) -> bool {
        // The sentinel value 0xFF is used to ensure that `self` has the same length as the string
        // in the entry buffer.
        //
        // Suppose `self` is a prefix of the string in the entry buffer. (it's not very likely since
        // it would require a PHF collision, but it is still possible).
        // We'd like this method to return `false` in this case. So we need not just check that the
        // first `self.len()` bytes of `buf` are equal to `self`, but also that they have the same
        // length. To achieve that, we compare `self + [0xFF]` with `buf + [0xFF]`.
        //
        // ┌───self────┐       ┌───self────┐                 ┌─────self─────┐
        //  'f' 'o' 'o' FF      'f' 'o' 'o' FF                'f' 'o' 'o' FF
        //  'f' 'o' 'o' FF      'f' 'o' 'o' 'b' 'a' 'r' FF    'f' 'o' 'o' FF 'b' 'a' 'r' FF
        // └───entry───┘       └─────────entry─────────┘     └───────────entry──────────┘
        //    Case 1                    Case 2                          Case 3
        //    (happy)                 (collision)                   (never happens)
        //
        // 1. The case 1 is the happy path. This function returns `true`.
        // 2. In the case 2, `self` is a prefix of `entry`, but since we are also checking the
        //    sentinel, this function returns `false`. (0xFF != 'b')
        // 3. Hypothetical case 3 might never happen unless the index data is corrupted. This is
        //    because it assumes that `entry` is a concatenation of three parts: a valid UTF-8
        //    string ('foo'), a byte 0xFF, and the rest ('bar'). Concatenating a valid UTF-8 string
        //    with 0xFF will always result in an invalid UTF-8 string. Such string could not be
        //    added to the index since we are adding only valid UTF-8 strings as Rust enforces the
        //    validity of `str`/`String` types.
        buf.get(..self.len()) == Some(IntoBytes::as_bytes(self))
            && buf.get(self.len()) == Some(&0xFF)
    }

    fn from_bytes(buf: &[u8]) -> ReadResult<&Self> {
        let Some(len) = buf.iter().position(|&b| b == 0xFF) else {
            return ReadResult::Incomplete;
        };
        match str::from_utf8(&buf[..len]) {
            Err(_) => ReadResult::Invalid(read_err("String key is not valid UTF-8")),
            Ok(s) => ReadResult::Ok(s),
        }
    }
}

impl Key for i64 {
    const ALIGN: usize = align_of::<i64>();

    const KEY_SIZE_EST: usize = size_of::<i64>();

    const NAME: [u8; 8] = *b"i64\0\0\0\0\0";

    fn write_bytes(&self) -> usize {
        size_of::<i64>()
    }

    fn write(&self, buf: &mut impl Write) -> io::Result<()> {
        buf.write_all(self.as_bytes())
    }

    fn matches(&self, buf: &[u8]) -> bool {
        buf.get(..size_of::<i64>()) == Some(self.as_bytes())
    }

    fn from_bytes(buf: &[u8]) -> ReadResult<&Self> {
        match i64::ref_from_prefix(buf) {
            Ok((res, _)) => ReadResult::Ok(res),
            Err(ConvertError::Alignment(_)) => {
                ReadResult::Invalid(read_err("i64 key is not properly aligned"))
            }
            Err(ConvertError::Size(_)) => ReadResult::Incomplete,
        }
    }
}

impl Key for u128 {
    const ALIGN: usize = size_of::<u128>();

    const KEY_SIZE_EST: usize = size_of::<u128>();

    const NAME: [u8; 8] = *b"u128\0\0\0\0";

    fn write_bytes(&self) -> usize {
        size_of::<u128>()
    }

    fn write(&self, buf: &mut impl Write) -> io::Result<()> {
        buf.write_all(self.as_bytes())
    }

    fn matches(&self, buf: &[u8]) -> bool {
        buf.get(..size_of::<u128>()) == Some(self.as_bytes())
    }

    fn from_bytes(buf: &[u8]) -> ReadResult<&Self> {
        match Self::ref_from_prefix(buf) {
            Ok((res, _)) => ReadResult::Ok(res),
            Err(ConvertError::Alignment(_)) => {
                ReadResult::Invalid(read_err("u128 key is not properly aligned"))
            }
            Err(ConvertError::Size(_)) => ReadResult::Incomplete,
        }
    }
}

/// Specifies which entry fields we are interested in. E.g., for
/// [`MaybeIncompleteEntryKind::KeyOnly`], we will read just enough to parse the
/// key and skip the values.
#[derive(Copy, Clone)]
#[expect(clippy::enum_variant_names)]
pub enum MaybeIncompleteEntryKind {
    KeyOnly,
    KeyAndValuesLen,
    KeyAndValues,
}

impl MaybeIncompleteEntryKind {
    /// Guesstimate the entry size based on key and value types.
    pub fn estimated_size<K: Key + ?Sized, V>(self) -> usize {
        // Entry structure:
        // ┌─1:pad─┬────2:key─────┬─3:pad─┬─4:len─┬─5:pad─┬─────6:vals─────┐
        // │ · · · │ "abcdef\xFF" │ · · · │   5   │ · · · │ 10 20 30 40 50 │
        // └───────┴──────────────┴───────┴───────┴───────┴────────────────┘

        let field2key = K::KEY_SIZE_EST;
        let field3pad = size_of::<V /*sic*/>().saturating_sub(1);
        let field4len = size_of::<ValuesLen>();
        let field5pad = size_of::<V>().saturating_sub(1);
        let field6vals = size_of::<V>() * 2; // wild guess: 2 values per key

        match self {
            Self::KeyOnly => field2key,
            Self::KeyAndValuesLen => field2key + field3pad + field4len,
            Self::KeyAndValues => field2key + field3pad + field4len + field5pad + field6vals,
        }
    }
}

/// An entry returned by [`MaybeIncompleteEntry::partial_parse`].
/// Might be complete or partial (if there are not enough bytes).
///
/// The variants are ordered by completeness.
/// | Variant           | key | values_len | values |
/// | ----------------- | --- | ---------- | ------ |
/// | `None`            |     |            |        |
/// | `Key`             | yes |            |        |
/// | `KeyAndValuesLen` | yes | yes        |        |
/// | `KeyAndValues`    | yes | yes        | yes    |
pub(super) enum MaybeIncompleteEntry<'a, K: Key + ?Sized, V> {
    None,
    Key(&'a K),
    KeyAndValuesLen(&'a K, u32),
    /// Complete entry. Also contains the remaining bytes after this entry.
    KeyAndValues(&'a K, &'a [V], &'a [u8]),
}

impl<K: Key + ?Sized, V> Copy for MaybeIncompleteEntry<'_, K, V> {}
impl<K: Key + ?Sized, V> Clone for MaybeIncompleteEntry<'_, K, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, K: Key + ?Sized, V: FromBytes + Immutable> MaybeIncompleteEntry<'a, K, V> {
    /// `true` if this entry has all fields required by `kind`.
    pub fn satisfies_kind(self, kind: MaybeIncompleteEntryKind) -> bool {
        match kind {
            MaybeIncompleteEntryKind::KeyOnly => self.key().is_some(),
            MaybeIncompleteEntryKind::KeyAndValuesLen => {
                self.key().is_some() && self.values_len().is_some()
            }
            MaybeIncompleteEntryKind::KeyAndValues => {
                self.key().is_some() && self.values().is_some()
            }
        }
    }

    pub fn key(self) -> Option<&'a K> {
        match self {
            MaybeIncompleteEntry::None => None,
            MaybeIncompleteEntry::Key(key) => Some(key),
            MaybeIncompleteEntry::KeyAndValuesLen(key, _) => Some(key),
            MaybeIncompleteEntry::KeyAndValues(key, _, _) => Some(key),
        }
    }

    pub fn values_len(self) -> Option<u32> {
        match self {
            MaybeIncompleteEntry::None => None,
            MaybeIncompleteEntry::Key(_) => None,
            MaybeIncompleteEntry::KeyAndValuesLen(_, values_len) => Some(values_len),
            MaybeIncompleteEntry::KeyAndValues(_, values, _) => Some(values.len() as u32),
        }
    }

    pub fn values(self) -> Option<&'a [V]> {
        match self {
            MaybeIncompleteEntry::None => None,
            MaybeIncompleteEntry::Key(_) => None,
            MaybeIncompleteEntry::KeyAndValuesLen(_, _) => None,
            MaybeIncompleteEntry::KeyAndValues(_, values, _) => Some(values),
        }
    }

    /// Try to parse an entry from `buf`.
    /// If there are not enough bytes in the buf, return the partially parsed entry.
    pub fn partial_parse(buf: &'a [u8]) -> io::Result<MaybeIncompleteEntry<'a, K, V>> {
        // Entry structure:
        // ┌─1:pad─┬────2:key─────┬─3:pad─┬─4:len─┬─5:pad─┬─────6:vals─────┐
        // │ · · · │ "abcdef\xFF" │ · · · │   5   │ · · · │ 10 20 30 40 50 │
        // └───────┴──────────────┴───────┴───────┴───────┴────────────────┘

        // 1. padding for the key
        let Some(buf) = align_slice_to(K::ALIGN, buf) else {
            return Ok(MaybeIncompleteEntry::None);
        };

        // 2. key
        let key = match K::from_bytes(buf) {
            ReadResult::Ok(k) => k,
            ReadResult::Incomplete => return Ok(MaybeIncompleteEntry::None),
            ReadResult::Invalid(e) => return Err(e),
        };
        let Some(buf) = buf.get(key.write_bytes()..) else {
            return Ok(MaybeIncompleteEntry::Key(key));
        };

        // 3. padding for values_len
        let Some(buf) = align_slice_to(size_of::<V /*sic*/>(), buf) else {
            return Ok(MaybeIncompleteEntry::Key(key));
        };

        // 4. values_len
        let (&values_len, buf) = match ValuesLen::ref_from_prefix(buf) {
            Ok(v) => v,
            Err(ConvertError::Alignment(_)) => {
                return Err(read_err("ValuesLen is not properly aligned"));
            }
            Err(ConvertError::Size(_)) => return Ok(MaybeIncompleteEntry::Key(key)),
        };

        // 5. padding for values
        let Some(buf) = align_slice_to(size_of::<V>(), buf) else {
            return Ok(MaybeIncompleteEntry::KeyAndValuesLen(key, values_len));
        };

        // 6. values
        let (values, buf) = match <[V]>::ref_from_prefix_with_elems(buf, values_len as usize) {
            Ok(v) => v,
            Err(ConvertError::Alignment(_)) => {
                return Err(read_err("Values are not properly aligned"));
            }
            Err(ConvertError::Size(_)) => {
                return Ok(MaybeIncompleteEntry::KeyAndValuesLen(key, values_len));
            }
        };

        Ok(MaybeIncompleteEntry::KeyAndValues(key, values, buf))
    }
}

fn align_slice_to(alignment: usize, data: &[u8]) -> Option<&[u8]> {
    data.get(data.as_ptr().align_offset(alignment)..)
}
