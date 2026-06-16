//! [`MmapHashMap`] implementation.

use std::io::{self, Cursor};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;

use memmap2::Mmap;
use ph::fmph::Function;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{BucketOffset, Header, Key, ReadResult, ValuesLen, read_err};
use crate::mmap::{AdviceSetting, Madviseable, open_read_mmap};

/// On-disk hash map backed by a memory-mapped file.
///
/// The layout of the memory-mapped file is as follows:
///
/// | header     | phf | padding       | alignment | buckets | entries   |
/// |------------|-----|---------------|-----------|---------|-----------|
/// | [`Header`] |     | `u8[0..4095]` |  `u8[]`   | `u32[]` | See below |
///
/// ## Entry format for the `str` key
///
/// | key    | `'\0xff'` | padding | values_len | padding | values |
/// |--------|-----------|---------|------------|---------|--------|
/// | `u8[]` | `u8`      | `u8[]`  | `u32`      | `u8[]`  | `V[]`  |
///
/// ## Entry format for the `i64` key
///
/// | key   | values_len | padding | values |
/// |-------|------------|---------|--------|
/// | `i64` | `u32`      | `u8[]`  | `V[]`  |
pub struct MmapHashMap<K: ?Sized, V: Sized + FromBytes + Immutable + IntoBytes + KnownLayout> {
    mmap: Mmap,
    header: Header,
    phf: Function,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
}

pub const BUCKET_OFFSET_OVERHEAD: usize = size_of::<BucketOffset>();

/// Overhead of reading a bucket in mmap hashmap.
const SIZE_OF_LENGTH_FIELD: usize = size_of::<u32>();
const SIZE_OF_KEY: usize = size_of::<u64>();

/// How many bytes we need to read from disk to locate an entry.
pub const READ_ENTRY_OVERHEAD: usize = SIZE_OF_LENGTH_FIELD + SIZE_OF_KEY + BUCKET_OFFSET_OVERHEAD;

impl<K: Key + ?Sized, V: Sized + FromBytes + Immutable + IntoBytes + KnownLayout>
    MmapHashMap<K, V>
{
    const VALUES_LEN_SIZE: usize = size_of::<ValuesLen>();
    const VALUE_SIZE: usize = size_of::<V>();

    const fn values_len_size_with_padding() -> usize {
        Self::VALUES_LEN_SIZE.next_multiple_of(Self::VALUE_SIZE)
    }

    /// Load the hash map from file.
    pub fn open(path: &Path, populate: bool) -> io::Result<Self> {
        let mmap = open_read_mmap(path, AdviceSetting::Global, populate)?;

        let (header, _) =
            Header::read_from_prefix(mmap.as_ref()).map_err(|_| io::ErrorKind::InvalidData)?;

        if header.key_type != K::NAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Key type mismatch",
            ));
        }

        let phf = Function::read(&mut Cursor::new(
            &mmap
                .get(size_of::<Header>()..header.buckets_pos as usize)
                .ok_or(io::ErrorKind::InvalidData)?,
        ))?;

        Ok(MmapHashMap {
            mmap,
            header,
            phf,
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        })
    }

    pub fn keys_count(&self) -> usize {
        self.header.buckets_count as usize
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        (0..self.keys_count()).filter_map(|i| match self.get_entry(i) {
            Ok(entry) => match K::from_bytes(entry) {
                ReadResult::Ok(entry) => Some(entry),
                ReadResult::Incomplete => None,
                ReadResult::Invalid(error) => {
                    debug_assert!(false, "Error reading key for entry {i}: {error}");
                    log::error!("Error reading key for entry {i}: {error}");
                    None
                }
            },
            Err(err) => {
                debug_assert!(false, "Error reading entry for key {i}: {err}");
                log::error!("Error reading entry for key {i}: {err}");
                None
            }
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &[V])> {
        (0..self.keys_count()).filter_map(|i| {
            // NOTE: errors are discarded
            let entry = self.get_entry(i).ok()?;
            let ReadResult::Ok(key) = K::from_bytes(entry) else {
                return None;
            };
            let values = Self::get_values_from_entry(entry, key).ok()?;
            Some((key, values))
        })
    }

    /// Get the values associated with the `key`.
    pub fn get(&self, key: &K) -> io::Result<Option<&[V]>> {
        let Some(hash) = self.phf.get(key) else {
            return Ok(None);
        };

        let entry = self.get_entry(hash as usize)?;

        if !key.matches(entry) {
            return Ok(None);
        }

        Ok(Some(Self::get_values_from_entry(entry, key)?))
    }

    fn get_values_from_entry<'a>(entry: &'a [u8], key: &K) -> io::Result<&'a [V]> {
        // ## Entry format for the `i64` key
        //
        // | key   | values_len | padding | values |
        // |-------|------------|---------|--------|
        // | `i64` | `u32`      | u8[]    | `V[]`  |

        let key_size = key.write_bytes();
        let key_size_with_padding = key_size.next_multiple_of(Self::VALUE_SIZE);

        let entry = entry.get(key_size_with_padding..).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Can't read entry from mmap, \
                         key_size_with_padding {key_size_with_padding} is out of bounds"
                ),
            )
        })?;

        let (values_len, _) = ValuesLen::read_from_prefix(entry).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Can't read values_len from mmap",
            )
        })?;

        let values_from = Self::values_len_size_with_padding();
        let values_to = values_from + values_len as usize * Self::VALUE_SIZE;

        let entry = entry.get(values_from..values_to).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Can't read values from mmap, relative range: {values_from}:{values_to}"),
            )
        })?;

        let result = <[V]>::ref_from_bytes(entry).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Can't convert mmap range into slice",
            )
        })?;
        Ok(result)
    }

    fn get_entry(&self, index: usize) -> io::Result<&[u8]> {
        // Absolute position of the bucket array in the mmap.
        let bucket_from = self.header.buckets_pos as usize;
        let bucket_to =
            bucket_from + self.header.buckets_count as usize * size_of::<BucketOffset>();

        let bucket_val = self
            .mmap
            .get(bucket_from..bucket_to)
            .and_then(|b| <[BucketOffset]>::ref_from_bytes(b).ok())
            .and_then(|buckets| buckets.get(index).copied())
            .ok_or_else(|| {
                read_err(format!(
                    "Can't read bucket from mmap, pos: {bucket_from}:{bucket_to}"
                ))
            })?;

        let entry_start = self.header.buckets_pos as usize
            + self.header.buckets_count as usize * size_of::<BucketOffset>()
            + bucket_val as usize;

        self.mmap.get(entry_start..).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Can't read entry from mmap, bucket_val {entry_start} is out of bounds"),
            )
        })
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> io::Result<()> {
        self.mmap.populate();
        Ok(())
    }

    /// Hint to the OS that pages backing this mmap can be reclaimed.
    pub fn clear_cache(&self) -> io::Result<()> {
        let Self {
            mmap,
            header: _,
            phf: _,
            _phantom_key,
            _phantom_value,
        } = self;
        mmap.clear_cache();
        Ok(())
    }
}
