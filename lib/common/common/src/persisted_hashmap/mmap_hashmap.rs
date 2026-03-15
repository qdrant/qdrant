#[cfg(any(test, feature = "testing"))]
use std::collections::{BTreeMap, BTreeSet};
use std::hash::Hash;
use std::io::{self, Cursor, Write};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;

use fs_err::File;
use memmap2::Mmap;
use ph::fmph::Function;
#[cfg(any(test, feature = "testing"))]
use rand::RngExt;
#[cfg(any(test, feature = "testing"))]
use rand::rngs::StdRng;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::mmap::{AdviceSetting, Madviseable, open_read_mmap};
use crate::persisted_hashmap::keys::Key;
use crate::zeros::WriteZerosExt as _;

type ValuesLen = u32;

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

#[repr(C)]
#[derive(Copy, Clone, Debug, FromBytes, Immutable, IntoBytes, KnownLayout)]
struct Header {
    key_type: [u8; 8],
    buckets_pos: u64,
    buckets_count: u64,
}

const PADDING_SIZE: usize = 4096;

pub const BUCKET_OFFSET_OVERHEAD: usize = size_of::<BucketOffset>();

/// Overhead of reading a bucket in mmap hashmap.
const SIZE_OF_LENGTH_FIELD: usize = size_of::<u32>();
const SIZE_OF_KEY: usize = size_of::<u64>();

/// How many bytes we need to read from disk to locate an entry.
pub const READ_ENTRY_OVERHEAD: usize = SIZE_OF_LENGTH_FIELD + SIZE_OF_KEY + BUCKET_OFFSET_OVERHEAD;

type BucketOffset = u64;

impl<K: Key + ?Sized, V: Sized + FromBytes + Immutable + IntoBytes + KnownLayout>
    MmapHashMap<K, V>
{
    /// Save `map` contents to `path`.
    pub fn create<'a>(
        path: &Path,
        map: impl Iterator<Item = (&'a K, impl ExactSizeIterator<Item = V>)> + Clone,
    ) -> io::Result<()>
    where
        K: 'a,
    {
        let keys_vec: Vec<_> = map.clone().map(|(k, _)| k).collect();
        let keys_count = keys_vec.len();
        let phf = Function::from(keys_vec);

        // == First pass ==

        let mut file_size = 0;
        // 1. Header
        file_size += size_of::<Header>();

        // 2. PHF
        file_size += phf.write_bytes();

        // 3. Padding
        let padding_len = file_size.next_multiple_of(PADDING_SIZE) - file_size;
        file_size += padding_len;

        // 4. Buckets
        let buckets_size = keys_count * size_of::<BucketOffset>();
        let bucket_align = buckets_size.next_multiple_of(K::ALIGN) - buckets_size;
        file_size += bucket_align;
        // Important: Bucket Position points after the alignment for backward compatibility.
        let buckets_pos = file_size;
        file_size += buckets_size;

        // 5. Data
        let mut buckets = vec![0 as BucketOffset; keys_count];
        let mut last_bucket = 0usize;
        for (k, v) in map.clone() {
            last_bucket = last_bucket.next_multiple_of(K::ALIGN);
            buckets[phf.get(k).expect("Key not found in phf") as usize] =
                last_bucket as BucketOffset;
            last_bucket += Self::entry_bytes(k, v.len());
        }
        file_size += last_bucket;
        _ = file_size;

        // == Second pass ==
        let (file, temp_path) = tempfile::Builder::new()
            .prefix(path.file_name().ok_or(io::ErrorKind::InvalidInput)?)
            .tempfile_in(path.parent().ok_or(io::ErrorKind::InvalidInput)?)?
            .into_parts();
        let file = File::from_parts::<&Path>(file, temp_path.as_ref());
        let mut bufw = io::BufWriter::new(file);

        // 1. Header
        let header = Header {
            key_type: K::NAME,
            buckets_pos: buckets_pos as u64,
            buckets_count: keys_count as u64,
        };
        bufw.write_all(header.as_bytes())?;

        // 2. PHF
        phf.write(&mut bufw)?;

        // 3. Padding
        bufw.write_zeros(padding_len)?;

        // 4. Buckets
        // Align the buckets to `K::ALIGN`, to make sure Entry.key is aligned.
        bufw.write_zeros(bucket_align)?;
        bufw.write_all(buckets.as_bytes())?;

        // 5. Data
        let mut pos = 0usize;
        for (key, values) in map {
            let next_pos = pos.next_multiple_of(K::ALIGN);
            if next_pos > pos {
                bufw.write_zeros(next_pos - pos)?;
                pos = next_pos;
            }

            let entry_size = Self::entry_bytes(key, values.len());
            pos += entry_size;

            key.write(&mut bufw)?;
            bufw.write_zeros(Self::key_padding_bytes(key))?;
            bufw.write_all((values.len() as ValuesLen).as_bytes())?;
            bufw.write_zeros(Self::values_len_padding_bytes())?;
            for i in values {
                bufw.write_all(i.as_bytes())?;
            }
        }

        // Explicitly flush write buffer so we can catch IO errors
        bufw.flush()?;
        let file = bufw.into_inner().unwrap();

        file.sync_all()?;
        drop(file);
        temp_path.persist(path)?;

        Ok(())
    }

    const VALUES_LEN_SIZE: usize = size_of::<ValuesLen>();
    const VALUE_SIZE: usize = size_of::<V>();

    fn key_size_with_padding(key: &K) -> usize {
        let key_size = key.write_bytes();
        key_size.next_multiple_of(Self::VALUE_SIZE)
    }

    fn key_padding_bytes(key: &K) -> usize {
        let key_size = key.write_bytes();
        key_size.next_multiple_of(Self::VALUE_SIZE) - key_size
    }

    const fn values_len_size_with_padding() -> usize {
        Self::VALUES_LEN_SIZE.next_multiple_of(Self::VALUE_SIZE)
    }

    const fn values_len_padding_bytes() -> usize {
        Self::VALUES_LEN_SIZE.next_multiple_of(Self::VALUE_SIZE) - Self::VALUES_LEN_SIZE
    }

    /// Return the total size of the entry in bytes, including: key, values_len, values, all with
    /// padding.
    fn entry_bytes(key: &K, values_len: usize) -> usize {
        Self::key_size_with_padding(key)
            + Self::values_len_size_with_padding()
            + values_len * Self::VALUE_SIZE
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
            Ok(entry) => K::from_bytes(entry),
            Err(err) => {
                debug_assert!(false, "Error reading entry for key {i}: {err}");
                log::error!("Error reading entry for key {i}: {err}");
                None
            }
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &[V])> {
        (0..self.keys_count()).filter_map(|i| {
            let entry = self.get_entry(i).ok()?;
            let key = K::from_bytes(entry)?;
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
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Can't read bucket from mmap, pos: {bucket_from}:{bucket_to}"),
                )
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
}

#[cfg(any(test, feature = "testing"))]
pub fn gen_map<T: Eq + Ord + Hash>(
    rng: &mut StdRng,
    gen_key: impl Fn(&mut StdRng) -> T,
    count: usize,
) -> BTreeMap<T, BTreeSet<u32>> {
    let mut map = BTreeMap::new();

    for _ in 0..count {
        let key = repeat_until(|| gen_key(rng), |key| !map.contains_key(key));
        let set = (0..rng.random_range(1..=100))
            .map(|_| rng.random_range(0..=1000))
            .collect::<BTreeSet<_>>();
        map.insert(key, set);
    }

    map
}

#[cfg(any(test, feature = "testing"))]
pub fn gen_ident(rng: &mut StdRng) -> String {
    (0..rng.random_range(5..=32))
        .map(|_| rng.random_range(b'a'..=b'z') as char)
        .collect()
}

#[cfg(any(test, feature = "testing"))]
pub fn repeat_until<T>(mut f: impl FnMut() -> T, cond: impl Fn(&T) -> bool) -> T {
    std::iter::from_fn(|| Some(f())).find(|v| cond(v)).unwrap()
}
