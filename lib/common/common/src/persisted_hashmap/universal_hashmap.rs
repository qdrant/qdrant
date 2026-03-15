use std::io::{self, Cursor};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;

use ph::fmph::Function;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::persisted_hashmap::keys::Key;
use crate::universal_io::{ElementsRange, OpenOptions, UniversalRead};

type ValuesLen = u32;
type BucketOffset = u64;

/// Same on-disk layout as [`super::mmap_hashmap`]'s header.
#[repr(C)]
#[derive(Copy, Clone, Debug, FromBytes, Immutable, IntoBytes, KnownLayout)]
struct Header {
    key_type: [u8; 8],
    buckets_pos: u64,
    buckets_count: u64,
}

/// How many bytes a single lookup conceptually touches for hardware-counter accounting.
pub const READ_ENTRY_OVERHEAD: usize = super::mmap_hashmap::READ_ENTRY_OVERHEAD;

/// On-disk hash map accessed via [`UniversalRead`].
///
/// Uses the same on-disk layout as [`super::mmap_hashmap::MmapHashMap`] and can open files
/// created by [`MmapHashMap::create`](super::mmap_hashmap::MmapHashMap::create).
///
/// Unlike the mmap variant, this implementation does not return borrowed slices into the
/// underlying storage. Instead it reads data on demand through the universal IO interface.
///
/// ## Access patterns
///
/// | Method              | IO reads | Allocations | Notes                              |
/// |---------------------|----------|-------------|------------------------------------|
/// | [`get_with`]        | 3        | 0–1         | Callback receives `&[V]` directly  |
/// | [`get`]             | 3        | 1           | Returns `Vec<V>`                   |
/// | [`get_values_count`]| 2        | 0           | Skips reading values entirely      |
/// | [`for_each_entry`]  | 2        | 0–N         | Bulk reads buckets + entries       |
///
/// [`get_with`]: Self::get_with
/// [`get`]: Self::get
/// [`get_values_count`]: Self::get_values_count
/// [`for_each_entry`]: Self::for_each_entry
pub struct UniversalHashMap<
    K: ?Sized,
    V: Sized + FromBytes + Immutable + IntoBytes + KnownLayout,
    R: UniversalRead<u8>,
> {
    reader: R,
    header: Header,
    phf: Function,
    /// Absolute byte offset where entry data begins (right after the bucket offsets array).
    entries_start: u64,
    _phantom_key: PhantomData<K>,
    _phantom_value: PhantomData<V>,
}

impl<
        K: Key + ?Sized,
        V: Sized + Copy + FromBytes + Immutable + IntoBytes + KnownLayout,
        R: UniversalRead<u8>,
    > UniversalHashMap<K, V, R>
{
    const VALUES_LEN_SIZE: usize = size_of::<ValuesLen>();
    const VALUE_SIZE: usize = size_of::<V>();

    /// Open the hash map from a file previously created by
    /// [`MmapHashMap::create`](super::mmap_hashmap::MmapHashMap::create).
    pub fn open(path: impl AsRef<Path>, options: OpenOptions) -> io::Result<Self> {
        let reader = R::open(path, options).map_err(io_err)?;

        // 1. Read header.
        let header_bytes = reader
            .read::<true>(ElementsRange {
                start: 0,
                length: size_of::<Header>() as u64,
            })
            .map_err(io_err)?;
        let (header, _) = Header::read_from_prefix(&header_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid header"))?;

        if header.key_type != K::NAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Key type mismatch",
            ));
        }

        // 2. Read PHF. The region between the header and buckets_pos contains the
        //    serialised PHF followed by padding; `Function::read` consumes only what
        //    it needs and ignores trailing bytes.
        let phf_region_start = size_of::<Header>() as u64;
        let phf_region_len = header
            .buckets_pos
            .checked_sub(phf_region_start)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "buckets_pos before header end")
            })?;
        let phf_bytes = reader
            .read::<true>(ElementsRange {
                start: phf_region_start,
                length: phf_region_len,
            })
            .map_err(io_err)?;
        let phf = Function::read(&mut Cursor::new(&*phf_bytes))?;

        let entries_start =
            header.buckets_pos + header.buckets_count * size_of::<BucketOffset>() as u64;

        Ok(Self {
            reader,
            header,
            phf,
            entries_start,
            _phantom_key: PhantomData,
            _phantom_value: PhantomData,
        })
    }

    /// Number of distinct keys stored in the hash map.
    pub fn keys_count(&self) -> usize {
        self.header.buckets_count as usize
    }

    // ── Single-key lookup ───────────────────────────────────────────────

    /// Look up the values associated with `key`, passing them to `f`.
    ///
    /// This is the most efficient lookup method: the callback receives a `&[V]` that may
    /// reference the backing storage directly (zero-copy for mmap-based readers) or a
    /// temporary read buffer.
    ///
    /// Three IO reads are performed: bucket offset, entry header, and values.
    pub fn get_with<T>(&self, key: &K, f: impl FnOnce(&[V]) -> T) -> io::Result<Option<T>> {
        let Some(hash) = self.phf.get(key) else {
            return Ok(None);
        };

        // 1. Bucket offset → entry position.
        let entry_offset = self.read_bucket_offset(hash as usize)?;

        // 2. Entry header: key (with padding) + values_len (with padding).
        let entry_start = self.entries_start + entry_offset;
        let key_size_with_padding = Self::key_size_with_padding(key);
        let header_size = key_size_with_padding + Self::values_len_size_with_padding();

        let entry_header = self
            .reader
            .read::<false>(ElementsRange {
                start: entry_start,
                length: header_size as u64,
            })
            .map_err(io_err)?;

        if !key.matches(&entry_header) {
            return Ok(None);
        }

        let values_len = Self::parse_values_len(&entry_header[key_size_with_padding..])?;
        if values_len == 0 {
            return Ok(Some(f(&[])));
        }

        // 3. Values.
        let values_start = entry_start + header_size as u64;
        let values_bytes = self
            .reader
            .read::<false>(ElementsRange {
                start: values_start,
                length: values_len as u64 * Self::VALUE_SIZE as u64,
            })
            .map_err(io_err)?;

        Self::with_values(&values_bytes, f).map(Some)
    }

    /// Convenience wrapper that returns an owned `Vec<V>`.
    pub fn get(&self, key: &K) -> io::Result<Option<Vec<V>>> {
        self.get_with(key, |values| values.to_vec())
    }

    /// Return the number of values for `key` *without* reading the values themselves.
    ///
    /// Only two IO reads are performed: bucket offset and entry header.
    pub fn get_values_count(&self, key: &K) -> io::Result<Option<usize>> {
        let Some(hash) = self.phf.get(key) else {
            return Ok(None);
        };

        let entry_offset = self.read_bucket_offset(hash as usize)?;
        let entry_start = self.entries_start + entry_offset;
        let key_size_with_padding = Self::key_size_with_padding(key);
        let header_size = key_size_with_padding + Self::values_len_size_with_padding();

        let entry_header = self
            .reader
            .read::<false>(ElementsRange {
                start: entry_start,
                length: header_size as u64,
            })
            .map_err(io_err)?;

        if !key.matches(&entry_header) {
            return Ok(None);
        }

        let values_len = Self::parse_values_len(&entry_header[key_size_with_padding..])?;
        Ok(Some(values_len as usize))
    }

    // ── Iteration ───────────────────────────────────────────────────────

    /// Iterate over all entries, calling `f` for each `(key, values)` pair.
    ///
    /// Performs two bulk IO reads (bucket offsets + full entries region), then iterates
    /// in-memory.
    pub fn for_each_entry(&self, mut f: impl FnMut(&K, &[V])) -> io::Result<()> {
        let bucket_count = self.header.buckets_count as usize;
        if bucket_count == 0 {
            return Ok(());
        }

        // Read all bucket offsets at once.
        let buckets_bytes = self
            .reader
            .read::<true>(ElementsRange {
                start: self.header.buckets_pos,
                length: (bucket_count * size_of::<BucketOffset>()) as u64,
            })
            .map_err(io_err)?;

        // Read the entire entries region.
        let file_len = self.reader.len().map_err(io_err)?;
        let entries_len = file_len - self.entries_start;
        if entries_len == 0 {
            return Ok(());
        }
        let entries_data = self
            .reader
            .read::<true>(ElementsRange {
                start: self.entries_start,
                length: entries_len,
            })
            .map_err(io_err)?;

        for i in 0..bucket_count {
            let offset_start = i * size_of::<BucketOffset>();
            let (offset, _) = BucketOffset::read_from_prefix(&buckets_bytes[offset_start..])
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Can't read bucket offset")
                })?;

            let entry = entries_data.get(offset as usize..).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Entry offset out of bounds")
            })?;

            let Some(key) = K::from_bytes(entry) else {
                debug_assert!(false, "Error reading key for bucket {i}");
                log::error!("Error reading key for bucket {i}");
                continue;
            };

            let key_size_with_padding = Self::key_size_with_padding(key);
            let values_len = Self::parse_values_len(
                entry.get(key_size_with_padding..).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "Entry too short for values_len")
                })?,
            )?;

            let values_from = key_size_with_padding + Self::values_len_size_with_padding();
            let values_to = values_from + values_len as usize * Self::VALUE_SIZE;

            let values_bytes = entry.get(values_from..values_to).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Values region out of bounds")
            })?;

            Self::with_values(values_bytes, |values| f(key, values))?;
        }

        Ok(())
    }

    /// Iterate over all keys.
    pub fn for_each_key(&self, mut f: impl FnMut(&K)) -> io::Result<()> {
        self.for_each_entry(|key, _values| f(key))
    }

    // ── Cache management ────────────────────────────────────────────────

    /// Populate the RAM cache for the backing file.
    pub fn populate(&self) -> io::Result<()> {
        self.reader.populate().map_err(io_err)
    }

    /// Evict the backing file data from RAM cache.
    pub fn clear_ram_cache(&self) -> io::Result<()> {
        self.reader.clear_ram_cache().map_err(io_err)
    }

    // ── Private helpers ─────────────────────────────────────────────────

    fn read_bucket_offset(&self, index: usize) -> io::Result<u64> {
        let byte_offset =
            self.header.buckets_pos + (index as u64) * size_of::<BucketOffset>() as u64;
        let bytes = self
            .reader
            .read::<false>(ElementsRange {
                start: byte_offset,
                length: size_of::<BucketOffset>() as u64,
            })
            .map_err(io_err)?;
        let (offset, _) = BucketOffset::read_from_prefix(&bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Can't read bucket offset"))?;
        Ok(offset)
    }

    fn key_size_with_padding(key: &K) -> usize {
        key.write_bytes().next_multiple_of(Self::VALUE_SIZE)
    }

    const fn values_len_size_with_padding() -> usize {
        Self::VALUES_LEN_SIZE.next_multiple_of(Self::VALUE_SIZE)
    }

    fn parse_values_len(bytes: &[u8]) -> io::Result<u32> {
        let (len, _) = ValuesLen::read_from_prefix(bytes).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "Can't read values_len")
        })?;
        Ok(len)
    }

    /// Interpret `bytes` as `&[V]` and pass to the callback.
    ///
    /// Fast path: if `bytes` are properly aligned for `V`, zero-copy reinterpretation.
    /// Slow path: copy into an aligned `Vec<V>` element-by-element.
    fn with_values<T>(bytes: &[u8], f: impl FnOnce(&[V]) -> T) -> io::Result<T> {
        if let Ok(values) = <[V]>::ref_from_bytes(bytes) {
            return Ok(f(values));
        }
        let values = Self::copy_values_from_bytes(bytes)?;
        Ok(f(&values))
    }

    /// Copy bytes into a `Vec<V>` one element at a time (alignment-safe).
    fn copy_values_from_bytes(bytes: &[u8]) -> io::Result<Vec<V>> {
        if Self::VALUE_SIZE == 0 || bytes.len() % Self::VALUE_SIZE != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Values byte length {} is not a multiple of value size {}",
                    bytes.len(),
                    Self::VALUE_SIZE,
                ),
            ));
        }
        bytes
            .chunks_exact(Self::VALUE_SIZE)
            .map(|chunk| {
                V::read_from_bytes(chunk).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Can't read value from bytes")
                })
            })
            .collect()
    }
}

fn io_err(e: crate::universal_io::UniversalIoError) -> io::Error {
    match e {
        crate::universal_io::UniversalIoError::Io(e) => e,
        other => io::Error::new(io::ErrorKind::Other, other),
    }
}
