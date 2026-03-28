use std::io::{self, Cursor};
use std::marker::PhantomData;
use std::mem::size_of;
use std::path::Path;

use ph::fmph::Function;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::bucket_offsets::BucketOffsets;
use crate::generic_consts::{Random, Sequential};
use crate::persisted_hashmap::keys::Key;
use crate::universal_io::{OpenOptions, ReadRange, UniversalRead};

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
        let reader: R = UniversalRead::<u8>::open(path, options).map_err(io_err)?;

        // 1. Read header.
        let header_bytes = reader
            .read::<Sequential>(ReadRange {
                byte_offset: 0,
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
            .read::<Sequential>(ReadRange {
                byte_offset: phf_region_start,
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
        let Some((entry_start, header_size, values_len)) = self.lookup_entry_header(key)? else {
            return Ok(None);
        };

        if values_len == 0 {
            return Ok(Some(f(&[])));
        }

        let values_start = entry_start + header_size as u64;
        let values_bytes = self
            .reader
            .read::<Random>(ReadRange {
                byte_offset: values_start,
                length: u64::from(values_len) * Self::VALUE_SIZE as u64,
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
        let Some((_entry_start, _header_size, values_len)) = self.lookup_entry_header(key)? else {
            return Ok(None);
        };
        Ok(Some(values_len as usize))
    }

    // ── Batch lookup ────────────────────────────────────────────────────

    /// Look up multiple keys at once, returning results in the same order as the input.
    ///
    /// This is more efficient than calling [`get_with`](Self::get_with) in a loop because
    /// IO reads are sorted by file position and batched for sequential access.
    ///
    /// Three batched IO phases are performed:
    /// 1. Read bucket offsets (sorted by bucket index).
    /// 2. Read entry headers (sorted by entry offset) and verify keys.
    /// 3. Read values for matched entries (sorted by entry offset).
    pub fn get_with_batch<'k, T>(
        &self,
        keys: &[&'k K],
        mut f: impl FnMut(&K, &[V]) -> T,
    ) -> io::Result<Vec<Option<T>>>
    where
        K: 'k,
    {
        let (idx_mapping, bucket_ids): (Vec<_>, Vec<_>) = keys
            .iter()
            .enumerate()
            .filter_map(|(idx, key)| self.phf.get(key).map(|bucket_id| (idx, bucket_id)))
            .unzip();

        let entry_offsets = self.batch_resolve_bucket_offsets(bucket_ids)?;

        let (idx_mapping, values_offsets, values_lens) =
            self.batch_read_entry_headers(keys, idx_mapping, &entry_offsets)?;

        let results =
            self.batch_read_values(keys, idx_mapping, values_offsets, values_lens, &mut f)?;

        Ok(results)
    }

    // ── Iteration ───────────────────────────────────────────────────────

    /// Iterate over all entries, calling `f` for each `(key, values)` pair.
    ///
    /// Reads bucket offsets in bulk, sorts them for sequential access, then reads
    /// entries in batches of [`ENTRY_BATCH_SIZE`] to bound memory usage.
    pub fn for_each_entry(&self, mut f: impl FnMut(&K, &[V])) -> io::Result<()> {
        let bucket_count = self.header.buckets_count as usize;
        if bucket_count == 0 {
            return Ok(());
        }

        let buckets = self.read_all_bucket_offsets()?;
        let sorted_offsets = buckets.to_sorted_vec();

        let file_len = self.reader.len().map_err(io_err)?;
        let entries_region_len = file_len - self.entries_start;

        const ENTRY_BATCH_SIZE: usize = 64;

        for chunk_start in (0..sorted_offsets.len()).step_by(ENTRY_BATCH_SIZE) {
            let chunk_end = (chunk_start + ENTRY_BATCH_SIZE).min(sorted_offsets.len());

            let range_start = sorted_offsets[chunk_start];
            let range_end = sorted_offsets
                .get(chunk_end)
                .copied()
                .unwrap_or(entries_region_len);

            let chunk_data = self
                .reader
                .read::<Sequential>(ReadRange {
                    byte_offset: self.entries_start + range_start,
                    length: range_end - range_start,
                })
                .map_err(io_err)?;

            for &offset in &sorted_offsets[chunk_start..chunk_end] {
                let local_offset = (offset - range_start) as usize;
                let entry = chunk_data.get(local_offset..).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "Entry offset out of bounds")
                })?;

                let Some(key) = K::from_bytes(entry) else {
                    debug_assert!(false, "Error reading key");
                    log::error!("Error reading key");
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
        }

        Ok(())
    }

    /// Iterate over all keys without reading the values.
    ///
    /// Reads bucket offsets in one bulk IO, then uses `read_batch` to fetch only the
    /// key-header portion of each entry (sorted by file offset for sequential access).
    /// Values data is never touched.
    ///
    /// For variable-length keys (e.g. `str`) a capped initial read is used; entries
    /// whose keys are longer than the cap are retried with the full entry size.
    pub fn for_each_key(&self, mut f: impl FnMut(&K)) -> io::Result<()> {
        let bucket_count = self.header.buckets_count as usize;
        if bucket_count == 0 {
            return Ok(());
        }

        // 1. Read all bucket offsets at once.
        let buckets = self.read_all_bucket_offsets()?;

        // Sort offsets by file position for sequential IO.
        let file_len = self.reader.len().map_err(io_err)?;
        let entries_region_len = file_len - self.entries_start;

        let sorted_offsets = buckets.to_sorted_vec();

        // 2. Build capped read ranges — read just enough for the key header.
        //
        // For fixed-size keys the cap is exact. For variable-length keys (str) we
        // use KEY_READ_CAP bytes; any entry whose key is longer will be retried.
        const KEY_READ_CAP: u64 = 512;

        let key_size = K::fixed_size().unwrap_or(KEY_READ_CAP);

        let ranges = sorted_offsets.iter().enumerate().map(|(i, &offset)| {
            let next_entry = sorted_offsets
                .get(i + 1)
                .copied()
                .unwrap_or(entries_region_len);
            let available = next_entry - offset;
            ReadRange {
                byte_offset: self.entries_start + offset,
                length: available.min(key_size),
            }
        });

        // 3. Batch-read the key headers (sequential order).
        let mut retry_indices: Vec<usize> = Vec::new();

        self.reader
            .read_batch::<Random>(ranges, |idx, data| {
                match K::from_bytes(data) {
                    Some(key) => f(key),
                    None => retry_indices.push(idx),
                }
                Ok(())
            })
            .map_err(io_err)?;

        // 4. Retry any truncated keys with the full entry size.
        if !retry_indices.is_empty() {
            let retry_ranges = retry_indices.iter().map(|&idx| {
                let offset = sorted_offsets[idx];
                let next_entry = sorted_offsets
                    .get(idx + 1)
                    .copied()
                    .unwrap_or(entries_region_len);
                ReadRange {
                    byte_offset: self.entries_start + offset,
                    length: next_entry - offset,
                }
            });

            self.reader
                .read_batch::<Random>(retry_ranges, |_idx, data| {
                    if let Some(key) = K::from_bytes(data) {
                        f(key);
                    } else {
                        debug_assert!(false, "Failed to read key even with full entry size");
                        log::error!("Failed to read key even with full entry size");
                    }
                    Ok(())
                })
                .map_err(io_err)?;
        }

        Ok(())
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

    /// Hash `key`, read the bucket offset and entry header, verify the key matches,
    /// and return `(entry_start, header_size, values_len)`.
    ///
    /// Returns `Ok(None)` if the PHF has no mapping or the stored key doesn't match.
    /// Two IO reads are performed: bucket offset and entry header.
    fn lookup_entry_header(&self, key: &K) -> io::Result<Option<(u64, usize, u32)>> {
        let Some(hash) = self.phf.get(key) else {
            return Ok(None);
        };

        let entry_offset = self.read_bucket_offset(hash as usize)?;
        let entry_start = self.entries_start + entry_offset;
        let key_size_with_padding = Self::key_size_with_padding(key);
        let header_size = key_size_with_padding + Self::values_len_size_with_padding();

        let entry_header = self
            .reader
            .read::<Random>(ReadRange {
                byte_offset: entry_start,
                length: header_size as u64,
            })
            .map_err(io_err)?;

        if !key.matches(&entry_header) {
            return Ok(None);
        }

        let values_len = Self::parse_values_len(&entry_header[key_size_with_padding..])?;
        Ok(Some((entry_start, header_size, values_len)))
    }

    /// Read all bucket offsets in one sequential IO.
    fn read_all_bucket_offsets(&self) -> io::Result<BucketOffsets<'_>> {
        let bucket_count = self.header.buckets_count as usize;
        let bytes = self
            .reader
            .read::<Sequential>(ReadRange {
                byte_offset: self.header.buckets_pos,
                length: (bucket_count * size_of::<BucketOffset>()) as u64,
            })
            .map_err(io_err)?;
        Ok(BucketOffsets::new(bytes))
    }

    fn read_bucket_offset(&self, index: usize) -> io::Result<u64> {
        let byte_offset =
            self.header.buckets_pos + (index as u64) * size_of::<BucketOffset>() as u64;
        let bytes = self
            .reader
            .read::<Random>(ReadRange {
                byte_offset,
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
        let (len, _) = ValuesLen::read_from_prefix(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Can't read values_len"))?;
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
        debug_assert!(
            false,
            "Values bytes not aligned for zero-copy; falling back to copy"
        );
        let values = Self::copy_values_from_bytes(bytes)?;
        Ok(f(&values))
    }

    /// Copy bytes into a `Vec<V>` one element at a time (alignment-safe).
    fn copy_values_from_bytes(bytes: &[u8]) -> io::Result<Vec<V>> {
        if Self::VALUE_SIZE == 0 || !bytes.len().is_multiple_of(Self::VALUE_SIZE) {
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

    // ── Batch-lookup helpers ───────────────────────────────────────────

    /// Phase 1: resolve bucket index → entry offset for each lookup.
    fn batch_resolve_bucket_offsets(&self, bucket_ids: Vec<u64>) -> io::Result<Vec<u64>> {
        let mut entry_offsets: Vec<u64> = vec![0; bucket_ids.len()];

        let ranges = bucket_ids.into_iter().map(|bucket_idx| ReadRange {
            byte_offset: self.header.buckets_pos + bucket_idx * size_of::<BucketOffset>() as u64,
            length: size_of::<BucketOffset>() as u64,
        });

        self.reader
            .read_batch::<Random>(ranges, |idx, data| {
                let (offset, _) = BucketOffset::read_from_prefix(data)
                    .map_err(|e| uio_data_err(e.to_string()))?;
                entry_offsets[idx] = offset;
                Ok(())
            })
            .map_err(io_err)?;

        Ok(entry_offsets)
    }

    /// Phase 2: read entry headers, verify keys, and parse values_len.
    fn batch_read_entry_headers(
        &self,
        keys: &[&K],
        idx_mapping: Vec<usize>,
        entry_offsets: &[u64],
    ) -> io::Result<(Vec<usize>, Vec<u64>, Vec<u32>)> {
        let mut new_idx_mapping = Vec::with_capacity(entry_offsets.len());
        let mut values_offsets = Vec::with_capacity(entry_offsets.len());
        let mut values_lens = Vec::with_capacity(entry_offsets.len());

        let ranges = entry_offsets.iter().enumerate().map(|(idx, entry_offset)| {
            let key = keys[idx_mapping[idx]];
            let header_size =
                Self::key_size_with_padding(key) + Self::values_len_size_with_padding();
            ReadRange {
                byte_offset: self.entries_start + *entry_offset,
                length: header_size as u64,
            }
        });

        self.reader
            .read_batch::<Random>(ranges, |idx, data| {
                let key_id = idx_mapping[idx];
                let key = keys[key_id];
                let header_size =
                    Self::key_size_with_padding(key) + Self::values_len_size_with_padding();
                let entry_offset = entry_offsets[idx];

                if !key.matches(data) {
                    return Ok(());
                }
                let key_pad = Self::key_size_with_padding(key);
                let vl_bytes = data
                    .get(key_pad..)
                    .ok_or_else(|| uio_data_err("Entry too short for values_len"))?;
                let (vl, _) = ValuesLen::read_from_prefix(vl_bytes)
                    .map_err(|e| uio_data_err(e.to_string()))?;

                let values_offset = self.entries_start + entry_offset + header_size as u64;

                values_offsets.push(values_offset);
                values_lens.push(vl);
                new_idx_mapping.push(key_id);
                Ok(())
            })
            .map_err(io_err)?;

        Ok((new_idx_mapping, values_offsets, values_lens))
    }

    /// Phase 3: read values for matched entries and populate results.
    fn batch_read_values<T>(
        &self,
        keys: &[&K],
        idx_mapping: Vec<usize>,
        values_offsets: Vec<u64>,
        values_lens: Vec<u32>,
        f: &mut impl FnMut(&K, &[V]) -> T,
    ) -> io::Result<Vec<Option<T>>> {
        let mut results: Vec<Option<T>> = Vec::with_capacity(keys.len());
        results.resize_with(keys.len(), || None);

        // Handle zero-length value matches.
        for (idx, &values_len) in values_lens.iter().enumerate() {
            if values_len == 0 {
                let orig_idx = idx_mapping[idx];
                results[orig_idx] = Some(f(keys[orig_idx], &[]));
            }
        }

        if values_lens.is_empty() {
            return Ok(results);
        }

        let ranges =
            values_offsets
                .into_iter()
                .zip(values_lens)
                .map(|(values_offset, values_len)| ReadRange {
                    byte_offset: values_offset,
                    length: u64::from(values_len) * Self::VALUE_SIZE as u64,
                });

        self.reader
            .read_batch::<Sequential>(ranges, |idx, data| {
                let key_id = idx_mapping[idx];
                let key = keys[key_id];
                Self::with_values(data, |values| {
                    results[key_id] = Some(f(key, values));
                })
                .map_err(crate::universal_io::UniversalIoError::Io)
            })
            .map_err(io_err)?;

        Ok(results)
    }
}

fn io_err(e: crate::universal_io::UniversalIoError) -> io::Error {
    match e {
        crate::universal_io::UniversalIoError::Io(e) => e,
        other => io::Error::other(other),
    }
}

fn uio_data_err(
    msg: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> crate::universal_io::UniversalIoError {
    crate::universal_io::UniversalIoError::Io(io::Error::new(io::ErrorKind::InvalidData, msg))
}
