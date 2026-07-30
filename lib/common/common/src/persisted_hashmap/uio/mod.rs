use std::io::{self, Cursor};
use std::marker::PhantomData;
use std::path::Path;

use ph::fmph::Function;
use random_reader::Request;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{BucketOffset, Header, Key, MaybeIncompleteEntry, MaybeIncompleteEntryKind, read_err};
use crate::aligned_buf::AlignedBuf;
use crate::generic_consts::Sequential;
use crate::iterator_ext::ordering_iterator::OrderingIterator;
use crate::universal_io::{
    CachedReadFs, OpenOptions, ReadRange, TypedStorage, UioResult, UniversalIoError, UniversalRead,
    UniversalReadFs, UserData,
};

mod random_reader;

/// If entries are smaller than that, it's likely more efficient to read them sequentially.
const SEQUENTIAL_READ_THRESHOLD: u64 = 8 * 1024;

/// Covers header + ~380k items worth of phf.
const HEADER_AND_BASIC_PHF_SIZE: u64 = size_of::<Header>() as u64 + 128 * 1024;

/// On-disk hash map accessed via [`UniversalRead`].
pub struct UniversalHashMap<K, V, S>
where
    K: Key + ?Sized,
    V: Sized + Copy + FromBytes + Immutable + IntoBytes + KnownLayout,
    S: UniversalRead,
{
    storage: TypedStorage<S, u8>,
    header: Header,
    phf: Function,
    /// Absolute byte offset where entry data begins (right after the bucket offsets array).
    entries_start: u64,
    average_entry_size: u64,
    phantom: PhantomData<(V, K)>,
}

impl<'key, K, V, S> UniversalHashMap<K, V, S>
where
    K: Key + ?Sized + 'key,
    V: Sized + Copy + FromBytes + Immutable + IntoBytes + KnownLayout,
    S: UniversalRead,
{
    /// Schedule background prefetch of the backing file, ahead of
    /// [`Self::open`].
    pub fn preopen<Fs: CachedReadFs<File = S>>(
        fs: &Fs,
        path: impl AsRef<Path>,
        mut options: OpenOptions,
    ) -> UioResult<()> {
        // Default a lazy open to partially populating the header + a slice of
        // the perfect-hash table, so the map can be opened without a full read.
        options.populate = options.populate.or_partial(0..HEADER_AND_BASIC_PHF_SIZE);

        fs.schedule_prefetch(path.as_ref(), Some(options), None)
    }

    /// Load the hash map from file.
    pub fn open<Fs: UniversalReadFs<File = S>>(
        fs: &Fs,
        path: impl AsRef<Path>,
        options: OpenOptions,
        extra: Fs::OpenExtra,
    ) -> UioResult<Self> {
        let storage = TypedStorage::<S, u8>::open(fs, path, options, extra)?;

        // 1. Read header.
        let header_bytes =
            storage.read(ReadRange::new(0, size_of::<Header>() as u64), Sequential)?;
        let (header, _) = Header::read_from_prefix(&header_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid header"))?;

        if header.key_type != K::NAME {
            return Err(UniversalIoError::from(io::Error::new(
                io::ErrorKind::InvalidData,
                "Key type mismatch",
            )));
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
        let phf_bytes =
            storage.read(ReadRange::new(phf_region_start, phf_region_len), Sequential)?;
        let phf = Function::read(&mut Cursor::new(&*phf_bytes))?;

        let entries_start =
            header.buckets_pos + header.buckets_count * size_of::<BucketOffset>() as u64;

        let average_entry_size = (storage.len()? - entries_start)
            .checked_div(header.buckets_count)
            .unwrap_or(0);

        Ok(UniversalHashMap {
            storage,
            header,
            phf,
            entries_start,
            average_entry_size,
            phantom: PhantomData,
        })
    }

    /// Number of distinct keys stored in the hash map.
    pub fn keys_count(&self) -> usize {
        self.header.buckets_count as usize
    }

    /// Populate the RAM cache for the backing file.
    pub fn populate(&self) -> UioResult<()> {
        self.storage.populate()
    }

    /// Evict the backing file data from RAM cache.
    pub fn clear_ram_cache(&self) -> UioResult<()> {
        self.storage.clear_ram_cache()
    }

    /// Read all entries and call the provided closure on each of them.
    ///
    /// Implementation detail: unlike [`UniversalHashMap::for_each_sparse`], it
    /// will read the whole file in a sequence.
    pub fn for_each_entry<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&K, &[V]) -> Result<(), E>,
        E: From<UniversalIoError>,
    {
        let entries_expected = self.keys_count();
        let mut entries_seen = 0;
        if entries_expected == 0 {
            return Ok(());
        }

        let mut buf = AlignedBuf::new_for_offset(self.entries_start, align_of::<u128>());
        let file_len = self.storage.len()?;
        let range = ReadRange {
            byte_offset: self.entries_start,
            length: file_len - self.entries_start,
        };
        let mut iter = OrderingIterator::new(
            self.storage
                .read_iter::<_, usize>(range.iter_autochunks::<u8>().enumerate(), Sequential)?,
        );

        while let Some(chunk) = iter.next() {
            let (_chunk_idx, chunk) = chunk?;
            buf.extend_from_slice(&chunk);

            let mut remaining: &[u8] = &buf;
            while entries_seen < entries_expected
                && let MaybeIncompleteEntry::KeyAndValues(key, values, new_remaining) =
                    MaybeIncompleteEntry::partial_parse(remaining)
                        .map_err(UniversalIoError::from)?
            {
                f(key, values)?;
                entries_seen += 1;
                remaining = new_remaining;
            }
            buf.remove_prefix(..buf.len() - remaining.len());

            if entries_seen == entries_expected {
                debug_assert!(
                    buf.is_empty() && iter.next().is_none(),
                    "Trailing bytes left after parsing all entries"
                );
                return Ok(());
            }
        }

        Err(E::from(UniversalIoError::Io(read_err(format!(
            "Truncated file: expected {entries_expected} entries but only parsed {entries_seen}"
        )))))
    }

    /// Read all entry keys and call the provided closure on each of them.
    pub fn for_each_key<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&K) -> Result<(), E>,
        E: From<UniversalIoError>,
    {
        if self.average_entry_size < SEQUENTIAL_READ_THRESHOLD {
            self.for_each_entry(|key, _| f(key))
        } else {
            let buckets = self.storage.read(
                ReadRange::new(
                    self.header.buckets_pos,
                    self.header.buckets_count * size_of::<BucketOffset>() as u64,
                ),
                Sequential,
            )?;
            let mut offsets: Vec<BucketOffset> = buckets
                .as_chunks::<{ size_of::<BucketOffset>() }>()
                .0
                .iter()
                .map(|c| parse_bucket_offset(c).unwrap())
                .collect();
            offsets.sort_unstable();

            self.for_each_sparse(
                MaybeIncompleteEntryKind::KeyOnly,
                offsets.into_iter().map(|o| ((), Request::Offset(o))),
                |(), entry| {
                    let entry = entry.expect("Entry from bucket offset array should exist");
                    let key = entry.key().expect("KeyOnly entry should have a key");
                    f(key)
                },
            )
        }
    }

    /// Read all keys and call the provided closure on each of them.
    pub fn for_each_entry_in_iter<U, I, F, E>(&self, keys: I, mut f: F) -> Result<(), E>
    where
        U: UserData,
        I: IntoIterator<Item = (U, &'key K)>,
        F: FnMut(U, Option<&[V]>) -> Result<(), E>,
        E: From<UniversalIoError>,
    {
        self.for_each_sparse(
            MaybeIncompleteEntryKind::KeyAndValues,
            keys.into_iter()
                .map(|(user_data, key)| (user_data, Request::Key(key))),
            |user_data, entry| {
                let values =
                    entry.map(|e| e.values().expect("KeyAndValues entry should have values"));
                f(user_data, values)
            },
        )
    }

    /// Get the values associated with the `key`.
    ///
    /// Prefer to use [`Self::for_each_entry_in_iter`] instead.
    pub fn unbatched_get(&self, key: &K) -> UioResult<Option<Vec<V>>> {
        let mut result: Option<Vec<V>> = None;
        self.for_each_entry_in_iter(std::iter::once(((), key)), |(), values| {
            result = values.map(|v| v.to_vec());
            UioResult::Ok(())
        })?;
        Ok(result)
    }

    /// Return the number of values for `key` without reading the values themselves.
    pub fn unbatched_get_values_count(&self, key: &K) -> UioResult<Option<usize>> {
        let mut result: Option<usize> = None;
        self.for_each_sparse(
            MaybeIncompleteEntryKind::KeyAndValuesLen,
            std::iter::once(((), Request::Key(key))),
            |(), entry| {
                if let Some(e) = entry {
                    let values_len = e
                        .values_len()
                        .expect("KeyAndValuesLen entry should have values len");
                    result = Some(values_len as usize);
                }
                UioResult::Ok(())
            },
        )?;
        Ok(result)
    }
}

fn parse_bucket_offset(data: &[u8]) -> UioResult<BucketOffset> {
    match <[u8; size_of::<BucketOffset>()]>::try_from(data) {
        Ok(bytes) => Ok(BucketOffset::from_ne_bytes(bytes)),
        Err(_) => Err(UniversalIoError::Io(read_err("Can't read bucket offset"))),
    }
}
