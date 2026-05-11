use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::super::read_err;
use super::{BucketOffset, Key, MaybeIncompleteEntry, MaybeIncompleteEntryKind, UniversalHashMap};
use crate::aligned_buf::AlignedBuf;
use crate::generic_consts::Random;
use crate::persisted_hashmap::uio::parse_bucket_offset;
use crate::universal_io::read::UniversalReadPipeline;
use crate::universal_io::{ReadRange, Result, UniversalIoError, UniversalRead, UserData};

pub(super) enum Request<'a, K: Key + ?Sized> {
    /// Request an entry by the given offset with unknown key.
    Offset(u64),
    /// Request an entry by the given key. Will resolve the offset first.
    Key(&'a K),
}

/// State machine driver for [`UniversalHashMap::for_each_sparse`].
struct PipelineDriver<'map, 'key, U, K, V, S>
where
    U: UserData,
    K: Key + ?Sized + 'key,
    V: Sized + Copy + FromBytes + Immutable + IntoBytes + KnownLayout,
    S: UniversalRead,
{
    map: &'map UniversalHashMap<K, V, S>,
    entry_kind: MaybeIncompleteEntryKind,
    queue: Vec<Entry<'key, U, K>>,
    entry_read_size_est: u64,
    file_len: u64,
}

struct Entry<'a, U: UserData, K: Key + ?Sized> {
    user_data: U,
    state: State<'a, K>,
}

/// Lifecycle:
///
/// - [`Request::Offset`]             →             [`State::ReadingEntry`]
/// - [`Request::Key`] → [`State::ReadingOffset`] → [`State::ReadingEntry`]
/// - [`State::ReadingEntry`] → [`State::ReadingEntry`]
///   (repeat with larger size if our entry size estimate was too small)
/// - [`State::ReadingEntry`] → done
enum State<'a, K: Key + ?Sized> {
    ReadingOffset {
        requested_key: &'a K,
    },
    ReadingEntry {
        byte_offset: u64,
        buf: AlignedBuf,
        expected_len: u64,
        requested_key: Option<&'a K>,
    },
}

impl<'key, K, V, S> UniversalHashMap<K, V, S>
where
    K: Key + ?Sized + 'key,
    V: Sized + Copy + FromBytes + Immutable + IntoBytes + KnownLayout,
    S: UniversalRead,
{
    /// Read entries for the requested keys and call the provided closure on
    /// each of them.
    ///
    /// Implementation detail: unlike [`UniversalHashMap::for_each_entry`], it
    /// will try to read only the requested entries.
    pub(super) fn for_each_sparse<U, I, F, E>(
        &self,
        entry_kind: MaybeIncompleteEntryKind,
        requests: I,
        mut f: F,
    ) -> Result<(), E>
    where
        U: UserData,
        I: Iterator<Item = (U, Request<'key, K>)>,
        F: FnMut(U, Option<MaybeIncompleteEntry<'_, K, V>>) -> Result<(), E>,
        E: From<UniversalIoError>,
    {
        let mut sparse = PipelineDriver::new(self, entry_kind)?;
        let mut pipeline = S::ReadPipeline::new()?;
        let mut requests = requests.into_iter();
        loop {
            while pipeline.can_schedule() {
                let Some((entry, range)) = sparse.schedule_next_entry(&mut requests, &mut f)?
                else {
                    break;
                };
                pipeline.schedule::<Random>(entry, &self.storage.inner, range)?;
            }
            let Some((entry, data)) = pipeline.wait()? else {
                break;
            };
            sparse.process(entry, &data, &mut f)?;
        }
        Ok(())
    }
}

type ScheduledEntry<'key, U, K> = (Entry<'key, U, K>, ReadRange);

impl<'map, 'key, U, K, V, S> PipelineDriver<'map, 'key, U, K, V, S>
where
    U: UserData,
    K: Key + ?Sized + 'key,
    V: Copy + FromBytes + Immutable + IntoBytes + KnownLayout,
    S: UniversalRead,
{
    fn new(
        map: &'map UniversalHashMap<K, V, S>,
        entry_kind: MaybeIncompleteEntryKind,
    ) -> Result<Self> {
        Ok(Self {
            map,
            entry_kind,
            queue: Vec::new(),
            entry_read_size_est: entry_kind.estimated_size::<K, V>() as u64,
            file_len: map.storage.len()?,
        })
    }

    /// Produce the next entry to schedule.
    fn schedule_next_entry<E, F>(
        &mut self,
        requests: &mut impl Iterator<Item = (U, Request<'key, K>)>,
        f: &mut F,
    ) -> Result<Option<ScheduledEntry<'key, U, K>>, E>
    where
        E: From<UniversalIoError>,
        F: FnMut(U, Option<MaybeIncompleteEntry<'_, K, V>>) -> Result<(), E>,
    {
        if let Some(entry) = self.queue.pop() {
            match &entry.state {
                State::ReadingOffset { .. } => unreachable!("we don't schedule ReadingOffset"),
                State::ReadingEntry {
                    byte_offset,
                    buf,
                    expected_len,
                    requested_key: _,
                } => {
                    let range = ReadRange::new(
                        byte_offset.saturating_add(buf.len() as u64),
                        expected_len.saturating_sub(buf.len() as u64),
                    )
                    .clamp::<u8>(self.file_len);
                    if range.length == 0 {
                        return Err(E::from(UniversalIoError::Io(read_err("unexpected eof"))));
                    }
                    return Ok(Some((entry, range)));
                }
            };
        }

        for (user_data, request) in requests.by_ref() {
            let (state, range);
            match request {
                Request::Offset(offset) => {
                    let byte_offset = self.map.entries_start + offset;
                    state = State::ReadingEntry {
                        byte_offset,
                        buf: AlignedBuf::new_for_offset(byte_offset),
                        expected_len: self.entry_read_size_est,
                        requested_key: None,
                    };
                    range = ReadRange {
                        byte_offset,
                        length: self.entry_read_size_est,
                    };
                }
                Request::Key(requested_key) => {
                    // PHF miss: no stored entry; report immediately and continue.
                    let Some(hash) = self.map.phf.get(requested_key) else {
                        f(user_data, None)?;
                        continue;
                    };
                    // PHF hit: schedule the bucket-offset read; transitions to
                    // Loading once the offset arrives.
                    let bucket_byte_offset =
                        self.map.header.buckets_pos + hash * size_of::<BucketOffset>() as u64;
                    state = State::ReadingOffset { requested_key };
                    range = ReadRange {
                        byte_offset: bucket_byte_offset,
                        length: size_of::<BucketOffset>() as u64,
                    };
                }
            }
            let entry = Entry { user_data, state };
            return Ok(Some((entry, range.clamp::<u8>(self.file_len))));
        }

        Ok(None)
    }

    /// Process a completed read result.
    fn process<E, F>(&mut self, entry: Entry<'key, U, K>, data: &[u8], f: &mut F) -> Result<(), E>
    where
        E: From<UniversalIoError>,
        F: FnMut(U, Option<MaybeIncompleteEntry<'_, K, V>>) -> Result<(), E>,
    {
        match entry.state {
            State::ReadingOffset { requested_key } => {
                let byte_offset = self.map.entries_start + parse_bucket_offset(data)?;
                self.queue.push(Entry {
                    user_data: entry.user_data,
                    state: State::ReadingEntry {
                        byte_offset,
                        buf: AlignedBuf::new_for_offset(byte_offset),
                        expected_len: self.entry_read_size_est,
                        requested_key: Some(requested_key),
                    },
                });
            }

            State::ReadingEntry {
                byte_offset,
                mut buf,
                expected_len: _,
                mut requested_key,
            } => {
                buf.extend_from_slice(data);
                let parsed =
                    MaybeIncompleteEntry::partial_parse(&buf).map_err(UniversalIoError::from)?;

                if let Some(key) = requested_key
                    && let Some(stored_key) = parsed.key()
                {
                    if key != stored_key {
                        f(entry.user_data, None)?;
                        return Ok(());
                    }
                    requested_key = None;
                }

                if parsed.satisfies_kind(self.entry_kind) {
                    f(entry.user_data, Some(parsed))?;
                } else {
                    self.queue.push(Entry {
                        user_data: entry.user_data,
                        state: State::ReadingEntry {
                            byte_offset,
                            // `+ 1` so the size strictly grows when `buf.len()` is already a
                            // power of two; otherwise the next refill reads 0 bytes and loops.
                            expected_len: (buf.len() as u64 + 1)
                                .next_power_of_two()
                                .max(K::KEY_SIZE_EST as u64),
                            buf,
                            requested_key,
                        },
                    })
                }
            }
        }

        Ok(())
    }
}
