//! An in-memory [`universal_io`](common::universal_io) backend.
//!
//! Every file is a byte buffer held in the linear memory, so reads are plain slicing and never
//! block. That is what makes an edge shard usable from `wasm32-unknown-unknown`: the
//! [`UniversalRead`] surface is synchronous, and the browser has no way to block a thread on a
//! network round-trip, so the bytes must already be resident by the time the shard is opened.
//! Fetching them is the caller's job (see [`crate::object_store`]).
//!
//! Consequently this backend trades laziness for portability: a shard opened over it has its whole
//! working set in RAM, bounded by the 32-bit address space.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::ext::aligned_vec::{ACow, AVec, RuntimeAlign};
use common::generic_consts::AccessPattern;
use common::universal_io::{
    ListedFile, OpenOptions, ReadPipeline, UioResult, UniversalIoError, UniversalKind,
    UniversalRead, UniversalReadFileOps, UniversalReadFs, UserData,
};
use segment::index::field_index::bool_index::{BoolConditionChecker, ReadOnlyBoolIndex};
use segment::index::field_index::full_text_index::{
    FullTextConditionChecker, ReadOnlyFullTextIndex,
};
use segment::index::field_index::geo_index::{GeoConditionChecker, ReadOnlyGeoIndex};
use segment::index::field_index::map_index::MapConditionChecker;
use segment::index::field_index::map_index::read_only::ReadOnlyMapIndex;
use segment::index::field_index::null_index::{NullConditionChecker, ReadOnlyNullIndex};
use segment::index::field_index::numeric_index::{
    RangeConditionChecker, ReadOnlyNumericIndexInner,
};
use segment::index::{ConditionCheckerEnum, UniversalReadExt};
use segment::types::{
    FloatPayloadType, GeoBoundingBox, GeoRadius, IntPayloadType, PolygonWrapper, UuidIntType,
};

/// Alignment every file buffer is allocated with.
///
/// Reads hand out sub-slices of these buffers, and a caller casting one to `[T]` needs it aligned
/// for `T`. Matching the usual page size means any offset that is a multiple of the element size
/// — which is how the on-disk formats lay their arrays out — lands aligned, so the common path
/// borrows instead of copying. Misaligned reads still work, via an owned aligned copy.
const FILE_ALIGNMENT: usize = 4096;

/// A set of in-memory files, keyed by the path the shard will ask for.
#[derive(Clone, Debug, Default)]
pub struct MemFs {
    files: Arc<HashMap<PathBuf, Arc<AVec<u8, RuntimeAlign>>>>,
}

impl MemFs {
    /// Build a filesystem from `(path, contents)` pairs, copying each into an aligned buffer.
    pub fn new(files: impl IntoIterator<Item = (PathBuf, Vec<u8>)>) -> Self {
        let files = files
            .into_iter()
            .map(|(path, bytes)| {
                let buf = AVec::<u8, RuntimeAlign>::from_slice(FILE_ALIGNMENT, &bytes);
                (path, Arc::new(buf))
            })
            .collect();

        MemFs {
            files: Arc::new(files),
        }
    }

    /// Paths of every file held, in unspecified order.
    pub fn paths(&self) -> impl Iterator<Item = &Path> {
        self.files.keys().map(PathBuf::as_path)
    }

    /// Total bytes held across all files.
    pub fn total_len(&self) -> u64 {
        self.files.values().map(|buf| buf.len() as u64).sum()
    }
}

impl UniversalReadFileOps for MemFs {
    type ContextConfig = ();

    fn from_context(_context: ()) -> UioResult<Self> {
        Ok(MemFs::default())
    }

    fn list_files(&self, prefix_path: &Path) -> UioResult<Vec<ListedFile>> {
        // The contract is a path *prefix*, not a directory: `gridstore/page_` must match
        // `gridstore/page_1.dat`. Compare the raw strings rather than path components.
        let prefix = prefix_path.to_string_lossy().into_owned();

        Ok(self
            .files
            .iter()
            .filter(|(path, _)| path.to_string_lossy().starts_with(&prefix))
            .map(|(path, buf)| ListedFile {
                path: path.clone(),
                size: buf.len() as u64,
                last_modified: None,
            })
            .collect())
    }

    fn exists(&self, path: &Path) -> UioResult<bool> {
        Ok(self.files.contains_key(path))
    }
}

impl UniversalReadFs for MemFs {
    type File = MemFile;
    type OpenExtra = ();

    fn open(
        &self,
        path: impl AsRef<Path>,
        _options: OpenOptions,
        _extra: (),
    ) -> UioResult<MemFile> {
        let path = path.as_ref();
        let bytes = self
            .files
            .get(path)
            .ok_or_else(|| UniversalIoError::NotFound {
                path: path.to_path_buf(),
            })?
            .clone();

        Ok(MemFile {
            path: path.to_path_buf(),
            bytes,
        })
    }
}

/// A single in-memory file, cheap to clone (the bytes are shared).
#[derive(Clone)]
pub struct MemFile {
    path: PathBuf,
    bytes: Arc<AVec<u8, RuntimeAlign>>,
}

impl fmt::Debug for MemFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemFile")
            .field("path", &self.path)
            .field("len", &self.bytes.len())
            .finish()
    }
}

impl MemFile {
    fn slice(&self, range: Range<u64>, align: usize) -> UioResult<ACow<'_>> {
        let elements = self.bytes.len();
        let out_of_bounds = || UniversalIoError::OutOfBounds {
            start: range.start,
            end: range.end,
            elements,
        };

        let start = usize::try_from(range.start).map_err(|_| out_of_bounds())?;
        let end = usize::try_from(range.end).map_err(|_| out_of_bounds())?;
        let slice = self.bytes.get(start..end).ok_or_else(out_of_bounds)?;

        if slice.as_ptr().addr().is_multiple_of(align) {
            Ok(ACow::Borrowed(slice))
        } else {
            Ok(ACow::Owned(AVec::from_slice(align, slice)))
        }
    }
}

impl UniversalRead for MemFile {
    type Fs = MemFs;
    type ReadPipeline<'file, U>
        = MemPipeline<'file, U>
    where
        U: UserData;

    fn reopen(&mut self) -> UioResult<()> {
        // Nothing to reopen: the buffer never grows behind our back.
        Ok(())
    }

    fn read_bytes<P: AccessPattern>(
        &self,
        range: Range<u64>,
        _access_pattern: P,
        align: usize,
    ) -> UioResult<ACow<'_>> {
        self.slice(range, align)
    }

    fn len<T>(&self) -> UioResult<u64> {
        Ok((self.bytes.len() / size_of::<T>()) as u64)
    }

    fn populate(&self) -> UioResult<()> {
        // Already resident, by construction.
        Ok(())
    }

    fn populate_auto() -> bool {
        false
    }

    fn clear_ram_cache(&self) -> UioResult<()> {
        // The buffer *is* the storage; dropping it would lose the data.
        Ok(())
    }

    fn kind() -> UniversalKind {
        // Closest of the existing kinds: fully resident, like an mmap that is already faulted in.
        UniversalKind::SimpleDiskCache
    }
}

/// Read pipeline over [`MemFile`].
///
/// There is nothing to overlap — a read is a slice — so `schedule` resolves immediately and
/// `wait` drains a queue.
pub struct MemPipeline<'file, U> {
    ready: Vec<(U, ACow<'file>)>,
}

impl<U> fmt::Debug for MemPipeline<'_, U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemPipeline")
            .field("ready", &self.ready.len())
            .finish()
    }
}

impl<'file, U: UserData> ReadPipeline<'file, U> for MemPipeline<'file, U> {
    type File = MemFile;

    fn new() -> UioResult<Self> {
        Ok(MemPipeline { ready: Vec::new() })
    }

    fn can_schedule(&mut self) -> bool {
        true
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file MemFile,
        range: Range<u64>,
        align: usize,
    ) -> UioResult<()> {
        self.ready.push((user_data, file.slice(range, align)?));
        Ok(())
    }

    fn schedule_whole(&mut self, user_data: U, file: &'file MemFile, from: u64) -> UioResult<()> {
        let end = file.bytes.len() as u64;
        self.schedule::<common::generic_consts::Sequential>(user_data, file, from..end, 1)
    }

    fn wait(&mut self) -> UioResult<Option<(U, ACow<'file>)>> {
        // Order within a pipeline is unspecified, so pop from the back and skip the shift.
        Ok(self.ready.pop())
    }
}

/// The condition-checker constructors all take the generic checker over `MemFile` and hand back
/// the boxed `Dyn` variant — the same thing the object-store backend does. The enum's named
/// variants exist to keep monomorphised dispatch for the backends on the hot local path; a
/// browser shard is nowhere near that regime.
#[rustfmt::skip]
impl UniversalReadExt for MemFile {
    fn condition_checker_bool            <'a>(i: BoolCC<'a>)                    -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_full_text       <'a>(i: FullTextCC<'a>)                -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_geo_bounding_box<'a>(i: GeoCC<'a, GeoBoundingBox>)     -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_geo_polygon     <'a>(i: GeoCC<'a, PolygonWrapper>)     -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_geo_radius      <'a>(i: GeoCC<'a, GeoRadius>)          -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_map_int         <'a>(i: MapCC<'a, IntPayloadType>)     -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_map_str         <'a>(i: MapCC<'a, str>)                -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_map_uuid        <'a>(i: MapCC<'a, UuidIntType>)        -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_null            <'a>(i: NullCC<'a>)                    -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_numeric_float   <'a>(i: RangeCC<'a, FloatPayloadType>) -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_numeric_int     <'a>(i: RangeCC<'a, IntPayloadType>)   -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
    fn condition_checker_numeric_uuid    <'a>(i: RangeCC<'a, UuidIntType>)      -> EnumCC<'a> { EnumCC::Dyn(Box::new(i)) }
}

type EnumCC<'a> = ConditionCheckerEnum<'a>;
type BoolCC<'a> = BoolConditionChecker<'a, ReadOnlyBoolIndex<MemFile>>;
type FullTextCC<'a> = FullTextConditionChecker<'a, ReadOnlyFullTextIndex<MemFile>>;
type GeoCC<'a, C> = GeoConditionChecker<'a, ReadOnlyGeoIndex<MemFile>, C>;
type MapCC<'a, T> = MapConditionChecker<'a, T, ReadOnlyMapIndex<T, MemFile>>;
type NullCC<'a> = NullConditionChecker<'a, ReadOnlyNullIndex<MemFile>>;
type RangeCC<'a, T> = RangeConditionChecker<'a, ReadOnlyNumericIndexInner<T, MemFile>, T>;
