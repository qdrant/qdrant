use std::borrow::Cow;
use std::cmp::max;
use std::ops::Range;
use std::path::{Path, PathBuf};

use common::bitvec::{BitVec, DeletedBitVec};
use common::counter::conditioned_counter::ConditionedCounter;
use common::ext::ResultOptionExt;
use common::generic_consts::Random;
use common::mmap::{AdviceSetting, create_and_ensure_length, open_write_mmap};
use common::types::PointOffsetType;
use common::universal_io::{
    self, CachedReadFs, OpenOptions, Populate, ReadOnly, ReadRange, UniversalRead, UniversalReadFs,
    UserData,
};
use zerocopy::IntoBytes;

use crate::common::operation_error::{OperationError, OperationResult};

const POINT_TO_VALUES_PATH: &str = "point_to_values.bin";
const NOT_ENOUGH_BYTES_ERROR_MESSAGE: &str = "Not enough bytes to operate with slice in file `point_to_values.bin`. Is the storage corrupted?";
const PADDING_SIZE: usize = 4096;

/// Trait for values that can be stored in a file. It's used in `StoredPointToValues` to store values.
pub trait StoredValue: ToOwned {
    /// Get the size in bytes that the value will take when stored in a slice.
    fn stored_size(value: &Self) -> usize;

    /// Read one value from the beginning of the bytes slice.
    fn read_from_prefix(bytes: &[u8]) -> Option<&Self>;

    /// Write the value into the beginning of the bytes slice. The slice must have enough capacity for the value.
    fn write_to_prefix(&self, bytes: &mut [u8]) -> Option<()>;
}

impl<T: bytemuck::Pod> StoredValue for T {
    fn stored_size(_value: &Self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn read_from_prefix(bytes: &[u8]) -> Option<&Self> {
        bytemuck::try_cast_slice(bytes).ok()?.first()
    }

    fn write_to_prefix(&self, bytes: &mut [u8]) -> Option<()> {
        let value_bytes = bytemuck::bytes_of(self);
        <[u8] as zerocopy::IntoBytes>::write_to_prefix(value_bytes, bytes).ok()
    }
}

impl StoredValue for str {
    fn stored_size(value: &str) -> usize {
        value.len() + std::mem::size_of::<u32>()
    }

    fn read_from_prefix(bytes: &[u8]) -> Option<&Self> {
        let (len, rest) = <u32 as zerocopy::FromBytes>::read_from_prefix(bytes).ok()?;
        std::str::from_utf8(rest.get(..len as usize)?).ok()
    }

    fn write_to_prefix(&self, bytes: &mut [u8]) -> Option<()> {
        <u32 as zerocopy::IntoBytes>::write_to_prefix(&(self.len() as u32), bytes).ok()?;
        bytes
            .get_mut(std::mem::size_of::<u32>()..std::mem::size_of::<u32>() + self.len())?
            .copy_from_slice(self.as_bytes());
        Some(())
    }
}

/// Flattened memmapped points-to-values map
/// It's an analogue of `Vec<Vec<N>>` but in memmapped file.
/// This structure is immutable.
/// It's used in mmap field indices like `UniversalMapIndex`, `UniversalNumericIndex`, etc to store points-to-values map.
/// This structure is not generic to avoid boxing lifetimes for `&str` values.
pub struct OnDiskPointToValues<T: StoredValue + ?Sized, S: UniversalRead> {
    file_name: PathBuf,
    store: ReadOnly<S>,
    header: Header,
    phantom: std::marker::PhantomData<T>,
}

/// Memory and IO overhead of accessing mmap index.
pub const MMAP_PTV_ACCESS_OVERHEAD: usize = size_of::<MmapRange>();

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, bytemuck::Pod, bytemuck::Zeroable)]
struct MmapRange {
    start: u64,
    count: u64,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, bytemuck::Pod, bytemuck::Zeroable)]
struct Header {
    ranges_start: u64,
    points_count: u64,
}

impl<T, S> OnDiskPointToValues<T, S>
where
    T: StoredValue + ?Sized,
    S: UniversalRead,
{
    fn open_options(populate: Populate) -> OpenOptions {
        OpenOptions {
            writeable: false,
            need_sequential: false,
            populate,
            advice: AdviceSetting::Global,
        }
    }

    pub fn build_from_iter<'a>(
        path: &Path,
        iter: impl Iterator<Item = (PointOffsetType, impl Iterator<Item = &'a T>)> + Clone,
    ) -> OperationResult<()>
    where
        T: 'a,
    {
        // calculate file size
        let mut points_count: usize = 0;
        let mut values_size = 0;
        for (point_id, values) in iter.clone() {
            points_count = max(points_count, (point_id + 1) as usize);
            values_size += values.map(|v| T::stored_size(v)).sum::<usize>();
        }
        let ranges_size = points_count * std::mem::size_of::<MmapRange>();
        let file_size = PADDING_SIZE + ranges_size + values_size;

        // create new file and mmap
        let file_name = path.join(POINT_TO_VALUES_PATH);
        create_and_ensure_length(&file_name, file_size)?;
        let mut mmap = open_write_mmap(&file_name, AdviceSetting::Global, false)?;

        // fill mmap file data
        let header = Header {
            ranges_start: PADDING_SIZE as u64,
            points_count: points_count as u64,
        };
        bytemuck::bytes_of(&header)
            .write_to_prefix(mmap.as_mut())
            .map_err(|_| OperationError::service_error(NOT_ENOUGH_BYTES_ERROR_MESSAGE))?;

        // counter for values offset
        let mut point_values_offset = header.ranges_start as usize + ranges_size;
        for (point_id, values) in iter {
            let start = point_values_offset;
            let mut values_count = 0;
            for value in values {
                values_count += 1;
                let bytes = mmap
                    .get_mut(point_values_offset..)
                    .ok_or_else(|| OperationError::service_error(NOT_ENOUGH_BYTES_ERROR_MESSAGE))?;
                value
                    .write_to_prefix(bytes)
                    .ok_or_else(|| OperationError::service_error(NOT_ENOUGH_BYTES_ERROR_MESSAGE))?;
                point_values_offset += T::stored_size(value);
            }

            let range = MmapRange {
                start: start as u64,
                count: values_count as u64,
            };
            mmap.get_mut(
                header.ranges_start as usize
                    + point_id as usize * std::mem::size_of::<MmapRange>()..,
            )
            .and_then(|bytes| range.write_to_prefix(bytes))
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGH_BYTES_ERROR_MESSAGE))?;
        }

        mmap.flush()?;

        Ok(())
    }

    pub fn preopen(
        fs: &impl CachedReadFs<File = S>,
        dir: &Path,
        populate: Populate,
    ) -> OperationResult<()> {
        let file_name = dir.join(POINT_TO_VALUES_PATH);

        // TODO(uio): Turn Populate::No into Populate::BackgroundPartial(0..header_size)
        fs.schedule_prefetch(&file_name, Some(Self::open_options(populate)), None)?;

        Ok(())
    }

    pub fn open(
        fs: &impl UniversalReadFs<File = S>,
        dir: &Path,
        populate: Populate,
    ) -> OperationResult<Self> {
        let file_name = dir.join(POINT_TO_VALUES_PATH);

        let open_options = common::universal_io::OpenOptions {
            writeable: false,
            need_sequential: false,
            populate,
            advice: AdviceSetting::Global,
        };

        let store = ReadOnly::open(fs, &file_name, open_options, Default::default())?;

        let header = store.read::<Random, Header>(ReadRange::one(0))?[0];

        Ok(Self {
            file_name,
            store,
            header,
            phantom: std::marker::PhantomData,
        })
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.file_name.clone()]
    }

    pub fn immutable_files(&self) -> Vec<PathBuf> {
        // `MmapPointToValues` is immutable
        vec![self.file_name.clone()]
    }

    pub fn check_values_any(
        &self,
        point_id: PointOffsetType,
        check_fn: impl Fn(&T) -> bool,
        hw_counter: &ConditionedCounter,
    ) -> OperationResult<bool> {
        let Some(mut values_iter) = self.values_iter(point_id, *hw_counter)? else {
            return Ok(false);
        };

        while let Some(satisfy) = values_iter.map_next(&check_fn) {
            if satisfy {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn values_iter(
        &self,
        point_id: PointOffsetType,
        hw_counter: ConditionedCounter, // TODO: make it by reference
    ) -> OperationResult<Option<ValuesIter<'_, T>>> {
        let hw_cell = hw_counter.payload_index_io_read_counter();

        // first, get range of values for point
        let Some(bytes_range) = self.get_bytes_range(point_id)?.map(|range| {
            let range = universal_io::ReadRange {
                byte_offset: range.start,
                length: range.end - range.start,
            };
            // Measure IO overhead of `self.get_bytes_range()` and the length of the values
            hw_cell.incr_delta(MMAP_PTV_ACCESS_OVERHEAD + range.length as usize);

            range
        }) else {
            return Ok(None);
        };

        let bytes = self.store.read::<Random, u8>(bytes_range)?;
        let count = self.get_values_count(point_id)?.unwrap_or(0);

        let iter = ValuesIter::new(bytes, count);

        Ok(Some(iter))
    }

    /// Batched counterpart of [`Self::values_iter`].
    ///
    /// Calls `f` exactly once per input item, even for deleted/missing points.
    pub fn values_iter_batch<U: UserData>(
        &self,
        items: impl Iterator<Item = (U, PointOffsetType)>,
        deleted: &DeletedBitVec,
        hw_counter: ConditionedCounter,
        mut f: impl FnMut(U, ValuesIter<'_, T>),
    ) -> OperationResult<()> {
        let hw_cell = hw_counter.payload_index_io_read_counter();

        let points_count = self.header.points_count as PointOffsetType;
        let ranges_start = self.header.ranges_start;
        let file_len = self.store.len::<u8>()?;

        // Batch 1: Resolve each values' range and count
        let mut value_reads = Vec::with_capacity(items.size_hint().0.min(10_000));
        let range_reads = items.filter_map(|(user_data, point_id)| {
            if point_id >= points_count || !deleted.is_active(point_id) {
                f(user_data, ValuesIter::empty());
                return None;
            }

            let byte_offset = ranges_start + u64::from(point_id) * size_of::<MmapRange>() as u64;

            // Fetch 2 `MmapRange` entries per id, so we can get range start..end.
            // In case of being the last entry, we will do start..file_len
            let length = if point_id + 1 < points_count { 2 } else { 1 };
            Some((user_data, ReadRange::new(byte_offset, length)))
        });
        self.store
            .read_batch::<Random, MmapRange, _>(range_reads, |user_data, ranges| {
                let MmapRange { start, count } = ranges[0];

                // Use next point's start as end offset for this one.
                let end = ranges.get(1).map_or(file_len, |next| next.start);
                let length = end - start;

                // Mirror `values_iter`: account the per-point access overhead
                // plus the length of the values.
                hw_cell.incr_delta(MMAP_PTV_ACCESS_OVERHEAD + length as usize);

                // Note: if `count == 0`, we'd better call `f` right away, but
                // `f` is already mut-borrowed in the iterator above. So,
                // instead we schedule redundant 0-length read. These 0-length
                // reads are unlikely to happen in hot loops since empty points
                // usually marked in `deleted`.
                value_reads.push((user_data, count as usize, ReadRange::new(start, length)));

                Ok(())
            })?;

        // Batch 2: Read and pass the values to the callback as `ValuesIter`s.
        let value_reads = value_reads
            .into_iter()
            .map(|(user_data, count, range)| ((user_data, count), range));
        self.store
            .read_batch::<Random, u8, _>(value_reads, |(user_data, count), bytes| {
                f(user_data, ValuesIter::new(Cow::Borrowed(bytes), count));
                Ok(())
            })?;

        Ok(())
    }

    pub fn get_values_count(&self, point_id: PointOffsetType) -> OperationResult<Option<usize>> {
        self.get_range(point_id)
            .map_some(|range| range.count as usize)
    }

    pub fn len(&self) -> usize {
        self.header.points_count as usize
    }

    /// Approximate RAM usage in bytes. Data is mmap-backed, so no significant
    /// heap allocations; on-disk data is accounted via `files()`.
    pub fn ram_usage_bytes(&self) -> usize {
        let Self {
            file_name: _,
            store: _,
            header: _,
            phantom: _,
        } = self;
        0
    }

    fn get_range(&self, point_id: PointOffsetType) -> OperationResult<Option<MmapRange>> {
        if point_id < self.header.points_count as PointOffsetType {
            let range_offset =
                (self.header.ranges_start as usize) + (point_id as usize) * size_of::<MmapRange>();

            let range = self
                .store
                .read::<Random, MmapRange>(ReadRange::one(range_offset as u64))?[0];
            Ok(Some(range))
        } else {
            Ok(None)
        }
    }

    fn get_bytes_range(&self, point_id: PointOffsetType) -> OperationResult<Option<Range<u64>>> {
        let Some(start) = self.get_range(point_id).map_some(|range| range.start)? else {
            return Ok(None);
        };

        // Use next point's start as end offset for this one.
        let end = match self.get_range(point_id + 1).map_some(|range| range.start)? {
            Some(end) => end,
            None => {
                // if there is no next point, then use end of file
                self.store.len::<u8>()?
            }
        };

        Ok(Some(start..end))
    }

    /// Populate all pages in the mmap.
    /// Block until all pages are populated.
    pub fn populate(&self) -> OperationResult<()> {
        self.store.populate().map_err(Into::into)
    }

    /// Drop disk cache.
    pub fn clear_cache(&self) -> OperationResult<()> {
        let Self {
            file_name: _,
            store,
            header: _,
            phantom: _,
        } = self;
        store.clear_ram_cache().map_err(OperationError::from)?;
        Ok(())
    }

    /// Read every point's values in batches, invoking `f` once per point that
    /// has at least one value.
    pub fn for_all_points_values(
        &self,
        mut f: impl FnMut(PointOffsetType, ValuesIter<'_, T>),
    ) -> OperationResult<()> {
        // Report deleted points with values too.
        let blank_bitmask = DeletedBitVec::new(BitVec::repeat(false, self.len()));
        // TODO: Propagate counter upwards
        self.values_iter_batch(
            (0..self.len() as PointOffsetType).map(|point_id| (point_id, point_id)),
            &blank_bitmask,
            ConditionedCounter::never(),
            |point_id, values| {
                if values.count > 0 {
                    f(point_id, values);
                }
            },
        )
    }
}

pub struct ValuesIter<'a, T: ?Sized> {
    bytes: Cow<'a, [u8]>,
    start: usize,
    count: usize,
    _type: std::marker::PhantomData<T>,
}

impl<'a, T: StoredValue + ?Sized + 'a> ValuesIter<'a, T> {
    fn empty() -> Self {
        Self {
            bytes: Cow::Borrowed(&[]),
            start: 0,
            count: 0,
            _type: std::marker::PhantomData,
        }
    }

    fn new(bytes: Cow<'a, [u8]>, count: usize) -> Self {
        Self {
            bytes,
            start: 0,
            count,
            _type: std::marker::PhantomData,
        }
    }

    /// Similar to [`Iterator::next()`], but immediately process the reference, so that we don't
    /// need to `Cow` the value to be able to return it.
    fn map_next<U>(&mut self, map: impl Fn(&T) -> U) -> Option<U> {
        if self.count == 0 {
            return None;
        }

        let value = T::read_from_prefix(self.bytes.get(self.start..)?)?;

        let out = map(value);

        self.start += T::stored_size(value);
        self.count = self.count.saturating_sub(1);

        Some(out)
    }
}

impl<'a, T: StoredValue + ?Sized + 'a> Iterator for ValuesIter<'a, T> {
    type Item = Cow<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count == 0 {
            return None;
        }

        let cow = match &self.bytes {
            Cow::Borrowed(slice) => Cow::Borrowed(T::read_from_prefix(slice.get(self.start..)?)?),
            Cow::Owned(vec) => Cow::Owned(T::read_from_prefix(vec.get(self.start..)?)?.to_owned()),
        };

        self.start += T::stored_size(cow.as_ref());
        self.count = self.count.saturating_sub(1);

        Some(cow)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;

    #[cfg(target_os = "linux")]
    use common::universal_io::IoUringFs;
    use common::universal_io::MmapFs;
    use itertools::Itertools;
    use tempfile::Builder;

    use super::*;
    use crate::types::GeoPoint;

    #[test]
    fn test_point_to_values() {
        let gp = |x, y| GeoPoint::new_unchecked(x, y);

        let str_data: &[&[&str]] = &[
            &["fox", "driver", "point", "it", "box"],
            &["alice", "red", "yellow", "blue", "apple"],
            &["box", "qdrant", "line", "bash", "reproduction"],
            &["list", "vitamin", "one", "two", "three"],
            &["tree", "metallic", "ownership"],
            &[],
            &["slice"],
            &["red", "pink"],
        ];
        let geo_data = &[
            vec![gp(6., 2.), gp(4., 3.), gp(2., 5.), gp(8., 7.), gp(1., 9.)],
            vec![gp(8., 1.), gp(3., 3.), gp(5., 9.), gp(1., 8.), gp(7., 2.)],
            vec![gp(6., 3.), gp(4., 4.), gp(3., 7.), gp(1., 2.), gp(4., 8.)],
            vec![gp(1., 3.), gp(3., 9.), gp(7., 0.)],
            vec![],
            vec![gp(8., 5.)],
            vec![gp(9., 4.)],
        ];

        let str_data = str_data
            .iter()
            .map(|arr| arr.iter().map(|s| s.to_string()).collect_vec())
            .collect_vec();

        check_point_to_values::<str, _>(&MmapFs, &str_data);
        #[cfg(target_os = "linux")]
        check_point_to_values::<str, _>(&IoUringFs, &str_data);

        check_point_to_values::<GeoPoint, _>(&MmapFs, geo_data);
        #[cfg(target_os = "linux")]
        check_point_to_values::<GeoPoint, _>(&IoUringFs, geo_data);
    }

    fn check_point_to_values<T, Fs>(fs: &Fs, values: &[Vec<T::Owned>])
    where
        T: StoredValue + ?Sized,
        T::Owned: Borrow<T> + PartialEq + std::fmt::Debug,
        Fs: UniversalReadFs,
    {
        let dir = Builder::new().prefix("point_to_values").tempdir().unwrap();
        OnDiskPointToValues::<T, Fs::File>::build_from_iter(
            dir.path(),
            values
                .iter()
                .enumerate()
                .map(|(id, values)| (id as PointOffsetType, values.iter().map(|s| s.borrow()))),
        )
        .unwrap();
        let ppv = OnDiskPointToValues::<T, Fs::File>::open(fs, dir.path(), Populate::No).unwrap();

        // Roundtrip check
        for (idx, values) in values.iter().enumerate() {
            let v = ppv
                .values_iter(idx as PointOffsetType, ConditionedCounter::never())
                .unwrap()
                .unwrap()
                .map(Cow::into_owned)
                .collect_vec();
            assert_eq!(&v, values, "roundtrip check");
        }

        // `values_iter_batch` check
        {
            // The callback should be called with an empty iterator even in
            // these edge cases:
            // 1. Point deleted via bitvec.
            let deleted_id = values.iter().position(|vals| !vals.is_empty()).unwrap();
            // 2. Non-deleted point with no values. (just assert that it exists in the input)
            let _empty_id = values.iter().position(|vals| vals.is_empty()).unwrap();
            // 3. Large ID. (out-of-range)
            let large_id = values.len() + 100;

            let mut deleted = DeletedBitVec::new(BitVec::repeat(false, values.len()));
            deleted.mark_deleted(deleted_id as PointOffsetType);

            // Run `values_iter_batch` and store its results into
            let mut reported = Vec::new();
            ppv.values_iter_batch(
                (0..values.len()).chain([large_id]).map(|id| (id, id as _)),
                &deleted,
                ConditionedCounter::never(),
                |id, vals| reported.push((id, vals.map(Cow::into_owned).collect_vec())),
            )
            .unwrap();
            reported.sort_unstable_by_key(|&(id, _)| id);

            // Compare with expected
            let mut expected = Vec::new();
            for (id, vals) in values.iter().enumerate() {
                let vals = vals.iter().map(|v| v.borrow().to_owned()).collect_vec();
                expected.push((id, vals));
            }
            expected[deleted_id].1.clear();
            expected.push((large_id, vec![]));
            assert_eq!(reported, expected);
        }
    }
}
