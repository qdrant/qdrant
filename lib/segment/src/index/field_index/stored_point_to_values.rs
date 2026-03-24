use std::borrow::Cow;
use std::cmp::max;
use std::ops::Range;
use std::path::{Path, PathBuf};

use common::counter::conditioned_counter::ConditionedCounter;
use common::ext::ResultOptionExt;
use common::fs::clear_disk_cache;
use common::generic_consts::Random;
use common::mmap::{AdviceSetting, create_and_ensure_length, open_write_mmap};
use common::types::PointOffsetType;
use common::universal_io::{self, ReadOnly, ReadRange, UniversalRead};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

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
/// It's used in mmap field indices like `MmapMapIndex`, `MmapNumericIndex`, etc to store points-to-values map.
/// This structure is not generic to avoid boxing lifetimes for `&str` values.
pub struct StoredPointToValues<T: StoredValue + ?Sized, S: UniversalRead<u8>> {
    file_name: PathBuf,
    store: ReadOnly<S>,
    header: Header,
    phantom: std::marker::PhantomData<T>,
}

/// Memory and IO overhead of accessing mmap index.
pub const MMAP_PTV_ACCESS_OVERHEAD: usize = size_of::<MmapRange>();

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, FromBytes, Immutable, IntoBytes, KnownLayout)]
struct MmapRange {
    start: u64,
    count: u64,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, FromBytes, Immutable, IntoBytes, KnownLayout)]
struct Header {
    ranges_start: u64,
    points_count: u64,
}

impl<T, S> StoredPointToValues<T, S>
where
    T: StoredValue + ?Sized,
    S: UniversalRead<u8>,
{
    pub fn from_iter<'a>(
        path: &Path,
        iter: impl Iterator<Item = (PointOffsetType, impl Iterator<Item = &'a T>)> + Clone,
    ) -> OperationResult<Self>
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
        header
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
            .and_then(|bytes| range.write_to_prefix(bytes).ok())
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGH_BYTES_ERROR_MESSAGE))?;
        }

        mmap.flush()?;
        drop(mmap);

        Self::open(path, true)
    }

    pub fn open(path: &Path, populate: bool) -> OperationResult<Self> {
        let file_name = path.join(POINT_TO_VALUES_PATH);

        let open_options = common::universal_io::OpenOptions {
            writeable: false,
            need_sequential: false,
            disk_parallel: None,
            populate: Some(populate),
            advice: None,
            prevent_caching: None,
        };

        let store = ReadOnly::open(&file_name, open_options)?;

        let header_bytes = store.read::<Random>(ReadRange {
            byte_offset: 0,
            length: std::mem::size_of::<Header>() as u64,
        })?;

        let (header, _) = Header::read_from_prefix(&header_bytes).map_err(|_| {
            OperationError::InconsistentStorage {
                description: NOT_ENOUGH_BYTES_ERROR_MESSAGE.to_owned(),
            }
        })?;

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

        let bytes = self.store.read::<Random>(bytes_range)?;
        let count = self.get_values_count(point_id)?.unwrap_or(0);

        let iter = ValuesIter::new(bytes, count);

        Ok(Some(iter))
    }

    pub fn get_values_count(&self, point_id: PointOffsetType) -> OperationResult<Option<usize>> {
        self.get_range(point_id)
            .map_some(|range| range.count as usize)
    }

    pub fn len(&self) -> usize {
        self.header.points_count as usize
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.header.points_count == 0
    }

    fn get_range(&self, point_id: PointOffsetType) -> OperationResult<Option<MmapRange>> {
        if point_id < self.header.points_count as PointOffsetType {
            let range_offset = (self.header.ranges_start as usize)
                + (point_id as usize) * std::mem::size_of::<MmapRange>();

            let bytes = self.store.read::<Random>(ReadRange {
                byte_offset: range_offset as u64,
                length: std::mem::size_of::<MmapRange>() as u64,
            })?;
            Ok(MmapRange::read_from_prefix(&bytes)
                .ok()
                .map(|(range, _)| range))
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
                self.store.len()?
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
        clear_disk_cache(&self.file_name)?;
        Ok(())
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<
        Item = OperationResult<(PointOffsetType, Option<impl Iterator<Item = Cow<'_, T>>>)>,
    > + Clone {
        // TODO: Propagate counter upwards
        (0..self.len() as PointOffsetType)
            .map(|idx| Ok((idx, self.values_iter(idx, ConditionedCounter::never())?)))
    }
}

pub struct ValuesIter<'a, T: ?Sized> {
    bytes: Cow<'a, [u8]>,
    start: usize,
    count: usize,
    _type: std::marker::PhantomData<T>,
}

impl<'a, T: StoredValue + ?Sized + 'a> ValuesIter<'a, T> {
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
    use common::universal_io::MmapFile;
    use itertools::Itertools;
    use tempfile::Builder;

    use super::*;
    use crate::types::GeoPoint;

    #[test]
    fn test_mmap_point_to_values_string() {
        let values: Vec<Vec<String>> = vec![
            vec![
                "fox".to_owned(),
                "driver".to_owned(),
                "point".to_owned(),
                "it".to_owned(),
                "box".to_owned(),
            ],
            vec![
                "alice".to_owned(),
                "red".to_owned(),
                "yellow".to_owned(),
                "blue".to_owned(),
                "apple".to_owned(),
            ],
            vec![
                "box".to_owned(),
                "qdrant".to_owned(),
                "line".to_owned(),
                "bash".to_owned(),
                "reproduction".to_owned(),
            ],
            vec![
                "list".to_owned(),
                "vitamin".to_owned(),
                "one".to_owned(),
                "two".to_owned(),
                "three".to_owned(),
            ],
            vec![
                "tree".to_owned(),
                "metallic".to_owned(),
                "ownership".to_owned(),
            ],
            vec![],
            vec!["slice".to_owned()],
            vec!["red".to_owned(), "pink".to_owned()],
        ];

        let dir = Builder::new()
            .prefix("mmap_point_to_values")
            .tempdir()
            .unwrap();
        StoredPointToValues::<str, MmapFile>::from_iter(
            dir.path(),
            values
                .iter()
                .enumerate()
                .map(|(id, values)| (id as PointOffsetType, values.iter().map(|s| s.as_str()))),
        )
        .unwrap();
        let point_to_values =
            StoredPointToValues::<str, MmapFile>::open(dir.path(), false).unwrap();

        for (idx, values) in values.iter().enumerate() {
            let v = point_to_values
                .values_iter(idx as PointOffsetType, ConditionedCounter::never())
                .unwrap()
                .unwrap()
                .map(|s| s.into_owned())
                .collect_vec();
            assert_eq!(&v, values);
        }
    }

    #[test]
    fn test_mmap_point_to_values_geo() {
        let values: Vec<Vec<GeoPoint>> = vec![
            vec![
                GeoPoint::new_unchecked(6.0, 2.0),
                GeoPoint::new_unchecked(4.0, 3.0),
                GeoPoint::new_unchecked(2.0, 5.0),
                GeoPoint::new_unchecked(8.0, 7.0),
                GeoPoint::new_unchecked(1.0, 9.0),
            ],
            vec![
                GeoPoint::new_unchecked(8.0, 1.0),
                GeoPoint::new_unchecked(3.0, 3.0),
                GeoPoint::new_unchecked(5.0, 9.0),
                GeoPoint::new_unchecked(1.0, 8.0),
                GeoPoint::new_unchecked(7.0, 2.0),
            ],
            vec![
                GeoPoint::new_unchecked(6.0, 3.0),
                GeoPoint::new_unchecked(4.0, 4.0),
                GeoPoint::new_unchecked(3.0, 7.0),
                GeoPoint::new_unchecked(1.0, 2.0),
                GeoPoint::new_unchecked(4.0, 8.0),
            ],
            vec![
                GeoPoint::new_unchecked(1.0, 3.0),
                GeoPoint::new_unchecked(3.0, 9.0),
                GeoPoint::new_unchecked(7.0, 0.0),
            ],
            vec![],
            vec![GeoPoint::new_unchecked(8.0, 5.0)],
            vec![GeoPoint::new_unchecked(9.0, 4.0)],
        ];

        let dir = Builder::new()
            .prefix("mmap_point_to_values")
            .tempdir()
            .unwrap();
        StoredPointToValues::<GeoPoint, MmapFile>::from_iter(
            dir.path(),
            values
                .iter()
                .enumerate()
                .map(|(id, values)| (id as PointOffsetType, values.iter())),
        )
        .unwrap();
        let point_to_values =
            StoredPointToValues::<GeoPoint, MmapFile>::open(dir.path(), false).unwrap();

        for (idx, values) in values.iter().enumerate() {
            let iter = point_to_values
                .values_iter(idx as PointOffsetType, ConditionedCounter::never())
                .unwrap();
            let v: Vec<GeoPoint> = iter
                .map(|iter| iter.map(Cow::into_owned).collect_vec())
                .unwrap_or_default();
            assert_eq!(&v, values);
        }
    }
}
