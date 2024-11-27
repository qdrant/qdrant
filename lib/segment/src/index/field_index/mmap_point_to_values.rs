use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use memmap2::Mmap;
use memory::madvise::AdviceSetting;
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{FloatPayloadType, GeoPoint, IntPayloadType, UuidIntType};

const POINT_TO_VALUES_PATH: &str = "point_to_values.bin";
const NOT_ENOUGHT_BYTES_ERROR_MESSAGE: &str =
    "Not enough bytes to operate with memmapped file `point_to_values.bin`. Is the storage corrupted?";
const PADDING_SIZE: usize = 4096;

/// Trait for values that can be stored in memmapped file. It's used in `MmapPointToValues` to store values.
pub trait MmapValue {
    /// Lifetime `'a` is required to define lifetime for `&'a str` case
    type Referenced<'a>: Sized + Clone;

    fn mmapped_size(value: Self::Referenced<'_>) -> usize;

    fn read_from_mmap(bytes: &[u8]) -> Option<Self::Referenced<'_>>;

    fn write_to_mmap(value: Self::Referenced<'_>, bytes: &mut [u8]) -> Option<()>;

    fn from_referenced<'a>(value: &'a Self::Referenced<'_>) -> &'a Self;

    fn as_referenced(&self) -> Self::Referenced<'_>;
}

impl MmapValue for IntPayloadType {
    type Referenced<'a> = &'a Self;

    fn mmapped_size(_value: Self::Referenced<'_>) -> usize {
        std::mem::size_of::<Self>()
    }

    fn read_from_mmap(bytes: &[u8]) -> Option<Self::Referenced<'_>> {
        Self::ref_from_prefix(bytes)
    }

    fn write_to_mmap(value: Self::Referenced<'_>, bytes: &mut [u8]) -> Option<()> {
        value.write_to_prefix(bytes)
    }

    fn from_referenced<'a>(value: &'a Self::Referenced<'_>) -> &'a Self {
        value
    }

    fn as_referenced(&self) -> Self::Referenced<'_> {
        self
    }
}

impl MmapValue for FloatPayloadType {
    type Referenced<'a> = Self;

    fn mmapped_size(_value: Self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn read_from_mmap(bytes: &[u8]) -> Option<Self> {
        Self::read_from_prefix(bytes)
    }

    fn write_to_mmap(value: Self, bytes: &mut [u8]) -> Option<()> {
        value.write_to_prefix(bytes)
    }

    fn from_referenced<'a>(value: &'a Self::Referenced<'_>) -> &'a Self {
        value
    }

    fn as_referenced(&self) -> Self::Referenced<'_> {
        *self
    }
}

impl MmapValue for UuidIntType {
    type Referenced<'a> = &'a Self;

    fn mmapped_size(_value: Self::Referenced<'_>) -> usize {
        std::mem::size_of::<Self>()
    }

    fn read_from_mmap(bytes: &[u8]) -> Option<Self::Referenced<'_>> {
        Self::ref_from_prefix(bytes)
    }

    fn write_to_mmap(value: Self::Referenced<'_>, bytes: &mut [u8]) -> Option<()> {
        value.write_to_prefix(bytes)
    }

    fn from_referenced<'a>(value: &'a Self::Referenced<'_>) -> &'a Self {
        value
    }

    fn as_referenced(&self) -> Self::Referenced<'_> {
        self
    }
}

impl MmapValue for GeoPoint {
    type Referenced<'a> = Self;

    fn mmapped_size(_value: Self) -> usize {
        2 * std::mem::size_of::<f64>()
    }

    fn read_from_mmap(bytes: &[u8]) -> Option<Self> {
        Some(Self {
            lon: f64::read_from_prefix(bytes)?,
            lat: bytes
                .get(std::mem::size_of::<f64>()..)
                .and_then(f64::read_from_prefix)?,
        })
    }

    fn write_to_mmap(value: Self, bytes: &mut [u8]) -> Option<()> {
        value.lon.write_to_prefix(bytes)?;
        bytes
            .get_mut(std::mem::size_of::<f64>()..)
            .and_then(|bytes| value.lat.write_to_prefix(bytes))
    }

    fn from_referenced<'a>(value: &'a Self::Referenced<'_>) -> &'a Self {
        value
    }

    fn as_referenced(&self) -> Self::Referenced<'_> {
        self.clone()
    }
}

impl MmapValue for str {
    type Referenced<'a> = &'a str;

    fn mmapped_size(value: &str) -> usize {
        value.len() + std::mem::size_of::<u32>()
    }

    fn read_from_mmap(bytes: &[u8]) -> Option<&str> {
        let size = u32::read_from_prefix(bytes)? as usize;

        let bytes = bytes.get(std::mem::size_of::<u32>()..std::mem::size_of::<u32>() + size)?;
        std::str::from_utf8(bytes).ok()
    }

    fn write_to_mmap(value: &str, bytes: &mut [u8]) -> Option<()> {
        u32::write_to_prefix(&(value.len() as u32), bytes)?;
        bytes
            .get_mut(std::mem::size_of::<u32>()..std::mem::size_of::<u32>() + value.len())?
            .copy_from_slice(value.as_bytes());
        Some(())
    }

    fn from_referenced<'a>(value: &'a Self::Referenced<'_>) -> &'a Self {
        value
    }

    fn as_referenced(&self) -> Self::Referenced<'_> {
        self
    }
}

/// Flattened memmapped points-to-values map
/// It's an analogue of `Vec<Vec<N>>` but in memmapped file.
/// This structure doesn't support adding new values, only removing.
/// It's used in mmap field indices like `MmapMapIndex`, `MmapNumericIndex`, etc to store points-to-values map.
/// This structure is not generic to avoid boxing lifetimes for `&str` values.
pub struct MmapPointToValues<T: MmapValue + ?Sized> {
    file_name: PathBuf,
    mmap: Mmap,
    header: Header,
    phantom: std::marker::PhantomData<T>,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, Default, AsBytes, FromBytes, FromZeroes)]
struct MmapRange {
    start: u64,
    count: u64,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, AsBytes, FromBytes, FromZeroes)]
struct Header {
    ranges_start: u64,
    points_count: u64,
}

impl<T: MmapValue + ?Sized> MmapPointToValues<T> {
    pub fn from_iter<'a>(
        path: &Path,
        iter: impl Iterator<Item = (PointOffsetType, impl Iterator<Item = T::Referenced<'a>>)> + Clone,
    ) -> OperationResult<Self> {
        // calculate file size
        let points_count = iter
            .clone()
            .map(|(point_id, _)| (point_id + 1) as usize)
            .max()
            .unwrap_or(0);
        let ranges_size = points_count * std::mem::size_of::<MmapRange>();
        let values_size = iter
            .clone()
            .map(|v| v.1.map(|v| T::mmapped_size(v)).sum::<usize>())
            .sum::<usize>();
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
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?;

        // counter for values offset
        let mut point_values_offset = header.ranges_start as usize + ranges_size;
        for (point_id, values) in iter {
            let start = point_values_offset;
            let mut values_count = 0;
            for value in values {
                values_count += 1;
                let bytes = mmap.get_mut(point_values_offset..).ok_or_else(|| {
                    OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE)
                })?;
                T::write_to_mmap(value.clone(), bytes).ok_or_else(|| {
                    OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE)
                })?;
                point_values_offset += T::mmapped_size(value);
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
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?;
        }

        mmap.flush()?;
        Ok(Self {
            file_name,
            mmap: mmap.make_read_only()?,
            header,
            phantom: std::marker::PhantomData,
        })
    }

    pub fn open(path: &Path) -> OperationResult<Self> {
        let file_name = path.join(POINT_TO_VALUES_PATH);
        let mmap = open_write_mmap(&file_name, AdviceSetting::Global, false)?;
        let header = Header::read_from_prefix(mmap.as_ref()).ok_or_else(|| {
            OperationError::InconsistentStorage {
                description: NOT_ENOUGHT_BYTES_ERROR_MESSAGE.to_owned(),
            }
        })?;

        Ok(Self {
            file_name,
            mmap: mmap.make_read_only()?,
            header,
            phantom: std::marker::PhantomData,
        })
    }

    pub fn files(&self) -> Vec<PathBuf> {
        vec![self.file_name.clone()]
    }

    pub fn check_values_any(
        &self,
        point_id: PointOffsetType,
        check_fn: impl Fn(T::Referenced<'_>) -> bool,
    ) -> bool {
        self.get_range(point_id)
            .map(|range| {
                let mut value_offset = range.start as usize;
                for _ in 0..range.count {
                    let bytes = self.mmap.get(value_offset..).unwrap();
                    let value = T::read_from_mmap(bytes).unwrap();
                    if check_fn(value.clone()) {
                        return true;
                    }
                    value_offset += T::mmapped_size(value);
                }
                false
            })
            .unwrap_or(false)
    }

    pub fn get_values<'a>(
        &'a self,
        point_id: PointOffsetType,
    ) -> Option<impl Iterator<Item = T::Referenced<'a>> + 'a> {
        // first, get range of values for point
        let range = self.get_range(point_id)?;

        // second, define iteration step for values
        // iteration step gets remainder range from memmapped file and returns left range
        let bytes: &[u8] = self.mmap.as_ref();
        let read_value = move |range: MmapRange| -> Option<(T::Referenced<'a>, MmapRange)> {
            if range.count > 0 {
                let bytes = bytes.get(range.start as usize..)?;
                T::read_from_mmap(bytes).map(|value| {
                    let range = MmapRange {
                        start: range.start + T::mmapped_size(value.clone()) as u64,
                        count: range.count - 1,
                    };
                    (value, range)
                })
            } else {
                None
            }
        };

        // finally, return iterator
        Some(
            std::iter::successors(read_value(range), move |range| read_value(range.1))
                .map(|(value, _)| value),
        )
    }

    pub fn get_values_count(&self, point_id: PointOffsetType) -> Option<usize> {
        self.get_range(point_id).map(|range| range.count as usize)
    }

    pub fn len(&self) -> usize {
        self.header.points_count as usize
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.header.points_count == 0
    }

    fn get_range(&self, point_id: PointOffsetType) -> Option<MmapRange> {
        if point_id < self.header.points_count as PointOffsetType {
            let range_offset = (self.header.ranges_start as usize)
                + (point_id as usize) * std::mem::size_of::<MmapRange>();
            self.mmap
                .get(range_offset..range_offset + std::mem::size_of::<MmapRange>())
                .and_then(MmapRange::read_from)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use tempfile::Builder;

    use super::*;

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
        MmapPointToValues::<str>::from_iter(
            dir.path(),
            values
                .iter()
                .enumerate()
                .map(|(id, values)| (id as PointOffsetType, values.iter().map(|s| s.as_str()))),
        )
        .unwrap();
        let point_to_values = MmapPointToValues::<str>::open(dir.path()).unwrap();

        for (idx, values) in values.iter().enumerate() {
            let iter = point_to_values.get_values(idx as PointOffsetType);
            let v: Vec<String> = iter
                .map(|iter| iter.map(|s: &str| s.to_owned()).collect_vec())
                .unwrap_or_default();
            assert_eq!(&v, values);
        }
    }

    #[test]
    fn test_mmap_point_to_values_geo() {
        let values: Vec<Vec<GeoPoint>> = vec![
            vec![
                GeoPoint { lon: 6.0, lat: 2.0 },
                GeoPoint { lon: 4.0, lat: 3.0 },
                GeoPoint { lon: 2.0, lat: 5.0 },
                GeoPoint { lon: 8.0, lat: 7.0 },
                GeoPoint { lon: 1.0, lat: 9.0 },
            ],
            vec![
                GeoPoint { lon: 8.0, lat: 1.0 },
                GeoPoint { lon: 3.0, lat: 3.0 },
                GeoPoint { lon: 5.0, lat: 9.0 },
                GeoPoint { lon: 1.0, lat: 8.0 },
                GeoPoint { lon: 7.0, lat: 2.0 },
            ],
            vec![
                GeoPoint { lon: 6.0, lat: 3.0 },
                GeoPoint { lon: 4.0, lat: 4.0 },
                GeoPoint { lon: 3.0, lat: 7.0 },
                GeoPoint { lon: 1.0, lat: 2.0 },
                GeoPoint { lon: 4.0, lat: 8.0 },
            ],
            vec![
                GeoPoint { lon: 1.0, lat: 3.0 },
                GeoPoint { lon: 3.0, lat: 9.0 },
                GeoPoint { lon: 7.0, lat: 0.0 },
            ],
            vec![],
            vec![GeoPoint { lon: 8.0, lat: 5.0 }],
            vec![GeoPoint { lon: 9.0, lat: 4.0 }],
        ];

        let dir = Builder::new()
            .prefix("mmap_point_to_values")
            .tempdir()
            .unwrap();
        MmapPointToValues::<GeoPoint>::from_iter(
            dir.path(),
            values
                .iter()
                .enumerate()
                .map(|(id, values)| (id as PointOffsetType, values.iter().cloned())),
        )
        .unwrap();
        let point_to_values = MmapPointToValues::<GeoPoint>::open(dir.path()).unwrap();

        for (idx, values) in values.iter().enumerate() {
            let iter = point_to_values.get_values(idx as PointOffsetType);
            let v: Vec<GeoPoint> = iter.map(|iter| iter.collect_vec()).unwrap_or_default();
            assert_eq!(&v, values);
        }
    }
}
