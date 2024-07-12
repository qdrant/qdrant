use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use memmap2::Mmap;
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{FloatPayloadType, GeoPoint, IntPayloadType};

const POINT_TO_VALUES_PATH: &str = "point_to_values.bin";
const NOT_ENOUGHT_BYTES_ERROR_MESSAGE: &str =
    "Not enough bytes to operate with memmaped file `point_to_values.bin`. Is the storage corrupted?";

/// Trait for values that can be stored in memmaped file. It's used in `MmapPointToValues` to store values.
/// Lifetime `'a` is used to define lifetime for `&'a str` case
pub trait MmapValue<'a>: Clone + Default + 'a {
    fn mmaped_size(&self) -> usize;

    fn read_from_mmap(bytes: &'a [u8]) -> Option<Self>;

    fn write_to_mmap(&self, bytes: &mut [u8]) -> OperationResult<()>;
}

impl<'a> MmapValue<'a> for IntPayloadType {
    fn mmaped_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn read_from_mmap(bytes: &[u8]) -> Option<Self> {
        Self::read_from_prefix(bytes)
    }

    fn write_to_mmap(&self, bytes: &mut [u8]) -> OperationResult<()> {
        self.write_to_prefix(bytes)
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))
    }
}

impl<'a> MmapValue<'a> for FloatPayloadType {
    fn mmaped_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn read_from_mmap(bytes: &[u8]) -> Option<Self> {
        Self::read_from_prefix(bytes)
    }

    fn write_to_mmap(&self, bytes: &mut [u8]) -> OperationResult<()> {
        self.write_to_prefix(bytes)
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))
    }
}

impl<'a> MmapValue<'a> for GeoPoint {
    fn mmaped_size(&self) -> usize {
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

    fn write_to_mmap(&self, bytes: &mut [u8]) -> OperationResult<()> {
        self.lon
            .write_to_prefix(bytes)
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?;
        bytes
            .get_mut(std::mem::size_of::<f64>()..)
            .and_then(|bytes| self.lat.write_to_prefix(bytes))
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))
    }
}

impl<'a> MmapValue<'a> for &'a str {
    fn mmaped_size(&self) -> usize {
        self.as_bytes().len() + std::mem::size_of::<u32>()
    }

    fn read_from_mmap(bytes: &'a [u8]) -> Option<&'a str> {
        let size = u32::read_from_prefix(bytes)? as usize;

        let bytes = bytes.get(std::mem::size_of::<u32>()..std::mem::size_of::<u32>() + size)?;
        std::str::from_utf8(bytes).ok()
    }

    fn write_to_mmap(&self, bytes: &mut [u8]) -> OperationResult<()> {
        u32::write_to_prefix(&(self.len() as u32), bytes)
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?;

        let bytes = bytes
            .get_mut(std::mem::size_of::<u32>()..std::mem::size_of::<u32>() + self.len())
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?;

        self.as_bytes()
            .iter()
            .enumerate()
            .for_each(|(i, &b)| bytes[i] = b);
        Ok(())
    }
}

/// Flattened memmaped points-to-values map
/// It's an analogue of `Vec<Vec<N>>` but in memmaped file.
/// This structure doesn't support adding new values, only removing.
/// It's used in mmap field indices like `MmapMapIndex`, `MmapNumericIndex`, etc to store points-to-values map.
/// This structure is not generic to avoid boxing lifetimes for `&str` values.
pub struct MmapPointToValues {
    file_name: PathBuf,
    mmap: Mmap,
    header: Header,
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

#[allow(dead_code)]
impl MmapPointToValues {
    pub fn from_iter<'a, T: MmapValue<'a>>(
        path: &Path,
        iter: impl Iterator<Item = (PointOffsetType, Vec<T>)> + Clone,
    ) -> OperationResult<Self> {
        // calculate file size
        let header_size = std::mem::size_of::<Header>();
        let points_count = iter
            .clone()
            .map(|(point_id, _)| (point_id + 1) as usize)
            .max()
            .unwrap_or(0);
        let ranges_size = points_count * std::mem::size_of::<MmapRange>();
        let values_size = iter
            .clone()
            .map(|v| v.1.iter().map(|v| v.mmaped_size()).sum::<usize>())
            .sum::<usize>();
        let file_size = header_size + ranges_size + values_size;

        // create new file and mmap
        let file_name = path.join(POINT_TO_VALUES_PATH);
        create_and_ensure_length(&file_name, file_size)?;
        let mut mmap = open_write_mmap(&file_name)?;

        // fill mmap file data
        let header = Header {
            ranges_start: header_size as u64,
            points_count: points_count as u64,
        };
        header
            .write_to_prefix(mmap.as_mut())
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?;

        // counter for values offset
        let mut point_values_offset = header_size + ranges_size;
        for (point_id, values) in iter {
            let range = MmapRange {
                start: point_values_offset as u64,
                count: values.len() as u64,
            };
            mmap.get_mut(header_size + point_id as usize * std::mem::size_of::<MmapRange>()..)
                .and_then(|bytes| range.write_to_prefix(bytes))
                .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?;

            for value in values {
                let bytes = mmap.get_mut(point_values_offset..).ok_or_else(|| {
                    OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE)
                })?;
                value.write_to_mmap(bytes)?;
                point_values_offset += value.mmaped_size();
            }
        }

        mmap.flush()?;
        Ok(Self {
            file_name,
            mmap: mmap.make_read_only()?,
            header,
        })
    }

    pub fn open(path: &Path) -> OperationResult<Self> {
        let file_name = path.join(POINT_TO_VALUES_PATH);
        let mmap = open_write_mmap(&file_name)?;
        let header =
            Header::read_from_prefix(mmap.as_ref()).ok_or(OperationError::InconsistentStorage {
                description: NOT_ENOUGHT_BYTES_ERROR_MESSAGE.to_owned(),
            })?;

        Ok(Self {
            file_name,
            mmap: mmap.make_read_only()?,
            header,
        })
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.file_name.clone()]
    }

    pub fn get_values<'a, T: MmapValue<'a>>(
        &'a self,
        point_id: PointOffsetType,
    ) -> Box<dyn Iterator<Item = OperationResult<T>> + 'a> {
        // first, get range of values for point
        let range = if point_id < self.header.points_count as PointOffsetType {
            let range_offset = (self.header.ranges_start as usize)
                + (point_id as usize) * std::mem::size_of::<MmapRange>();
            self.mmap
                .get(range_offset..range_offset + std::mem::size_of::<MmapRange>())
                .and_then(MmapRange::read_from)
                .unwrap_or_default()
        } else {
            return Box::new(std::iter::empty());
        };

        // second, define iteration step for values
        // iteration step gets remainder range from memmaped file and returns left range
        let bytes: &[u8] = self.mmap.as_ref();
        let read_value = move |range: MmapRange| -> Option<(OperationResult<T>, MmapRange)> {
            if range.count > 0 {
                let bytes = bytes.get(range.start as usize..)?;
                T::read_from_mmap(bytes).map(|value| {
                    let range = MmapRange {
                        start: range.start + value.mmaped_size() as u64,
                        count: range.count - 1,
                    };
                    (Ok(value), range)
                })
            } else {
                None
            }
        };

        // finally, return iterator
        Box::new(
            std::iter::successors(read_value(range), move |range| read_value(range.1))
                .map(|(value, _)| value),
        )
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
        MmapPointToValues::from_iter(
            dir.path(),
            values.iter().enumerate().map(|(id, values)| {
                (
                    id as PointOffsetType,
                    values.iter().map(|s| s.as_str()).collect_vec(),
                )
            }),
        )
        .unwrap();
        let point_to_values = MmapPointToValues::open(dir.path()).unwrap();

        for (idx, values) in values.iter().enumerate() {
            let v: Vec<String> = point_to_values
                .get_values(idx as PointOffsetType)
                .map(|s: OperationResult<&str>| s.unwrap().to_owned())
                .collect_vec();
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
        MmapPointToValues::from_iter(
            dir.path(),
            values.iter().enumerate().map(|(id, values)| {
                (
                    id as PointOffsetType,
                    values.iter().cloned().collect_vec(),
                )
            }),
        )
        .unwrap();
        let point_to_values = MmapPointToValues::open(dir.path()).unwrap();

        for (idx, values) in values.iter().enumerate() {
            let v: Vec<GeoPoint> = point_to_values
                .get_values(idx as PointOffsetType)
                .map(|s: OperationResult<GeoPoint>| s.unwrap().clone())
                .collect_vec();
            assert_eq!(&v, values);
        }
    }
}
