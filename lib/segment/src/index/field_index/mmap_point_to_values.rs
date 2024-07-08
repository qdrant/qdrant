use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::common::mmap_type::MmapSlice;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::common::Flusher;
use crate::types::{FloatPayloadType, GeoPoint, IntPayloadType};

const POINT_TO_VALUES_PATH: &str = "point_to_values.bin";
const NOT_ENOUGHT_BYTES_ERROR_MESSAGE: &str =
    "Not enough bytes to operate with mmaped file `point_to_values.bin`. Is the storage corrupted?";

/// Trait for values that can be stored in mmaped file. It's used in `MmapPointToValues` to store values.
/// Lifetime `'a` is used to define lifetime for `&'a str` case
pub trait MmapValue<'a>: Clone + Default + 'a {
    fn mmaped_size(&self) -> usize;

    fn read_from_mmap(bytes: &'a [u8], offset: usize) -> OperationResult<Self>;

    fn write_to_mmap(&self, bytes: &mut [u8], offset: usize) -> OperationResult<()>;
}

impl<'a> MmapValue<'a> for IntPayloadType {
    fn mmaped_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn read_from_mmap(bytes: &[u8], offset: usize) -> OperationResult<Self> {
        Self::read_from_prefix(&bytes[offset..])
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))
    }

    fn write_to_mmap(&self, bytes: &mut [u8], offset: usize) -> OperationResult<()> {
        self.write_to_prefix(&mut bytes[offset..])
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))
    }
}

impl<'a> MmapValue<'a> for FloatPayloadType {
    fn mmaped_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn read_from_mmap(bytes: &[u8], offset: usize) -> OperationResult<Self> {
        Self::read_from_prefix(&bytes[offset..])
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))
    }

    fn write_to_mmap(&self, bytes: &mut [u8], offset: usize) -> OperationResult<()> {
        self.write_to_prefix(&mut bytes[offset..])
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))
    }
}

impl<'a> MmapValue<'a> for GeoPoint {
    fn mmaped_size(&self) -> usize {
        2 * std::mem::size_of::<f64>()
    }

    fn read_from_mmap(bytes: &[u8], offset: usize) -> OperationResult<Self> {
        Ok(Self {
            lon: f64::read_from_prefix(&bytes[offset..])
                .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?,
            lat: f64::read_from_prefix(&bytes[offset + std::mem::size_of::<f64>()..])
                .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?,
        })
    }

    fn write_to_mmap(&self, bytes: &mut [u8], offset: usize) -> OperationResult<()> {
        self.lon
            .write_to_prefix(&mut bytes[offset..])
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?;
        self.lat
            .write_to_prefix(&mut bytes[offset + std::mem::size_of::<f64>()..])
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))
    }
}

impl<'a> MmapValue<'a> for &'a str {
    fn mmaped_size(&self) -> usize {
        self.as_bytes().len() + std::mem::size_of::<u32>()
    }

    fn read_from_mmap(bytes: &'a [u8], offset: usize) -> OperationResult<&'a str> {
        let bytes = &bytes[offset..];
        let size = u32::read_from_prefix(bytes)
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?
            as usize;

        let bytes = &bytes[std::mem::size_of::<u32>()..];
        if bytes.len() < size {
            return Err(OperationError::service_error(
                NOT_ENOUGHT_BYTES_ERROR_MESSAGE,
            ));
        }

        let bytes = &bytes[..size];

        let string = std::str::from_utf8(bytes).map_err(|_| OperationError::service_error(format!("UTF8 conversion error while reading {POINT_TO_VALUES_PATH}. Is the storage corrupted?")))?;
        Ok(string)
    }

    fn write_to_mmap(&self, bytes: &mut [u8], offset: usize) -> OperationResult<()> {
        let bytes = &mut bytes[offset..];
        if bytes.len() < self.mmaped_size() {
            return Err(OperationError::service_error(
                NOT_ENOUGHT_BYTES_ERROR_MESSAGE,
            ));
        }

        u32::write_to_prefix(&(self.len() as u32), bytes)
            .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?;

        let bytes = &mut bytes[std::mem::size_of::<u32>()..];
        self.as_bytes()
            .iter()
            .enumerate()
            .for_each(|(i, &b)| bytes[i] = b);
        Ok(())
    }
}

/// Flattened mmaped points-to-values map
/// It's an analogue of `Vec<Vec<N>>` but in mmaped file.
/// This structure doesn't support adding new values, only removing.
/// It's used in mmap field indices like `MmapMapIndex`, `MmapNumericIndex`, etc to store points-to-values map.
/// This structure is not generic to avoid boxing lifetimes for `&str` values.
pub struct MmapPointToValues {
    file_name: PathBuf,
    mmap: MmapSlice<u8>,
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

        // create new file amd mmap
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
            range
                .write_to_prefix(
                    &mut mmap[header_size + point_id as usize * std::mem::size_of::<MmapRange>()..],
                )
                .ok_or_else(|| OperationError::service_error(NOT_ENOUGHT_BYTES_ERROR_MESSAGE))?;

            for value in values {
                value.write_to_mmap(mmap.as_mut(), point_values_offset)?;
                point_values_offset += value.mmaped_size();
            }
        }

        let mmap = unsafe { MmapSlice::try_from(mmap)? };
        mmap.flusher()()?;

        Ok(Self {
            file_name,
            mmap,
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
        let mmap = unsafe { MmapSlice::try_from(mmap)? };

        Ok(Self {
            file_name,
            mmap,
            header,
        })
    }

    fn flusher(&self) -> Flusher {
        self.mmap.flusher()
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.file_name.clone()]
    }

    pub fn get_values<'a, T: MmapValue<'a>>(
        &'a self,
        point_id: PointOffsetType,
    ) -> OperationResult<Box<dyn Iterator<Item = OperationResult<T>> + 'a>> {
        // first, get range of values for point
        let range = if point_id < self.header.points_count as PointOffsetType {
            let range_offset = (self.header.ranges_start as usize)
                + (point_id as usize) * std::mem::size_of::<MmapRange>();
            self.mmap
                .get(range_offset..range_offset + std::mem::size_of::<MmapRange>())
                .and_then(MmapRange::read_from)
                .unwrap_or_default()
        } else {
            MmapRange { start: 0, count: 0 }
        };

        // second, define iteration step for values
        // iteration step gets remainder range from mmaped file and returns next range with left range
        let bytes: &[u8] = self.mmap.as_ref();
        let read_value = move |range: MmapRange| -> Option<(OperationResult<T>, MmapRange)> {
            if range.count > 0 {
                let value = T::read_from_mmap(bytes, range.start as usize);
                match value {
                    Ok(value) => {
                        let range = MmapRange {
                            start: range.start + value.mmaped_size() as u64,
                            count: range.count - 1,
                        };
                        Some((Ok(value), range))
                    }
                    // if error occurs, return error and empty range to stop iteration
                    Err(err) => Some((Err(err), MmapRange::default())),
                }
            } else {
                None
            }
        };

        // finally, return iterator
        Ok(Box::new(
            std::iter::successors(read_value(range), move |range| read_value(range.1))
                .map(|(value, _)| value),
        ))
    }

    pub fn remove_point(&mut self, point_id: PointOffsetType) -> OperationResult<()> {
        // change points values range to empty
        let range_offset = (self.header.ranges_start as usize)
            + (point_id as usize) * std::mem::size_of::<MmapRange>();
        if let Some(range) = self
            .mmap
            .get_mut(range_offset..range_offset + std::mem::size_of::<MmapRange>())
            .and_then(MmapRange::mut_from)
        {
            *range = Default::default();
            Ok(())
        } else {
            Err(OperationError::InconsistentStorage {
                description: NOT_ENOUGHT_BYTES_ERROR_MESSAGE.to_owned(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_mmap_point_to_values_remove() {
        let mut values: Vec<Vec<String>> = vec![
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
        let mut point_to_values = MmapPointToValues::open(dir.path()).unwrap();

        let check = |point_to_values: &MmapPointToValues, values: &[Vec<_>]| {
            for (idx, values) in values.iter().enumerate() {
                let v: Vec<String> = point_to_values
                    .get_values(idx as PointOffsetType)
                    .unwrap()
                    .map(|s: OperationResult<&str>| s.unwrap().to_owned())
                    .collect_vec();
                assert_eq!(&v, values);
            }
        };

        check(&point_to_values, &values);

        point_to_values.remove_point(0).unwrap();
        values[0].clear();

        check(&point_to_values, &values);

        point_to_values.remove_point(3).unwrap();
        values[3].clear();

        check(&point_to_values, &values);
    }
}
