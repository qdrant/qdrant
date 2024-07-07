use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use itertools::Itertools;
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::{common::{mmap_type::MmapSlice, operation_error::{OperationError, OperationResult}, Flusher}, types::{FloatPayloadType, GeoPoint, IntPayloadType}};

pub trait MmapValue: Clone + Default {
    fn mmaped_size(&self) -> usize;

    fn read_from_mmap(bytes: &[u8], offset: usize) -> OperationResult<Self>;

    fn write_to_mmap(&self, bytes: &mut [u8], offset: usize) -> OperationResult<()>;
}

impl MmapValue for IntPayloadType {
    fn mmaped_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn read_from_mmap(bytes: &[u8], offset: usize) -> OperationResult<Self> {
        let bytes = &bytes[offset..];
        if bytes.len() < std::mem::size_of::<Self>() {
            return Err(OperationError::service_error("Not enough bytes to read int payload index"));
        }

        Ok(Self::from_le_bytes(bytes[..std::mem::size_of::<Self>()].try_into().unwrap()))
    }

    fn write_to_mmap(&self, bytes: &mut [u8], offset: usize) -> OperationResult<()> {
        let bytes = &mut bytes[offset..];
        if bytes.len() < self.mmaped_size() {
            return Err(OperationError::service_error("Not enough bytes to write string key index"));
        }
        Self::to_le_bytes(*self).iter().enumerate().for_each(|(i, &b)| bytes[i] = b);
        Ok(())
    }
}

impl MmapValue for FloatPayloadType {
    fn mmaped_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn read_from_mmap(bytes: &[u8], offset: usize) -> OperationResult<Self> {
        let bytes = &bytes[offset..];
        if bytes.len() < std::mem::size_of::<Self>() {
            return Err(OperationError::service_error("Not enough bytes to read int payload index"));
        }

        Ok(Self::from_le_bytes(bytes[..std::mem::size_of::<Self>()].try_into().unwrap()))
    }

    fn write_to_mmap(&self, bytes: &mut [u8], offset: usize) -> OperationResult<()> {
        let bytes = &mut bytes[offset..];
        if bytes.len() < self.mmaped_size() {
            return Err(OperationError::service_error("Not enough bytes to write string key index"));
        }
        Self::to_le_bytes(*self).iter().enumerate().for_each(|(i, &b)| bytes[i] = b);
        Ok(())
    }
}

impl MmapValue for String {
    fn mmaped_size(&self) -> usize {
        self.as_bytes().len() + std::mem::size_of::<u32>()
    }

    fn read_from_mmap(bytes: &[u8], offset: usize) -> OperationResult<Self> {
        const ERROR_MESSAGE: &str = "Not enough bytes to read string payload index";

        let bytes = &bytes[offset..];
        if bytes.len() < std::mem::size_of::<u32>() {
            return Err(OperationError::service_error(ERROR_MESSAGE));
        }

        let size = u32::from_le_bytes(bytes[..std::mem::size_of::<u32>()].try_into().unwrap()) as usize;

        let bytes = &bytes[std::mem::size_of::<u32>()..];
        if bytes.len() < size {
            return Err(OperationError::service_error(ERROR_MESSAGE));
        }

        let bytes = &bytes[..size];

        let string = std::str::from_utf8(bytes).map_err(|_| OperationError::service_error("UFT8 conversion error. Is the storage corrupted?"))?;
        Ok(string.to_owned())
    }

    fn write_to_mmap(&self, bytes: &mut [u8], offset: usize) -> OperationResult<()> {
        let bytes = &mut bytes[offset..];
        if bytes.len() < self.mmaped_size() {
            return Err(OperationError::service_error("Not enough bytes to write string key index"));
        }

        u32::to_le_bytes(self.len() as u32).iter().enumerate().for_each(|(i, &b)| bytes[i] = b);

        let bytes = &mut bytes[std::mem::size_of::<u32>()..];
        self.as_bytes().iter().enumerate().for_each(|(i, &b)| bytes[i] = b);
        Ok(())
    }
}

impl MmapValue for GeoPoint {
    fn mmaped_size(&self) -> usize {
        2 * std::mem::size_of::<f64>()
    }

    fn read_from_mmap(bytes: &[u8], offset: usize) -> OperationResult<Self> {
        todo!()
    }

    fn write_to_mmap(&self, bytes: &mut [u8], offset: usize) -> OperationResult<()> {
        todo!()
    }
}

// Flatten points-to-values map
// It's an analogue of `Vec<Vec<N>>` but in mmaped file.
// This structure doesn't support adding new values, only removing.
// It's used in mmap field indices like `ImmutableMapIndex`, `ImmutableNumericIndex`, etc to store points-to-values map.
pub struct MmapPointToValues<T: MmapValue> {
    file_name: PathBuf,
    mmap: MmapSlice<u8>,
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

impl<T: MmapValue> MmapPointToValues<T> {
    pub fn from_iter(
        path: &Path,
        iter: impl Iterator<Item = (PointOffsetType, Vec<T>)> + Clone,
    ) -> OperationResult<Self> {
        let points_count = iter.clone().count();
        let header_size = std::mem::size_of::<Header>();
        let ranges_size = points_count * std::mem::size_of::<MmapRange>();
        let values_size = iter.clone().map(|v| v.1.iter().map(|v| v.mmaped_size()).sum::<usize>()).sum::<usize>();
        let file_size = header_size + ranges_size + values_size;

        let file_name = Self::get_file_name(path);
        create_and_ensure_length(&file_name, file_size)?;
        let mmap = open_write_mmap(&file_name)?;

        // fill mmap
        let header = Header {
            ranges_start: header_size as u64,
            points_count: points_count as u64,
        };

        let mmap = unsafe { MmapSlice::try_from(mmap)? };
        Ok(Self {
            file_name,
            mmap,
            header,
            phantom: std::marker::PhantomData,
        })
    }

    pub fn open(path: &Path) -> OperationResult<Self> {
        let file_name = Self::get_file_name(path);
        let mmap = open_write_mmap(&file_name)?;
        let header = Header::read_from_prefix(mmap.as_ref()).ok_or(OperationError::InconsistentStorage {
            description: format!("Error while field index header conversion")
        })?;
        let mmap = unsafe { MmapSlice::try_from(mmap)? };

        Ok(Self {
            file_name,
            mmap,
            header,
            phantom: std::marker::PhantomData,
        })
    }

    fn flusher(&self) -> Flusher {
        self.mmap.flusher()
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.file_name.clone()]
    }

    pub fn get_values(&self, point_id: PointOffsetType) -> OperationResult<Box<dyn Iterator<Item = T> + '_>> {
        let range = if point_id < self.header.points_count as PointOffsetType {
            let range_offset = (self.header.ranges_start as usize) + (point_id as usize) * std::mem::size_of::<MmapRange>();
            self
                .mmap
                .get(range_offset..range_offset + std::mem::size_of::<MmapRange>())
                .and_then(|bytes| MmapRange::read_from(bytes))
                .unwrap_or_default()
        } else {
            MmapRange { start: 0, count: 0 }
        };

        let bytes: &[u8] = self.mmap.as_ref();
        let read_value = move |range: MmapRange| -> Option<(T, MmapRange)> {
            if range.count > 0 {
                let value = T::read_from_mmap(bytes, range.start as usize).unwrap();
                let range = MmapRange { start: range.start + value.mmaped_size() as u64, count: range.count - 1 };
                Some((value, range))
            } else {
                None
            }
        };
        Ok(Box::new(std::iter::successors(read_value(range), move |range| read_value(range.1)).map(|(value, _)| value)))
    }

    pub fn remove_point(&mut self, point_id: PointOffsetType) -> OperationResult<Vec<T>> {
        let values = self.get_values(point_id)?.collect_vec();

        // change points values range to empty
        let range_offset = (self.header.ranges_start as usize) + (point_id as usize) * std::mem::size_of::<MmapRange>();
        self
            .mmap
            .get_mut(range_offset..range_offset + std::mem::size_of::<MmapRange>())
            .and_then(|bytes| MmapRange::mut_from(bytes))
            .map(|range| *range = MmapRange { start: 0, count: 0 });

        Ok(values)
    }

    fn get_file_name(path: &Path) -> PathBuf {
        // TODO
        path.join("")
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_immutable_point_to_values_remove() {
        let mut values: Vec<Vec<String>> = vec![
            vec!["0".to_owned(), "1".to_owned(), "2".to_owned(), "3".to_owned(), "4".to_owned()],
            vec!["5".to_owned(), "6".to_owned(), "7".to_owned(), "8".to_owned(), "9".to_owned()],
            vec!["0".to_owned(), "1".to_owned(), "2".to_owned(), "3".to_owned(), "4".to_owned()],
            vec!["5".to_owned(), "6".to_owned(), "7".to_owned(), "8".to_owned(), "9".to_owned()],
            vec!["10".to_owned(), "11".to_owned(), "12".to_owned()],
            vec![],
            vec!["13".to_owned()],
            vec!["14".to_owned(), "15".to_owned()],
        ];

        let dir = Builder::new().prefix("mmap_point_to_values").tempdir().unwrap();
        let mut point_to_values = MmapPointToValues::from_iter(dir.path(), values.clone().into_iter().enumerate().map(|(id, values)| (id as PointOffsetType, values))).unwrap();

        let check = |point_to_values: &MmapPointToValues<_>, values: &[Vec<_>]| {
            for (idx, values) in values.iter().enumerate() {
                let v = point_to_values.get_values(idx as PointOffsetType).unwrap().collect_vec();
                assert_eq!(&v, values);
            }
        };

        check(&point_to_values, &values);

        point_to_values.remove_point(0);
        values[0].clear();

        check(&point_to_values, &values);

        point_to_values.remove_point(3);
        values[3].clear();

        check(&point_to_values, &values);
    }
}
