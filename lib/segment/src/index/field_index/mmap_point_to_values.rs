use std::path::{Path, PathBuf};

use common::types::PointOffsetType;
use itertools::Itertools;
use memmap2::MmapMut;
use memory::mmap_ops::{create_and_ensure_length, open_write_mmap, transmute_from_u8, transmute_from_u8_mut};

use crate::{common::{operation_error::{OperationError, OperationResult}, Flusher}, types::{FloatPayloadType, GeoPoint, IntPayloadType}};

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

        let string = std::str::from_utf8(bytes).map_err(|_| OperationError::service_error(ERROR_MESSAGE))?;
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
    mmap: MmapMut,
    ranges_offset: usize,
    points_count: usize,
    phantom: std::marker::PhantomData<T>,
}

#[derive(Clone, Copy)]
struct MmapRange {
    start: u64,
    count: u64,
}

impl<T: MmapValue> MmapPointToValues<T> {
    pub fn from_iter(
        path: &Path,
        iter: impl Iterator<Item = (PointOffsetType, Vec<T>)> + Clone,
    ) -> OperationResult<Self> {
        let points_count = iter.clone().count();
        let header_size = std::mem::size_of::<MmapRange>();
        let ranges_size = points_count * std::mem::size_of::<MmapRange>();
        let values_size = iter.clone().map(|v| v.1.iter().map(|v| v.mmaped_size()).sum::<usize>()).sum::<usize>();
        let file_size = header_size + ranges_size + values_size;

        let file_name = Self::get_file_name(path);
        create_and_ensure_length(&file_name, file_size)?;
        let mmap = open_write_mmap(&file_name)?;

        // fill mmap
        
        Ok(Self {
            file_name,
            mmap,
            ranges_offset: header_size,
            points_count,
            phantom: std::marker::PhantomData,
        })
    }

    pub fn open(path: &Path) -> OperationResult<Self> {
        let file_name = Self::get_file_name(path);
        let mmap = open_write_mmap(&file_name)?;
        let header: MmapRange = *transmute_from_u8(mmap.get(0..std::mem::size_of::<MmapRange>()).unwrap());

        Ok(Self {
            file_name,
            mmap,
            ranges_offset: header.start as usize,
            points_count: header.count as usize,
            phantom: std::marker::PhantomData,
        })
    }

    fn flusher(&self) -> Flusher {
        self.mmap.flusher()
    }

    fn files(&self) -> Vec<PathBuf> {
        vec![self.file_name.clone()]
    }

    pub fn get_values(&self, point_id: PointOffsetType) -> OperationResult<impl Iterator<Item = T>> {
        let range = self.get_range(point_id)?;
        Ok(self.get_values_from_range(range))
    }

    pub fn remove_point(&mut self, point_id: PointOffsetType) -> OperationResult<Vec<T>> {
        let point_id = point_id as usize;
        let range: &mut MmapRange = transmute_from_u8_mut(self.mmap.get_mut(
            (point_id + 1) * std::mem::size_of::<MmapRange>()..(point_id + 2) * std::mem::size_of::<MmapRange>()
        ).unwrap());
        let values = self.get_values_from_range(range.clone()).collect_vec();
        *range = MmapRange { start: 0, count: 0 };
        Ok(values)
    }

    fn get_range(&self, point_id: PointOffsetType) -> OperationResult<MmapRange> {
        let point_id = point_id as usize;
        Ok(*transmute_from_u8(self.mmap.get(
            (point_id + 1) * std::mem::size_of::<MmapRange>()..(point_id + 2) * std::mem::size_of::<MmapRange>()
        ).unwrap()))
    }

    pub fn get_values_from_range(&self, range: MmapRange) -> impl Iterator<Item = T> {
        std::iter::successors(Some(range), |range| {
            todo!()
        })
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
        let mut point_to_values = MmapPointToValues::from_iter(dir.path(), values.iter().enumerate().map(|(id, values)| (id as PointOffsetType, values))).unwrap();

        let check = |point_to_values: &MmapPointToValues<_>, values: &[Vec<_>]| {
            for (idx, values) in values.iter().enumerate() {
                let v = point_to_values.get_values(idx as PointOffsetType).unwrap();
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
