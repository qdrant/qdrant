use std::path::Path;

use common::types::PointOffsetType;

use crate::{common::operation_error::{OperationError, OperationResult}, types::{FloatPayloadType, GeoPoint, IntPayloadType}};

use super::immutable_point_to_values::ImmutablePointToValues;

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
    phantom: std::marker::PhantomData<T>,
}

impl<T: MmapValue> MmapPointToValues<T> {
    pub fn from_vec(path: &Path, src: Vec<Vec<T>>) -> OperationResult<Self> {
        todo!()
    }

    pub fn from_immutable(path: &Path, src: ImmutablePointToValues<T>) -> OperationResult<Self> {
        todo!()
    }

    pub fn get_values(&self, idx: PointOffsetType) -> Option<Vec<T>> {
        todo!()
    }

    pub fn remove_point(&mut self, idx: PointOffsetType) -> Option<Vec<T>> {
        todo!()
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
        let mut point_to_values = MmapPointToValues::from_vec(dir.path(), values.clone()).unwrap();

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
