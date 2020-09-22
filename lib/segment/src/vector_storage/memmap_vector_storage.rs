use crate::vector_storage::vector_storage::VectorStorage;
use crate::entry::entry_point::OperationResult;
use std::ops::Range;
use std::path::Path;
use std::fs::{File, OpenOptions};
use memmap::{MmapOptions, Mmap};
use std::mem::size_of;
use crate::types::{VectorElementType, PointOffsetType};
use std::io::Write;

pub struct MemmapVectorStorage {
    dim: usize,
    num_vectors: usize,
    mmap: Mmap,
}

const HEADER_SIZE: usize = 4;


impl MemmapVectorStorage {
    fn ensure_data_file_exists(path: &Path) -> OperationResult<()> {
        if path.exists() {
            return Ok(());
        }
        let mut file = File::create(path)?;
        file.write(b"data");
        Ok(())
    }

    pub fn open(path: &Path, dim: usize) -> OperationResult<Self> {
        let data_path = path.join("matrix.dat");

        MemmapVectorStorage::ensure_data_file_exists(data_path.as_path())?;

        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .append(true)
            .create(true)
            .open(data_path)?;

        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let num_vectors = (mmap.len() - HEADER_SIZE) / dim / size_of::<VectorElementType>();

        Ok(MemmapVectorStorage {
            dim,
            num_vectors,
            mmap,
        })
    }

    fn data_offset(&self, key: PointOffsetType) -> Option<usize> {
        let vector_data_length = self.dim * size_of::<VectorElementType>();
        let max_offset = vector_data_length * self.num_vectors;
        let offset = key * vector_data_length + HEADER_SIZE;
        if offset + vector_data_length > max_offset {
            return None;
        }
        Some(offset)
    }

    fn raw_vector(&self, offset: usize) -> &[VectorElementType] {
        unimplemented!()
    }
}


impl VectorStorage for MemmapVectorStorage {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn vector_count(&self) -> usize {
        self.num_vectors
    }

    fn deleted_count(&self) -> usize {
        unimplemented!()
    }

    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>> {
        unimplemented!()
        // self.data_offset(key).map(|offset|)
    }

    fn put_vector(&mut self, vector: &Vec<VectorElementType>) -> OperationResult<PointOffsetType> {
        unimplemented!()
    }

    fn update_from(&mut self, other: &dyn VectorStorage) -> OperationResult<Range<PointOffsetType>> {
        unimplemented!()
    }

    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()> {
        unimplemented!()
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item=usize>> {
        unimplemented!()
    }

    fn flush(&self) -> OperationResult<usize> {
        unimplemented!()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use std::mem::transmute;

    #[test]
    fn test_basic_persistence() {
        let dir = TempDir::new("storage_dir").unwrap();


        let storage = MemmapVectorStorage::open(dir.path(), 10).unwrap();

        eprintln!("storage.vector_count = {:#?}", storage.vector_count());
    }

    fn vf_to_u8(v: &Vec<VectorElementType>) -> &[u8] {
        unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * size_of::<VectorElementType>()) }
    }


    #[test]
    fn test_casts() {

        let mut data: Vec<VectorElementType> = vec![0.42, 0.069, 333.1, 100500.];

        let raw_data = vf_to_u8(&data);

        eprintln!("raw_data.len() = {:#?}", raw_data.len());


        let arr: &[VectorElementType] = unsafe { transmute(raw_data) };

        let slice = &arr[0..data.len()];

        eprintln!("slice.len() = {:#?}", slice.len());

        for (idx, element) in slice.iter().enumerate() {
            println!("slice[{}]  = {:?}", idx, element);
        }

    }
}