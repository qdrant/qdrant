use crate::vector_storage::vector_storage::{VectorStorage, ScoredPointOffset};
use crate::entry::entry_point::OperationResult;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use memmap::{MmapOptions, Mmap, MmapMut};
use std::mem::{size_of, transmute};
use crate::types::{VectorElementType, PointOffsetType, Distance};
use std::io::Write;
use crate::spaces::tools::{mertic_object, peek_top_scores};

pub struct MemmapVectorStorage {
    dim: usize,
    num_vectors: usize,
    mmap: Option<Mmap>,
    deleted_mmap: Option<MmapMut>,
    data_path: PathBuf,
    deleted_path: PathBuf,
    deleted_count: usize,
}

const HEADER_SIZE: usize = 4;

fn vf_to_u8<T>(v: &Vec<T>) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * size_of::<T>()) }
}


impl MemmapVectorStorage {
    fn ensure_data_file_exists(path: &Path) -> OperationResult<()> {
        if path.exists() {
            return Ok(());
        }
        let mut file = File::create(path)?;
        file.write(b"data")?;
        Ok(())
    }

    fn ensure_deleted_file_exists(path: &Path) -> OperationResult<()> {
        if path.exists() {
            return Ok(());
        }
        let mut file = File::create(path)?;
        file.write(b"drop")?;
        Ok(())
    }

    fn open_read(path: &Path) -> OperationResult<Mmap> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .append(true)
            .create(true)
            .open(path)?;

        let mmap = unsafe { MmapOptions::new().map(&file)? };
        return Ok(mmap);
    }

    fn open_write(path: &Path) -> OperationResult<MmapMut> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };
        return Ok(mmap);
    }


    pub fn open(path: &Path, dim: usize) -> OperationResult<Self> {
        let data_path = path.join("matrix.dat");
        let deleted_path = path.join("deleted.dat");

        MemmapVectorStorage::ensure_data_file_exists(data_path.as_path())?;
        MemmapVectorStorage::ensure_deleted_file_exists(deleted_path.as_path())?;

        let mmap = MemmapVectorStorage::open_read(&data_path)?;
        let num_vectors = (mmap.len() - HEADER_SIZE) / dim / size_of::<VectorElementType>();

        let deleted_mmap = MemmapVectorStorage::open_write(&deleted_path)?;

        let deleted_count = (HEADER_SIZE..deleted_mmap.len())
            .map(|idx| *deleted_mmap.get(idx).unwrap() as usize).sum();

        Ok(MemmapVectorStorage {
            dim,
            num_vectors,
            mmap: Some(mmap),
            deleted_mmap: Some(deleted_mmap),
            data_path,
            deleted_path,
            deleted_count,
        })
    }

    fn data_offset(&self, key: PointOffsetType) -> Option<usize> {
        let vector_data_length = self.dim * size_of::<VectorElementType>();
        let offset = key * vector_data_length + HEADER_SIZE;
        if key >= self.num_vectors {
            return None;
        }
        Some(offset)
    }

    fn raw_size(&self) -> usize {
        self.dim * size_of::<VectorElementType>()
    }

    fn raw_vector_offset(&self, offset: usize) -> &[VectorElementType] {
        let byte_slice = &self.mmap.as_ref().unwrap()[offset..(offset + self.raw_size())];
        let arr: &[VectorElementType] = unsafe { transmute(byte_slice) };
        return &arr[0..self.dim];
    }

    fn raw_vector(&self, key: PointOffsetType) -> Option<&[VectorElementType]> {
        self.data_offset(key).map(|offset| self.raw_vector_offset(offset))
    }

    fn deleted(&self, key: PointOffsetType) -> Option<bool> {
        self.deleted_mmap.as_ref().unwrap().get(HEADER_SIZE + key).map(|x| *x > 0)
    }
}


impl VectorStorage for MemmapVectorStorage {
    fn vector_dim(&self) -> usize {
        self.dim
    }

    fn vector_count(&self) -> usize {
        self.num_vectors - self.deleted_count
    }

    fn deleted_count(&self) -> usize {
        self.deleted_count
    }

    fn get_vector(&self, key: PointOffsetType) -> Option<Vec<VectorElementType>> {
        match self.deleted(key) {
            None => None,
            Some(false) => self.data_offset(key).map(|offset| {
                self.raw_vector_offset(offset).to_vec()
            }),
            Some(true) => None
        }
    }

    fn put_vector(&mut self, _vector: &Vec<VectorElementType>) -> OperationResult<PointOffsetType> {
        unimplemented!()
    }

    fn update_vector(&mut self, _key: usize, _vector: &Vec<f64>) -> OperationResult<usize> {
        unimplemented!()
    }

    fn update_from(&mut self, other: &dyn VectorStorage) -> OperationResult<Range<PointOffsetType>> {
        self.mmap = None;
        self.deleted_mmap = None;

        let start_index = self.num_vectors;
        let mut end_index = self.num_vectors;

        {
            let mut file = OpenOptions::new()
                .read(false)
                .write(false)
                .append(true)
                .create(false)
                .open(self.data_path.as_path())?;

            for id in other.iter_ids() {
                let vector = &other.get_vector(id).unwrap();
                let raw_bites = vf_to_u8(vector);
                file.write(raw_bites)?;
                end_index += 1;
            }

            file.flush()?;
        }
        {
            let mut file = OpenOptions::new()
                .read(false)
                .write(false)
                .append(true)
                .create(false)
                .open(self.deleted_path.as_path())?;

            let flags: Vec<u8> = vec![0; end_index - start_index];
            let flag_bytes = vf_to_u8(&flags);
            file.write(flag_bytes)?;
            file.flush()?;
        }


        let tmp_storage = Self::open(self.data_path.parent().unwrap(), self.dim)?;

        self.mmap = tmp_storage.mmap;
        self.deleted_mmap = tmp_storage.deleted_mmap;
        self.num_vectors = tmp_storage.num_vectors;
        self.deleted_count = tmp_storage.deleted_count;

        return Ok(start_index..end_index);
    }

    fn delete(&mut self, key: PointOffsetType) -> OperationResult<()> {
        if key < self.num_vectors {
            let mmap = self.deleted_mmap.as_mut().unwrap();
            let flag = mmap.get_mut(key + HEADER_SIZE).unwrap();

            *flag = 1;
            self.deleted_count += 1;
        }
        Ok(())
    }

    fn iter_ids(&self) -> Box<dyn Iterator<Item=PointOffsetType> + '_> {
        let iter = (0..self.num_vectors)
            .filter(move |id| !self.deleted(*id).unwrap());
        return Box::new(iter);
    }

    fn flush(&self) -> OperationResult<usize> {
        self.deleted_mmap.as_ref().unwrap().flush()?;
        Ok(0)
    }

    fn score_points(
        &self, vector: &Vec<VectorElementType>,
        points: &[PointOffsetType],
        top: usize,
        distance: &Distance,
    ) -> Vec<ScoredPointOffset> {
        let metric = mertic_object(distance);

        let scores: Vec<ScoredPointOffset> = points.iter()
            .cloned()
            .filter(|point| !self.deleted(*point).unwrap_or(true))
            .map(|point| {
                let other_vector = self.raw_vector(point).unwrap();
                ScoredPointOffset {
                    idx: point,
                    score: metric.similarity(vector, other_vector),
                }
            }).collect();
        return peek_top_scores(&scores, top, distance);
    }

    fn score_all(&self, vector: &Vec<VectorElementType>, top: usize, distance: &Distance) -> Vec<ScoredPointOffset> {
        let metric = mertic_object(distance);

        let scores: Vec<ScoredPointOffset> = self.iter_ids()
            .map(|point| {
                let other_vector = self.raw_vector(point).unwrap();
                ScoredPointOffset {
                    idx: point,
                    score: metric.similarity(vector, other_vector),
                }
            }).collect();

        return peek_top_scores(&scores, top, distance);
    }

    fn score_internal(
        &self,
        point: PointOffsetType,
        points: &[PointOffsetType],
        top: usize,
        distance: &Distance,
    ) -> Vec<ScoredPointOffset> {
        let vector = self.get_vector(point).unwrap();
        return self.score_points(&vector, points, top, distance);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use std::mem::transmute;
    use crate::vector_storage::simple_vector_storage::SimpleVectorStorage;

    #[test]
    fn test_basic_persistence() {
        let dir = TempDir::new("storage_dir").unwrap();
        let mut storage = MemmapVectorStorage::open(dir.path(), 4).unwrap();

        let vec1 = vec![1.0, 0.0, 1.0, 1.0];
        let vec2 = vec![1.0, 0.0, 1.0, 0.0];
        let vec3 = vec![1.0, 1.0, 1.0, 1.0];
        let vec4 = vec![1.0, 1.0, 0.0, 1.0];
        let vec5 = vec![1.0, 0.0, 0.0, 0.0];

        {
            let dir2 = TempDir::new("storage_dir2").unwrap();
            let mut storage2 = SimpleVectorStorage::open(dir2.path(), 4).unwrap();

            storage2.put_vector(&vec1).unwrap();
            storage2.put_vector(&vec2).unwrap();
            storage2.put_vector(&vec3).unwrap();
            storage.update_from(&storage2).unwrap();
        }

        assert_eq!(storage.vector_count(), 3);

        let vector = storage.get_vector(1).unwrap();

        assert_eq!(vec2, vector);

        assert_eq!(storage.deleted_count(), 0);

        storage.delete(2).unwrap();

        {
            let dir2 = TempDir::new("storage_dir2").unwrap();
            let mut storage2 = SimpleVectorStorage::open(dir2.path(), 4).unwrap();
            storage2.put_vector(&vec4).unwrap();
            storage2.put_vector(&vec5).unwrap();
            storage.update_from(&storage2).unwrap();
        }

        assert_eq!(storage.vector_count(), 4);


        let stored_ids: Vec<PointOffsetType> = storage.iter_ids().collect();

        assert_eq!(stored_ids, vec![0, 1, 3, 4]);


        let res = storage.score_all(&vec3, 2, &Distance::Dot);

        assert_eq!(res.len(), 2);

        assert_ne!(res[0].idx, 2);

        let res = storage.score_points(
            &vec3, &vec![0, 1, 2, 3, 4], 2, &Distance::Dot);

        assert_eq!(res.len(), 2);
        assert_ne!(res[0].idx, 2);
    }


    #[test]
    fn test_casts() {
        let data: Vec<VectorElementType> = vec![0.42, 0.069, 333.1, 100500.];

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