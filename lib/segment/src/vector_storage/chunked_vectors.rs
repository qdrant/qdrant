use std::cmp::max;
use std::mem;

use crate::types::{PointOffsetType, VectorElementType};

type Chunk = Vec<VectorElementType>;

// chunk size in bytes
const CHUNK_SIZE: usize = 32 * 1024 * 1024;

// if dimension is too high, use this capacity
const MIN_CHUNK_CAPACITY: usize = 16;

pub struct ChunkedVectors {
    dim: usize,
    len: usize,            // amount of stored vectors
    chunk_capacity: usize, // max amount of vectors in each chunk
    chunks: Vec<Chunk>,
}

impl ChunkedVectors {
    pub fn new(dim: usize) -> ChunkedVectors {
        assert_ne!(dim, 0, "The vector's dimension cannot be 0");
        let vector_size = dim * mem::size_of::<VectorElementType>();
        let chunk_capacity = max(MIN_CHUNK_CAPACITY, CHUNK_SIZE / vector_size);
        ChunkedVectors {
            dim,
            len: 0,
            chunk_capacity,
            chunks: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, key: PointOffsetType) -> &[VectorElementType] {
        let key = key as usize;
        let chunk_data = &self.chunks[key / self.chunk_capacity];
        let idx = (key % self.chunk_capacity) * self.dim;
        &chunk_data[idx..idx + self.dim]
    }

    pub fn push(&mut self, vector: &[VectorElementType]) -> PointOffsetType {
        let new_id = self.len as PointOffsetType;
        self.insert(new_id, vector);
        new_id
    }

    pub fn insert(&mut self, key: PointOffsetType, vector: &[VectorElementType]) {
        let key = key as usize;
        self.len = max(self.len, key + 1);
        while self.chunks.len() * self.chunk_capacity < self.len {
            self.chunks.push(vec![]);
        }

        let chunk_data = &mut self.chunks[key / self.chunk_capacity];
        let idx = (key % self.chunk_capacity) * self.dim;
        if chunk_data.len() < idx + self.dim {
            chunk_data.resize(idx + self.dim, 0.);
        }
        let data = &mut chunk_data[idx..idx + self.dim];
        data.copy_from_slice(vector);
    }
}
