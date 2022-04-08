use crate::types::{PointOffsetType, VectorElementType};
use std::cmp::max;

pub struct ChunkedVectors {
    dim: usize,
    len: usize,
    chunk_capacity: usize,
    chunks: Vec<Vec<VectorElementType>>,
}

impl ChunkedVectors {
    pub fn new(dim: usize) -> ChunkedVectors {
        ChunkedVectors {
            dim,
            len: 0,
            chunk_capacity: 128,
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
            self.chunks.push(vec![0.; self.chunk_capacity * self.dim]);
        }

        let chunk_data = &mut self.chunks[key / self.chunk_capacity];
        let idx = (key % self.chunk_capacity) * self.dim;
        let data = &mut chunk_data[idx..idx + self.dim];
        data.clone_from_slice(vector);
    }
}
