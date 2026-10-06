#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorIndexType {
    Plain,
    Hnsw { m: usize, payload_m: Option<usize> },
    Sparse,
}
