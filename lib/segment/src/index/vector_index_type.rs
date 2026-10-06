/// Type and graph parameters of an opened vector index.
///
/// This describes the index itself, independently of the collection's optimizer configuration
/// and the number of indexed vectors. Inspecting it does not load deferred index files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorIndexType {
    Plain,
    Hnsw { m: usize, payload_m: Option<usize> },
    Sparse,
}
