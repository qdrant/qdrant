//! How the main graph of an HNSW index is built. Searches work the same way on graphs built by
//! any algorithm. Payload block graphs, healing, small segments and vectors an algorithm does not
//! support are always built by HNSW insertion, with `ef_construct`.
//!
//! Resolved by whoever starts the build, from the collection config and the feature flags, and
//! sent as is to a remote indexer, so that it builds what the local build would, whatever its
//! own version and flags.

/// The algorithm of the main graph, with its parameters.
///
/// A new algorithm, or a version of one with other parameters, is a new variant here and a new
/// case of the `IndexingGraphAlgorithm` oneof of remote indexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MainGraphAlgorithm {
    /// Insert the points one by one. `ef_construct` controls the quality.
    Hnsw,
    /// PiPNN where it applies (large enough segments, vectors with a matrix scorer, no GPU),
    /// HNSW insertion otherwise.
    Pipnn(PipnnSettings),
}

/// The tunable parameters of a PiPNN build. The partitioning keeps its defaults, and the seed
/// is drawn by each build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipnnSettings {
    /// Nearest neighbors each point keeps per leaf.
    pub leaf_k: usize,
    /// Reservoir capacity per point, as a multiple of the level's degree limit.
    pub reservoir_factor: usize,
}

impl Default for PipnnSettings {
    /// The paper's default leaf k is 2, and it finds that "3 or 4 would also suffice"
    /// (appendix A.3). It found reservoirs of 64 to 192 slots for a degree of 64 about equally
    /// good with a final prune (appendix A.5).
    fn default() -> Self {
        Self {
            leaf_k: 4,
            reservoir_factor: 2,
        }
    }
}
