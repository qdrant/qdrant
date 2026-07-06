//! Types for the IDF estimation API.
//!
//! The API exposes the IDF statistics used by sparse vector search with the
//! `idf` modifier: for the given query term indices it reports the document
//! count `N` and per-term document frequencies `df(term)`, along with the
//! IDF values derived from them — computed globally or over an *IDF corpus*
//! (the points matching a caller-supplied filter, see
//! [`IdfParams`](crate::types::IdfParams)).

use std::collections::HashMap;

use itertools::Itertools as _;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sparse::common::types::{DimId, DimWeight};
use validator::{Validate, ValidationError, ValidationErrors};

use crate::data_types::query_context::fancy_idf;
use crate::types::{Filter, VectorNameBuf};

/// Parameters of an IDF estimation request.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Validate, Hash)]
pub struct IdfEstimateParams {
    /// Name of the sparse vector the statistics are computed for.
    /// The vector must be configured with the `idf` modifier.
    pub using: VectorNameBuf,

    /// Sparse query the statistics are computed for. Document frequencies
    /// are reported for each of its term indices.
    #[validate(nested)]
    pub query: IdfEstimateQuery,

    /// Filter defining the corpus: IDF statistics are computed over the
    /// points matching this filter. If not specified, statistics are
    /// computed over the whole collection.
    #[validate(nested)]
    pub corpus: Option<Filter>,
}

impl IdfEstimateParams {
    /// Build params from bare query term indices.
    pub fn from_indices(using: VectorNameBuf, indices: Vec<DimId>, corpus: Option<Filter>) -> Self {
        Self {
            using,
            query: IdfEstimateQuery { indices },
            corpus,
        }
    }
}

/// Sparse query of an IDF estimation request. Only term indices: values of
/// a sparse vector do not participate in the statistics.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Hash)]
pub struct IdfEstimateQuery {
    /// Indices of the query terms the statistics are reported for.
    pub indices: Vec<DimId>,
}

impl Validate for IdfEstimateQuery {
    fn validate(&self) -> Result<(), ValidationErrors> {
        let Self { indices } = self;

        // Same constraint sparse vectors are validated against; duplicated
        // terms would report duplicated statistics.
        if indices.iter().unique().count() != indices.len() {
            let mut errors = ValidationErrors::new();
            errors.add("indices", ValidationError::new("must be unique"));
            return Err(errors);
        }
        Ok(())
    }
}

/// Raw IDF statistics of a single shard, aggregated across its segments.
///
/// Unlike [`IdfEstimate`], raw statistics can be summed across shards, so
/// this is what shards report to the coordinating node.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IdfStats {
    /// Number of documents the statistics are computed over: vectors of the
    /// points matching the corpus filter, or all indexed vectors for global
    /// statistics.
    pub document_count: usize,

    /// Document frequency per query term index.
    pub document_frequency: HashMap<DimId, usize>,
}

impl IdfStats {
    /// Fold statistics of another shard into this one.
    pub fn merge(&mut self, other: IdfStats) {
        let IdfStats {
            document_count,
            document_frequency,
        } = other;

        self.document_count += document_count;
        for (index, count) in document_frequency {
            *self.document_frequency.entry(index).or_default() += count;
        }
    }

    /// Derive the IDF estimate for the given query term indices.
    pub fn estimate(&self, indices: &[DimId]) -> IdfEstimate {
        let Self {
            document_count,
            document_frequency,
        } = self;

        let n = *document_count as DimWeight;
        let terms = indices
            .iter()
            .map(|&index| {
                let document_frequency =
                    document_frequency.get(&index).copied().unwrap_or_default();
                IdfTermEstimate {
                    index,
                    document_frequency,
                    idf: fancy_idf(n, document_frequency as DimWeight),
                }
            })
            .collect();

        IdfEstimate {
            document_count: *document_count,
            terms,
        }
    }
}

/// IDF statistics for a sparse query vector.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct IdfEstimate {
    /// Number of documents the statistics are computed over: points matching
    /// the corpus filter which have the sparse vector, or all points with the
    /// sparse vector for global statistics.
    pub document_count: usize,

    /// Per-term statistics, one entry per index of the query vector.
    pub terms: Vec<IdfTermEstimate>,
}

/// IDF statistics of a single query term.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct IdfTermEstimate {
    /// Index of the term in the sparse vector.
    pub index: DimId,

    /// Number of documents containing the term.
    pub document_frequency: usize,

    /// IDF value: the factor the term weight is multiplied by when searching
    /// with the `idf` modifier.
    pub idf: DimWeight,
}
