//! Fluent builder for [`Prefetch`].
//!
//! Builder fields mirror [`Prefetch`] explicitly so adding a field
//! to the target struct forces a compile error here.

use common::types::ScoreType;
use segment::types::{Filter, SearchParams};
use shard::query::ScoringQuery;

use crate::requests::query::Prefetch;

/// Fluent builder for [`Prefetch`].
///
/// `limit` is required and passed through [`Self::new`]; every other field is
/// optional and falls back to the [`Prefetch::new`] defaults.
#[derive(Clone, Debug)]
pub struct PrefetchBuilder {
    prefetches: Vec<Prefetch>,
    query: Option<ScoringQuery>,
    limit: usize,
    params: Option<SearchParams>,
    filter: Option<Filter>,
    score_threshold: Option<ScoreType>,
}

impl PrefetchBuilder {
    pub fn new(limit: usize) -> Self {
        let Prefetch {
            prefetches,
            query,
            limit,
            params,
            filter,
            score_threshold,
        } = Prefetch::new(limit);
        Self {
            prefetches,
            query,
            limit,
            params,
            filter,
            score_threshold,
        }
    }

    /// Replaces the whole nested prefetch list; see [`Self::add_prefetch`] to append one stage.
    pub fn prefetches(mut self, prefetches: Vec<Prefetch>) -> Self {
        self.prefetches = prefetches;
        self
    }

    pub fn add_prefetch(mut self, prefetch: Prefetch) -> Self {
        self.prefetches.push(prefetch);
        self
    }

    pub fn query(mut self, query: ScoringQuery) -> Self {
        self.query = Some(query);
        self
    }

    pub fn params(mut self, params: SearchParams) -> Self {
        self.params = Some(params);
        self
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn score_threshold(mut self, score_threshold: ScoreType) -> Self {
        self.score_threshold = Some(score_threshold);
        self
    }

    pub fn build(self) -> Prefetch {
        // Exhaustively destructure Self and construct Prefetch:
        // adding a field to either type forces a compile error here.
        let Self {
            prefetches,
            query,
            limit,
            params,
            filter,
            score_threshold,
        } = self;
        Prefetch {
            prefetches,
            query,
            limit,
            params,
            filter,
            score_threshold,
        }
    }
}
