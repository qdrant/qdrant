use std::sync::Arc;

use segment::types::{WithPayloadInterface, WithVector};

use super::{query_enum::QueryEnum, types::CoreSearchRequestBatch};

pub enum Rescore {
    /// Rescore points against a vector
    Vector(QueryEnum),
}

pub struct Merge {
    /// Alter the scores before selecting the best limit
    pub rescore: Option<Rescore>,

    /// Keep this much points from the top
    pub limit: usize,
}

pub enum Source {
    /// A reference offset into the main search batch
    Idx(usize),

    /// A nested prefetch
    Prefetch(Prefetch),
}

pub struct Prefetch {
    /// Gather all these sources
    pub sources: Vec<Source>,

    /// How to merge the sources
    pub merge: Merge,
}

pub struct PlannedQuery {
    pub merge_plan: Prefetch,
    pub batch: Arc<CoreSearchRequestBatch>,
    pub offset: usize,
    pub with_vector: WithVector,
    pub with_payload: WithPayloadInterface,
}
