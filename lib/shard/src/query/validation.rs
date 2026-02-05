use segment::common::operation_error::{OperationError, OperationResult};

use super::planned_query::{MergePlan, RescoreStages, Source};
use super::{FusionInternal, ScoringQuery};

impl MergePlan {
    /// Creates a new MergePlan and validates it.
    ///
    /// Validation includes:
    /// - If fusion with weights is used, the number of weights must match the number of sources.
    pub fn new(
        sources: Vec<Source>,
        rescore_stages: Option<RescoreStages>,
    ) -> OperationResult<Self> {
        let merge_plan = Self {
            sources,
            rescore_stages,
        };
        merge_plan.validate()?;
        Ok(merge_plan)
    }

    /// Validates the MergePlan.
    fn validate(&self) -> OperationResult<()> {
        // Validate fusion weights at all levels
        self.validate_fusion_weights()?;

        // Recursively validate nested prefetches
        for source in &self.sources {
            match source {
                Source::SearchesIdx(_) => {}
                Source::ScrollsIdx(_) => {}
                Source::Prefetch(nested_plan) => nested_plan.validate()?,
            }
        }

        Ok(())
    }

    fn validate_fusion_weights(&self) -> OperationResult<()> {
        let Self {
            sources,
            rescore_stages,
        } = self;

        if let Some(stages) = rescore_stages {
            let RescoreStages {
                shard_level,
                collection_level,
            } = stages;

            if let Some(shard_level) = shard_level {
                validate_query(&shard_level.rescore, sources)?;
            }
            if let Some(collection_level) = collection_level {
                validate_query(&collection_level.rescore, sources)?;
            }
        }
        Ok(())
    }
}

fn validate_query(query: &ScoringQuery, sources: &[Source]) -> OperationResult<()> {
    match query {
        ScoringQuery::Vector(_) => Ok(()),
        ScoringQuery::Fusion(fusion) => validate_fusion(fusion, sources.len()),
        ScoringQuery::OrderBy(_) => Ok(()),
        ScoringQuery::Formula(_) => Ok(()),
        ScoringQuery::Sample(_) => Ok(()),
        ScoringQuery::Mmr(_) => Ok(()),
    }
}

fn validate_fusion(fusion: &FusionInternal, num_sources: usize) -> OperationResult<()> {
    match fusion {
        FusionInternal::Rrf {
            k: _,
            weights: Some(weights),
        } => {
            if weights.len() != num_sources {
                return Err(OperationError::validation_error(format!(
                    "RRF weights length ({}) does not match number of prefetches ({})",
                    weights.len(),
                    num_sources
                )));
            }
            Ok(())
        }
        FusionInternal::Rrf {
            k: _,
            weights: None,
        } => Ok(()),
        FusionInternal::Dbsf => Ok(()),
    }
}
