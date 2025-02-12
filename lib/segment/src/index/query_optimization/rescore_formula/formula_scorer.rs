use std::collections::HashMap;

use common::types::{PointOffsetType, ScoreType};
use serde_json::Value;

use super::parsed_formula::{ConditionId, Expression, VariableId};
use super::value_retriever::VariableRetrieverFn;
use crate::common::operation_error::OperationResult;
use crate::index::query_optimization::optimized_filter::ConditionCheckerFn;
use crate::json_path::JsonPath;

const DEFAULT_SCORE: ScoreType = 0.0;

pub struct FormulaScorer<'a> {
    /// The formula to evaluate
    formula: Expression,
    /// One hashmap for each prefetch results
    scores: &'a [HashMap<PointOffsetType, ScoreType>],
    /// Payload key -> retriever function
    payload_retrievers: HashMap<JsonPath, VariableRetrieverFn<'a>>,
    /// Condition id -> checker function
    condition_checkers: HashMap<ConditionId, ConditionCheckerFn<'a>>,
    /// Default values for all variables
    defaults: HashMap<VariableId, Value>,
}

impl FormulaScorer<'_> {
    pub fn score(&self, point_id: PointOffsetType) -> OperationResult<ScoreType> {
        let mut variables = HashMap::new();
        for (path, retriever) in &self.payload_retrievers {
            if let Some(value) = retriever(point_id) {
                variables.insert(path.clone(), value);
            }
        }

        let mut conditions = HashMap::new();
        for (condition_id, checker) in &self.condition_checkers {
            let result = checker(point_id);
            conditions.insert(*condition_id, result);
        }

        let mut scores = Vec::with_capacity(self.scores.len());
        for (query_idx, score_map) in self.scores.iter().enumerate() {
            let score = score_map
                .get(&point_id)
                .copied()
                .or_else(|| {
                    self.defaults
                        .get(&VariableId::Score(query_idx))
                        // if parsing fails, we just use default score
                        .and_then(|v| v.as_f64())
                        .map(|v| v as ScoreType)
                })
                .unwrap_or(DEFAULT_SCORE);
            scores.push(score);
        }

        self.formula
            .evaluate(&scores, &variables, &conditions, &self.defaults)
    }
}
