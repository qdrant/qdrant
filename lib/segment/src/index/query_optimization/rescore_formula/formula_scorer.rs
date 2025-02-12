use std::collections::HashMap;

use common::counter::hardware_counter::HardwareCounterCell;
use common::types::{PointOffsetType, ScoreType};
use serde_json::Value;

use super::parsed_formula::{Expression, ParsedFormula, VariableId};
use super::value_retriever::VariableRetrieverFn;
use crate::common::operation_error::OperationResult;
use crate::index::query_optimization::optimized_filter::{check_condition, OptimizedCondition};
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::struct_payload_index::StructPayloadIndex;
use crate::json_path::JsonPath;

const DEFAULT_SCORE: ScoreType = 0.0;

/// A scorer to evaluate the same formula for many points
pub struct FormulaScorer<'a> {
    /// The formula to evaluate
    formula: Expression,
    /// One hashmap for each prefetch results
    prefetches_scores: &'a [HashMap<PointOffsetType, ScoreType>],
    /// Payload key -> retriever function
    payload_retrievers: HashMap<JsonPath, VariableRetrieverFn<'a>>,
    /// Condition id -> checker function
    condition_checkers: Vec<OptimizedCondition<'a>>,
    /// Default values for all variables
    defaults: HashMap<VariableId, Value>,
}

/// Ephemeral type to hold all formula variables for a single point
struct PointVariables<'a> {
    /// The scores in each prefetch for the point.
    prefetch_scores: Vec<Option<ScoreType>>,
    /// The retrieved values for the point. Not all jsonpaths in the formula may be present here.
    payload_values: HashMap<JsonPath, Value>,
    /// The evaluated conditions for the point. All conditions in the formula must be present.
    conditions: Vec<bool>,
    /// The default values for all variables
    defaults: &'a HashMap<VariableId, Value>,
}

impl StructPayloadIndex {
    pub fn formula_scorer<'s, 'q>(
        &'s self,
        parsed_formula: &'q ParsedFormula,
        prefetches_scores: &'q [HashMap<PointOffsetType, ScoreType>],
        hw_counter: &'q HardwareCounterCell,
    ) -> FormulaScorer<'q>
    where
        's: 'q,
    {
        let ParsedFormula {
            payload_vars,
            conditions,
            defaults,
            formula,
        } = parsed_formula;

        let payload_retrievers = self.retrievers_map(payload_vars.clone(), hw_counter);

        let payload_provider = PayloadProvider::new(self.payload.clone());
        let total = self.available_point_count();
        let condition_checkers = self
            .convert_conditions(conditions, payload_provider, total, hw_counter)
            .into_iter()
            .map(|(checker, _estimation)| checker)
            .collect();

        FormulaScorer {
            formula: formula.clone(),
            prefetches_scores,
            payload_retrievers,
            condition_checkers,
            defaults: defaults.clone(),
        }
    }
}

impl FormulaScorer<'_> {
    /// Evaluate the formula for the given point
    pub fn score(&self, point_id: PointOffsetType) -> OperationResult<ScoreType> {
        // Collect all variables
        let mut payload_values = HashMap::new();
        for (path, retriever) in &self.payload_retrievers {
            if let Some(value) = retriever(point_id) {
                payload_values.insert(path.clone(), value);
            }
        }

        // Collect all evaluated conditions
        let conditions = self
            .condition_checkers
            .iter()
            .map(|checker| check_condition(checker, point_id))
            .collect();

        // Collect all scores for this point in the prefetch results
        let mut prefetch_scores = Vec::with_capacity(self.prefetches_scores.len());
        for score_map in self.prefetches_scores.iter() {
            let score = score_map.get(&point_id).copied();
            prefetch_scores.push(score);
        }

        let _vars = PointVariables {
            prefetch_scores,
            payload_values,
            conditions,
            defaults: &self.defaults,
        };

        todo!() // self.formula.evaluate(&vars)
    }
}
