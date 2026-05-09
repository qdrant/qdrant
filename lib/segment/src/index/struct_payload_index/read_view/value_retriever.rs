use std::collections::{HashMap, HashSet};

use common::counter::hardware_counter::HardwareCounterCell;

use super::StructPayloadIndexReadView;
use crate::id_tracker::IdTrackerRead;
use crate::index::query_optimization::payload_provider::PayloadProvider;
use crate::index::query_optimization::rescore_formula::value_retriever::{
    VariableRetrieverFn, variable_retriever,
};
use crate::json_path::JsonPath;
use crate::payload_storage::PayloadStorageRead;
use crate::vector_storage::VectorStorageRead;

impl<'a, P, I, V> StructPayloadIndexReadView<'a, P, I, V>
where
    P: PayloadStorageRead,
    I: IdTrackerRead,
    V: VectorStorageRead,
{
    /// Prepares optimized functions to extract each of the variables, given a point id.
    pub(crate) fn retrievers_map<'b, 'q>(
        &'b self,
        variables: HashSet<JsonPath>,
        hw_counter: &'q HardwareCounterCell,
    ) -> HashMap<JsonPath, VariableRetrieverFn<'q>>
    where
        'b: 'q,
    {
        let payload_provider = PayloadProvider::new(self.payload.clone());

        // prepare extraction of the variables from field indices or payload.
        let mut var_retrievers = HashMap::new();
        for key in variables {
            let payload_provider = payload_provider.clone();

            let retriever = variable_retriever(
                self.field_indexes,
                &key,
                payload_provider.clone(),
                hw_counter,
            );

            var_retrievers.insert(key, retriever);
        }

        var_retrievers
    }
}
