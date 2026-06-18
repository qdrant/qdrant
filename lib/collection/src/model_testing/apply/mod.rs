mod reads;
mod writes;

use std::collections::{BTreeSet, HashMap};

use common::counter::hardware_accumulator::HwMeasurementAcc;
use segment::types::VectorNameBuf;

use super::op::{NamedVectors, Op};
use super::{Model, VectorValue};
use crate::collection::Collection;
use crate::operations::CollectionUpdateOperations;
use crate::operations::point_ops::{VectorPersisted, VectorStructPersisted, WriteOrdering};

pub(super) async fn apply(
    collection: &Collection,
    model: &mut Model,
    active: &mut BTreeSet<VectorNameBuf>,
    op: &Op,
) {
    match op {
        Op::Upsert(id, vecs, payload) => {
            writes::apply_upsert(collection, model, *id, vecs, payload).await
        }
        Op::UpsertBatch(points) => writes::apply_upsert_batch(collection, model, points).await,
        Op::Delete(ids) => writes::apply_delete(collection, model, ids).await,
        Op::DeleteByFilter(num) => writes::apply_delete_by_filter(collection, model, *num).await,
        Op::SetPayload(ids, payload) => {
            writes::apply_set_payload(collection, model, ids, payload).await
        }
        Op::OverwritePayload(ids, payload) => {
            writes::apply_overwrite_payload(collection, model, ids, payload).await
        }
        Op::DeletePayload(ids, keys) => {
            writes::apply_delete_payload(collection, model, ids, keys).await
        }
        Op::ClearPayload(ids) => writes::apply_clear_payload(collection, model, ids).await,
        Op::CreateIndex(field, schema) => {
            writes::apply_create_index(collection, field, schema).await
        }
        Op::DropIndex(field) => writes::apply_drop_index(collection, field).await,
        Op::RetrieveRandom(ids) => {
            reads::apply_retrieve(collection, model, ids, "RetrieveRandom").await
        }
        Op::RetrieveExisting(ids) => {
            reads::apply_retrieve(collection, model, ids, "RetrieveExisting").await
        }
        Op::Search {
            vector_name,
            query,
            limit,
            exact,
            filter_num,
        } => {
            reads::apply_search(
                collection,
                model,
                vector_name,
                query,
                *limit,
                *exact,
                *filter_num,
            )
            .await
        }
        Op::Query {
            vector_name,
            query,
            limit,
            exact,
            filter_num,
        } => {
            reads::apply_query(
                collection,
                model,
                vector_name,
                query,
                *limit,
                *exact,
                *filter_num,
            )
            .await
        }
        Op::CountByNum(num) => reads::apply_count_by_num(collection, model, *num).await,
        Op::UpsertConditional {
            points,
            condition_num,
            mode,
        } => {
            writes::apply_upsert_conditional(collection, model, points, *condition_num, *mode).await
        }
        Op::UpdateVectors {
            points,
            condition_num,
        } => writes::apply_update_vectors(collection, model, points, *condition_num).await,
        Op::DeleteVectors { ids, names } => {
            writes::apply_delete_vectors(collection, model, ids, names).await
        }
        Op::DeleteVectorsByFilter { num, names } => {
            writes::apply_delete_vectors_by_filter(collection, model, *num, names).await
        }
        Op::ScrollFilteredByNum(num) => {
            reads::apply_scroll_filtered_by_num(collection, model, *num).await
        }
        Op::CountByTag(tag) => reads::apply_count_by_tag(collection, model, tag).await,
        Op::ScrollFilteredByTag(tag) => {
            reads::apply_scroll_filtered_by_tag(collection, model, tag).await
        }
        Op::ScrollOrdered(direction) => {
            reads::apply_scroll_ordered(collection, model, *direction).await
        }
        Op::Recommend {
            positive,
            negative,
            limit,
            strategy,
            vector_name,
        } => {
            reads::apply_recommend(
                collection,
                model,
                positive,
                negative,
                *limit,
                *strategy,
                vector_name,
            )
            .await
        }
        Op::CreateVectorName { name, config } => {
            writes::apply_create_vector_name(collection, active, name, config).await
        }
        Op::DeleteVectorName(name) => {
            writes::apply_delete_vector_name(collection, model, active, name).await
        }
        Op::SetPayloadByFilter { num, payload } => {
            writes::apply_set_payload_by_filter(collection, model, *num, payload).await
        }
        Op::OverwritePayloadByFilter { num, payload } => {
            writes::apply_overwrite_payload_by_filter(collection, model, *num, payload).await
        }
        Op::DeletePayloadByFilter { num, keys } => {
            writes::apply_delete_payload_by_filter(collection, model, *num, keys).await
        }
        Op::ClearPayloadByFilter(num) => {
            writes::apply_clear_payload_by_filter(collection, model, *num).await
        }
        Op::Facet { key, filter_num } => {
            reads::apply_facet(collection, model, key, *filter_num).await
        }
        Op::SetPayloadByKey { ids, payload, key } => {
            writes::apply_set_payload_by_key(collection, model, ids, payload, key).await
        }
        Op::RetrieveSelective {
            ids,
            with_payload,
            with_vector,
        } => {
            reads::apply_retrieve_selective(collection, model, ids, with_payload, with_vector).await
        }
        Op::ScrollPaged { limit, filter } => {
            reads::apply_scroll_paged(collection, model, *limit, filter).await
        }
    }
}

// ───── shared utilities ─────────────────────────────────────────────────────

/// Submit a write op through the standard path and panic with the engine error on failure.
async fn apply_update(collection: &Collection, op: CollectionUpdateOperations, ctx: &str) {
    collection
        .update_from_client_simple(
            op,
            true,
            None,
            WriteOrdering::default(),
            HwMeasurementAcc::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{ctx} failed: {e:?}"));
}

/// Convert the test-side `NamedVectors` into the on-the-wire `VectorStructPersisted::Named`.
fn to_named_persisted(vecs: &NamedVectors) -> VectorStructPersisted {
    let mut map: HashMap<VectorNameBuf, VectorPersisted> = HashMap::new();
    for (name, value) in vecs {
        let persisted = match value {
            VectorValue::Dense(v) => VectorPersisted::Dense(v.clone()),
            VectorValue::Sparse(s) => VectorPersisted::Sparse(s.clone()),
            VectorValue::MultiDense(matrix) => VectorPersisted::MultiDense(matrix.clone()),
        };
        map.insert(name.clone(), persisted);
    }
    VectorStructPersisted::Named(map)
}
