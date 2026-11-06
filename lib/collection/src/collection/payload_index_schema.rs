use std::path::{Path, PathBuf};

use common::ambient::AmbientFutureExt;
use common::reason::reason;
use common::save_on_disk::SaveOnDisk;
use itertools::Itertools;
use segment::json_path::JsonPath;
use segment::types::{Condition, Filter, Match, PayloadFieldSchema};
use shard::files::PAYLOAD_INDEX_CONFIG_FILE;
pub use shard::payload_index_schema::PayloadIndexSchema;

use crate::collection::Collection;
use crate::operations::types::{CollectionResult, UpdateResult};
use crate::operations::universal_query::formula::ExpressionInternal;
use crate::operations::{CollectionUpdateOperations, CreateIndex, FieldIndexOperations};
use crate::problems::unindexed_field;
use crate::shards::shard_trait::WaitUntil;

impl Collection {
    pub(crate) fn payload_index_file(collection_path: &Path) -> PathBuf {
        collection_path.join(PAYLOAD_INDEX_CONFIG_FILE)
    }

    pub(crate) fn load_payload_index_schema(
        collection_path: &Path,
    ) -> CollectionResult<SaveOnDisk<PayloadIndexSchema>> {
        let payload_index_file = Self::payload_index_file(collection_path);
        let schema: SaveOnDisk<PayloadIndexSchema> =
            SaveOnDisk::load_or_init_default(payload_index_file)?;
        Ok(schema)
    }

    pub async fn create_payload_index(
        &self,
        field_name: JsonPath,
        field_schema: PayloadFieldSchema,
    ) -> CollectionResult<Option<UpdateResult>> {
        // This function is called from consensus, so we use `wait = false`,
        // because we can't afford to wait for the result as indexing may take a long time
        self.create_payload_index_with_wait(field_name, field_schema, false)
            .await
    }

    pub async fn create_payload_index_with_wait(
        &self,
        field_name: JsonPath,
        field_schema: PayloadFieldSchema,
        wait: bool,
    ) -> CollectionResult<Option<UpdateResult>> {
        let field_schema = field_schema.normalized();
        self.payload_index_schema.write(|schema| {
            schema
                .schema
                .insert(field_name.clone(), field_schema.clone());
        })?;

        // This operation might be redundant, if we also create index as a regular collection op,
        // but it looks better in long term to also have it here, so
        // the creation of payload index may be eventually completely converted
        // into the consensus operation
        let create_index_operation = CollectionUpdateOperations::FieldIndexOperation(
            FieldIndexOperations::CreateIndex(CreateIndex {
                field_name,
                field_schema: Some(field_schema),
            }),
        );

        self.update_all_local(create_index_operation, WaitUntil::from(wait), false)
            .await
    }

    pub async fn drop_payload_index(
        &self,
        field_name: JsonPath,
    ) -> CollectionResult<Option<UpdateResult>> {
        self.payload_index_schema.write(|schema| {
            schema.schema.remove(&field_name);
        })?;

        let delete_index_operation = CollectionUpdateOperations::FieldIndexOperation(
            FieldIndexOperations::DeleteIndex(field_name),
        );

        let result = self
            .update_all_local(delete_index_operation, WaitUntil::from(false), false)
            .unmeasured(reason("Unmeasured API"))
            .await?;

        Ok(result)
    }

    pub fn payload_key_index_schema(&self, key: &JsonPath) -> Option<PayloadFieldSchema> {
        self.payload_index_schema.read().schema.get(key).cloned()
    }

    /// Returns an arbitrary payload key along with acceptable
    /// schemas used by `filter` which can be indexed but currently is not.
    /// If this function returns `None` all indexable keys in `filter` are indexed.
    pub fn one_unindexed_key(
        &self,
        filter: &Filter,
    ) -> Option<(JsonPath, Vec<PayloadFieldSchema>)> {
        one_unindexed_filter_key(&self.payload_index_schema.read(), filter)
    }

    pub fn one_unindexed_expression_key(
        &self,
        expr: &ExpressionInternal,
    ) -> Option<(JsonPath, Vec<PayloadFieldSchema>)> {
        one_unindexed_expression_key(&self.payload_index_schema.read(), expr)
    }

    /// Returns the tenant keys of the collection if `filter` does not restrict a read to tenants,
    /// sorted. Returns `None` if it does, or if the collection has no tenant index.
    ///
    /// A filter restricts a read to tenants if its top-level `must` contains a `value` or `any`
    /// match on a key indexed with `is_tenant: true`.
    pub fn tenant_keys_if_unscoped(&self, filter: Option<&Filter>) -> Option<Vec<JsonPath>> {
        let schema = self.payload_index_schema.read();

        let mut tenant_keys = schema
            .schema
            .iter()
            .filter(|(_, field_schema)| field_schema.is_tenant())
            .map(|(key, _)| key)
            .peekable();
        tenant_keys.peek()?;

        let scoped = filter
            .and_then(|filter| filter.must.as_ref())
            .into_iter()
            .flatten()
            .any(|condition| is_tenant_match(condition, &schema));
        if scoped {
            return None;
        }

        Some(tenant_keys.cloned().sorted().collect())
    }
}

/// Whether `condition` selects specific values of a tenant-indexed key.
fn is_tenant_match(condition: &Condition, schema: &PayloadIndexSchema) -> bool {
    let field = match condition {
        Condition::Field(field) => field,
        Condition::IsEmpty(_)
        | Condition::IsNull(_)
        | Condition::HasId(_)
        | Condition::HasVector(_)
        | Condition::Slice(_)
        | Condition::Nested(_)
        | Condition::Filter(_)
        | Condition::CustomIdChecker(_) => return false,
    };

    let selects_values = match &field.r#match {
        Some(Match::Value(_) | Match::Any(_)) => true,
        Some(
            Match::Text(_)
            | Match::TextAny(_)
            | Match::Phrase(_)
            | Match::Prefix(_)
            | Match::Substring(_)
            | Match::Except(_),
        )
        | None => false,
    };

    selects_values
        && schema
            .schema
            .get(&field.key)
            .is_some_and(PayloadFieldSchema::is_tenant)
}

enum PotentiallyUnindexed<'a> {
    Filter(&'a Filter),
    Expression(&'a ExpressionInternal),
}

/// Returns an arbitrary payload key with acceptable schemas
/// used by `filter` which can be indexed but currently is not.
/// If this function returns `None` all indexable keys in `filter` are indexed.
fn one_unindexed_key(
    schema: &PayloadIndexSchema,
    suspect: PotentiallyUnindexed<'_>,
) -> Option<(JsonPath, Vec<PayloadFieldSchema>)> {
    let mut extractor = unindexed_field::Extractor::new(&schema.schema);

    match suspect {
        PotentiallyUnindexed::Filter(filter) => {
            extractor.update_from_filter_once(None, filter);
        }
        PotentiallyUnindexed::Expression(expression) => {
            extractor.update_from_expression(expression);
        }
    }

    // Get the first unindexed field from the extractor.
    extractor
        .unindexed_schema()
        .iter()
        .next()
        .map(|(key, schema)| (key.clone(), schema.clone()))
}

/// Returns an arbitrary payload key with acceptable schemas
/// used by `filter` which can be indexed but currently is not.
/// If this function returns `None` all indexable keys in `filter` are indexed.
pub fn one_unindexed_filter_key(
    schema: &PayloadIndexSchema,
    filter: &Filter,
) -> Option<(JsonPath, Vec<PayloadFieldSchema>)> {
    one_unindexed_key(schema, PotentiallyUnindexed::Filter(filter))
}

pub fn one_unindexed_expression_key(
    schema: &PayloadIndexSchema,
    expr: &ExpressionInternal,
) -> Option<(JsonPath, Vec<PayloadFieldSchema>)> {
    one_unindexed_key(schema, PotentiallyUnindexed::Expression(expr))
}
