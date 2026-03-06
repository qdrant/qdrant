mod count;
mod discovery;
mod facet;
mod local_shard;
mod matrix;
mod query;
mod recommend;
mod search;
mod update;

use std::fmt::Display;

use itertools::Itertools;
use segment::json_path::JsonPath;
use segment::types::{Filter, SearchParams, StrictModeConfig};
pub use shard::operation_rate_cost;

use super::types::{CollectionError, CollectionResult};
use crate::collection::Collection;

// Creates a new `VerificationPass` without actually verifying anything.
// This is useful in situations where we don't need to check for strict mode, but still
// want to be able to access `TableOfContent` using `.toc()`.
// If you're not implementing a new point-api endpoint for which a strict mode check
// is required, this is safe to use.
pub const fn new_unchecked_verification_pass() -> VerificationPass {
    VerificationPass { _inner: () }
}

/// A pass, created on successful verification.
pub struct VerificationPass {
    // Private field, so we can't instantiate it from somewhere else.
    _inner: (),
}

/// Trait to verify strict mode for requests.
/// This trait ignores the `enabled` parameter in `StrictModeConfig`.
pub trait StrictModeVerification {
    /// Implementing this method allows adding a custom check for request specific values.
    #[allow(async_fn_in_trait)]
    async fn check_custom(
        &self,
        _collection: &Collection,
        _strict_mode_config: &StrictModeConfig,
    ) -> CollectionResult<()> {
        Ok(())
    }

    /// Implement this to check the limit of a request.
    fn query_limit(&self) -> Option<usize>;

    /// Verifies that all keys in the given filter have an index available. Only implement this
    /// if the filter operates on a READ-operation, like search.
    /// For filtered updates implement `request_indexed_filter_write`!
    fn indexed_filter_read(&self) -> Option<&Filter>;

    /// Verifies that all keys in the given filter have an index available. Only implement this
    /// if the filter is used for filtered-UPDATES like delete by payload.
    /// For read only filters implement `request_indexed_filter_read`!
    fn indexed_filter_write(&self) -> Option<&Filter>;

    fn request_exact(&self) -> Option<bool>;

    fn request_search_params(&self) -> Option<&SearchParams>;

    /// Checks the 'exact' parameter.
    fn check_request_exact(&self, strict_mode_config: &StrictModeConfig) -> CollectionResult<()> {
        check_bool_opt(
            self.request_exact(),
            strict_mode_config.search_allow_exact,
            "Exact search",
            "exact",
        )
    }

    /// Checks the request limit.
    fn check_request_query_limit(
        &self,
        strict_mode_config: &StrictModeConfig,
    ) -> CollectionResult<()> {
        check_limit_opt(
            self.query_limit(),
            strict_mode_config.max_query_limit,
            "limit",
        )
    }

    /// Checks search parameters.
    #[allow(async_fn_in_trait)]
    async fn check_search_params(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> CollectionResult<()> {
        if let Some(search_params) = self.request_search_params() {
            Box::pin(search_params.check_strict_mode(collection, strict_mode_config)).await?;
        }
        Ok(())
    }

    // Checks all filters use indexed fields only.
    fn check_request_filter(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> CollectionResult<()> {
        let check_filter = |filter: Option<&Filter>,
                            allow_unindexed_filter: Option<bool>|
         -> CollectionResult<()> {
            let Some(filter) = filter else {
                return Ok(());
            };

            // Check for filter indices
            if allow_unindexed_filter == Some(false)
                && let Some((key, schemas)) = collection.one_unindexed_key(filter)
            {
                let possible_schemas_str = schemas
                    .iter()
                    .map(|schema| schema.to_string())
                    .sorted()
                    .dedup()
                    .collect::<Vec<_>>()
                    .join(", ");

                return Err(CollectionError::strict_mode(
                    format!(
                        "Index required but not found for \"{key}\" of one of the following types: [{possible_schemas_str}]",
                    ),
                    "Create an index for this key or use a different filter.",
                ));
            }

            check_filter_limits(filter, strict_mode_config)?;

            Ok(())
        };

        check_filter(
            self.indexed_filter_read(),
            strict_mode_config.unindexed_filtering_retrieve,
        )?;
        check_filter(
            self.indexed_filter_write(),
            strict_mode_config.unindexed_filtering_update,
        )?;

        Ok(())
    }

    /// Does the verification of all configured parameters. Only implement this function if you know what
    /// you are doing. In most cases implementing `check_custom` is sufficient.
    #[allow(async_fn_in_trait)]
    async fn check_strict_mode(
        &self,
        collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> CollectionResult<()> {
        self.check_custom(collection, strict_mode_config).await?;
        self.check_request_query_limit(strict_mode_config)?;
        self.check_request_filter(collection, strict_mode_config)?;
        self.check_request_exact(strict_mode_config)?;
        self.check_search_params(collection, strict_mode_config)
            .await?;
        Ok(())
    }
}

fn check_filter_limits(
    filter: &Filter,
    strict_mode_config: &StrictModeConfig,
) -> CollectionResult<()> {
    // Filter condition count limit
    if let Some(filter_condition_limit) = strict_mode_config.filter_max_conditions {
        let filter_conditions = filter.total_conditions_count();

        if !check_custom(|| Some(filter_conditions), Some(filter_condition_limit)) {
            return Err(CollectionError::strict_mode(
                format!(
                    "Filter condition limit reached ({filter_conditions} > {filter_condition_limit})",
                ),
                "Reduce the amount of conditions of your filter.",
            ));
        }
    }

    // Filter condition size limit
    if let Some(max_condition_size) = strict_mode_config.condition_max_size {
        let input_condition_size = filter.max_condition_input_size();

        if !check_custom(|| Some(input_condition_size), Some(max_condition_size)) {
            return Err(CollectionError::strict_mode(
                format!(
                    "Condition size limit reached ({input_condition_size} > {max_condition_size})"
                ),
                "Reduce the size of your condition.",
            ));
        }
    }

    Ok(())
}

pub fn check_timeout(
    timeout: usize,
    strict_mode_config: &StrictModeConfig,
) -> CollectionResult<()> {
    check_limit_opt(Some(timeout), strict_mode_config.max_timeout, "timeout")
}

pub(crate) fn check_bool_opt(
    value: Option<bool>,
    allowed: Option<bool>,
    name: &str,
    parameter: &str,
) -> CollectionResult<()> {
    if allowed != Some(false) || !value.unwrap_or_default() {
        return Ok(());
    }

    Err(CollectionError::strict_mode(
        format!("{name} disabled!"),
        format!("Set {parameter}=false."),
    ))
}

pub(crate) fn check_limit_opt<T: PartialOrd + Display>(
    value: Option<T>,
    limit: Option<T>,
    name: &str,
) -> CollectionResult<()> {
    let (Some(limit), Some(value)) = (limit, value) else {
        return Ok(());
    };

    if value > limit {
        return Err(CollectionError::strict_mode(
            format!("Limit exceeded {value} > {limit} for \"{name}\""),
            format!("Reduce the \"{name}\" parameter to or below {limit}."),
        ));
    }

    Ok(())
}

pub(crate) fn check_custom<T: PartialOrd>(
    value_fn: impl FnOnce() -> Option<T>,
    limit: Option<T>,
) -> bool {
    let Some(limit) = limit else {
        return true;
    };

    let Some(value) = value_fn() else {
        return true;
    };

    value <= limit
}

impl StrictModeVerification for SearchParams {
    async fn check_custom(
        &self,
        _collection: &Collection,
        strict_mode_config: &StrictModeConfig,
    ) -> CollectionResult<()> {
        check_limit_opt(
            self.quantization.and_then(|i| i.oversampling),
            strict_mode_config.search_max_oversampling,
            "oversampling",
        )?;

        check_limit_opt(
            self.hnsw_ef,
            strict_mode_config.search_max_hnsw_ef,
            "hnsw_ef",
        )?;
        Ok(())
    }

    fn request_exact(&self) -> Option<bool> {
        Some(self.exact)
    }

    fn query_limit(&self) -> Option<usize> {
        None
    }

    fn indexed_filter_read(&self) -> Option<&Filter> {
        None
    }

    fn indexed_filter_write(&self) -> Option<&Filter> {
        None
    }

    fn request_search_params(&self) -> Option<&SearchParams> {
        None
    }
}

pub fn check_grouping_field(
    group_by: &JsonPath,
    collection: &Collection,
    strict_mode_config: &StrictModeConfig,
) -> CollectionResult<()> {
    // check for unindexed fields targeted by group_by
    if strict_mode_config.unindexed_filtering_retrieve == Some(false) {
        // check the group_by field is indexed and support `match` statement
        if let Some(schema) = collection.payload_key_index_schema(group_by) {
            if !schema.supports_match() {
                let schema_kind = schema.kind();
                return Err(CollectionError::strict_mode(
                    format!("Index of type \"{schema_kind:?}\" found for \"{group_by}\""),
                    "Create an index supporting `match` for this key.",
                ));
            }
        } else {
            return Err(CollectionError::strict_mode(
                format!("Index required but not found for \"{group_by}\""),
                "Create an index supporting `match` for this key.",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common::budget::ResourceBudget;
    use common::counter::hardware_accumulator::HwMeasurementAcc;
    use segment::types::{
        Condition, FieldCondition, Filter, Match, PayloadFieldSchema, PayloadSchemaType,
        SearchParams, StrictModeConfig, ValueVariants,
    };
    use tempfile::Builder;

    use super::StrictModeVerification;
    use crate::collection::{Collection, RequestShardTransfer};
    use crate::config::{CollectionConfigInternal, CollectionParams, WalConfig};
    use crate::operations::point_ops::{FilterSelector, PointsSelector};
    use crate::operations::shared_storage_config::SharedStorageConfig;
    use crate::operations::types::{
        CollectionError, CountRequestInternal, DiscoverRequestInternal,
    };
    use crate::optimizers_builder::OptimizersConfig;
    use crate::shards::channel_service::ChannelService;
    use crate::shards::collection_shard_distribution::CollectionShardDistribution;
    use crate::shards::replica_set::{AbortShardTransfer, ChangePeerFromState};

    const UNINDEXED_KEY: &str = "key";
    const INDEXED_KEY: &str = "num";

    #[tokio::test(flavor = "multi_thread")]
    async fn test_strict_mode_verification_trait() {
        let collection = fixture().await;

        test_query_limit(&collection).await;
        test_search_params(&collection).await;
        test_filter_read(&collection).await;
        test_filter_write(&collection).await;
        test_request_exact(&collection).await;
    }

    async fn test_query_limit(collection: &Collection) {
        assert_strict_mode_error(discovery_fixture(Some(10), None, None), collection).await;
        assert_strict_mode_success(discovery_fixture(Some(4), None, None), collection).await;
    }

    async fn test_filter_read(collection: &Collection) {
        let filter = filter_fixture(UNINDEXED_KEY);
        assert_strict_mode_error(discovery_fixture(None, Some(filter), None), collection).await;

        let filter = filter_fixture(INDEXED_KEY);
        assert_strict_mode_success(discovery_fixture(None, Some(filter), None), collection).await;
    }

    async fn test_search_params(collection: &Collection) {
        let restricted_params = search_params_fixture(true);
        assert_strict_mode_error(
            discovery_fixture(None, None, Some(restricted_params)),
            collection,
        )
        .await;

        let allowed_params = search_params_fixture(false);
        assert_strict_mode_success(
            discovery_fixture(None, None, Some(allowed_params)),
            collection,
        )
        .await;
    }

    async fn test_filter_write(collection: &Collection) {
        let restricted_request = PointsSelector::FilterSelector(FilterSelector {
            filter: filter_fixture(UNINDEXED_KEY),
            shard_key: None,
        });
        assert_strict_mode_error(restricted_request, collection).await;

        let allowed_request = PointsSelector::FilterSelector(FilterSelector {
            filter: filter_fixture(INDEXED_KEY),
            shard_key: None,
        });
        assert_strict_mode_success(allowed_request, collection).await;
    }

    async fn test_request_exact(collection: &Collection) {
        let request = CountRequestInternal {
            filter: None,
            exact: true,
        };
        assert_strict_mode_error(request, collection).await;

        let request = CountRequestInternal {
            filter: None,
            exact: false,
        };
        assert_strict_mode_success(request, collection).await;
    }

    async fn assert_strict_mode_error<R: StrictModeVerification>(
        request: R,
        collection: &Collection,
    ) {
        let strict_mode_config = collection.strict_mode_config().await.unwrap();
        let error = request
            .check_strict_mode(collection, &strict_mode_config)
            .await
            .expect_err("Expected strict mode error but got Ok() value");
        if !matches!(error, CollectionError::StrictMode { .. }) {
            panic!("Expected strict mode error but got {error:#}");
        }
    }

    async fn assert_strict_mode_success<R: StrictModeVerification>(
        request: R,
        collection: &Collection,
    ) {
        let strict_mode_config = collection.strict_mode_config().await.unwrap();
        let res = request
            .check_strict_mode(collection, &strict_mode_config)
            .await;
        if let Err(CollectionError::StrictMode { description }) = res {
            panic!("Strict mode check should've passed but failed with error: {description:?}");
        } else if res.is_err() {
            panic!("Unexpected error");
        }
    }

    fn filter_fixture(key: &str) -> Filter {
        Filter::new_must(Condition::Field(FieldCondition::new_match(
            key.try_into().unwrap(),
            Match::new_value(ValueVariants::Integer(123)),
        )))
    }

    fn search_params_fixture(exact: bool) -> SearchParams {
        SearchParams {
            exact,
            ..SearchParams::default()
        }
    }

    fn discovery_fixture(
        limit: Option<usize>,
        filter: Option<Filter>,
        search_params: Option<SearchParams>,
    ) -> DiscoverRequestInternal {
        DiscoverRequestInternal {
            limit: limit.unwrap_or(0),
            filter,
            params: search_params,
            target: None,
            context: None,
            offset: None,
            with_payload: None,
            with_vector: None,
            using: None,
            lookup_from: None,
        }
    }

    async fn fixture() -> Collection {
        let strict_mode_config = StrictModeConfig {
            enabled: Some(true),
            max_timeout: Some(3),
            max_query_limit: Some(4),
            unindexed_filtering_update: Some(false),
            unindexed_filtering_retrieve: Some(false),
            search_max_hnsw_ef: Some(3),
            search_allow_exact: Some(false),
            search_max_oversampling: Some(0.2),
            ..Default::default()
        };

        fixture_collection(&strict_mode_config).await
    }

    async fn fixture_collection(strict_mode_config: &StrictModeConfig) -> Collection {
        let wal_config = WalConfig::default();
        let collection_params = CollectionParams::empty();

        let config = CollectionConfigInternal {
            params: collection_params,
            optimizer_config: OptimizersConfig::fixture(),
            wal_config,
            hnsw_config: Default::default(),
            quantization_config: Default::default(),
            strict_mode_config: Some(strict_mode_config.clone()),
            uuid: None,
            metadata: None,
        };

        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
        let snapshots_path = Builder::new().prefix("test_snapshots").tempdir().unwrap();

        let collection_name = "test".to_string();

        let storage_config: SharedStorageConfig = SharedStorageConfig::default();
        let storage_config = Arc::new(storage_config);

        let collection = Collection::new(
            collection_name.clone(),
            0,
            collection_dir.path(),
            snapshots_path.path(),
            &config,
            storage_config.clone(),
            CollectionShardDistribution::all_local(None, 0),
            None,
            ChannelService::default(),
            dummy_on_replica_failure(),
            dummy_request_shard_transfer(),
            dummy_abort_shard_transfer(),
            None,
            None,
            ResourceBudget::default(),
            None,
        )
        .await
        .expect("Failed to create new fixture collection");

        collection
            .create_payload_index(
                INDEXED_KEY.parse().unwrap(),
                PayloadFieldSchema::FieldType(PayloadSchemaType::Integer),
                HwMeasurementAcc::new(),
            )
            .await
            .expect("failed to create payload index");

        collection
    }

    pub fn dummy_on_replica_failure() -> ChangePeerFromState {
        Arc::new(move |_peer_id, _shard_id, _from_state| {})
    }

    pub fn dummy_request_shard_transfer() -> RequestShardTransfer {
        Arc::new(move |_transfer| {})
    }

    pub fn dummy_abort_shard_transfer() -> AbortShardTransfer {
        Arc::new(|_transfer, _reason| {})
    }
}
