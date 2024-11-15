pub mod cluster_api;
pub mod collections_api;
pub mod count_api;
pub mod debug_api;
pub mod discovery_api;
pub mod facet_api;
pub mod issues_api;
pub mod local_shard_api;
pub mod query_api;
pub mod read_params;
pub mod recommend_api;
pub mod retrieve_api;
pub mod search_api;
pub mod service_api;
pub mod shards_api;
pub mod snapshot_api;
pub mod update_api;

use common::counter::hardware_accumulator::HwMeasurementAcc;
use common::validation::validate_collection_name;
use serde::Deserialize;
use validator::Validate;

use crate::settings::ServiceConfig;

/// A collection path with stricter validation
///
/// Validation for collection paths has been made more strict over time.
/// To prevent breaking changes on existing collections, this is only enforced for newly created
/// collections. Basic validation is enforced everywhere else.
#[derive(Deserialize, Validate)]
struct StrictCollectionPath {
    #[validate(
        length(min = 1, max = 255),
        custom(function = "validate_collection_name")
    )]
    name: String,
}

/// A collection path with basic validation
///
/// Validation for collection paths has been made more strict over time.
/// To prevent breaking changes on existing collections, this is only enforced for newly created
/// collections. Basic validation is enforced everywhere else.
#[derive(Deserialize, Validate)]
struct CollectionPath {
    #[validate(length(min = 1, max = 255))]
    name: String,
}

pub(crate) fn hardware_opt<'a>(
    config: &ServiceConfig,
    hw_counter: &'a HwMeasurementAcc,
) -> Option<&'a HwMeasurementAcc> {
    if config.hardware_reporting() {
        Some(hw_counter)
    } else {
        hw_counter.discard();
        None
    }
}
