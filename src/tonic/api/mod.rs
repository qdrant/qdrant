pub mod collections_api;
mod collections_common;
pub mod collections_internal_api;
pub mod points_api;
mod points_common;
pub mod points_internal_api;
pub mod raft_api;
pub mod snapshots_api;

use tonic::Status;
use validator::Validate;

/// Validate the given request. Returns validation error on failure.
fn validate(request: &dyn Validate) -> Result<(), Status> {
    // TODO: nicely format once <https://github.com/qdrant/qdrant/pull/1463/files> is merged
    request
        .validate()
        .map_err(|err| Status::invalid_argument(format!("Validation error in body: {}", err)))
}
