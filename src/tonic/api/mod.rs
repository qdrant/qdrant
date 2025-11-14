pub mod collections_api;
pub mod collections_internal_api;
pub mod points_api;
pub mod points_internal_api;
pub mod raft_api;
pub mod snapshots_api;

mod collections_common;
mod query_common;
mod update_common;

use std::time::Duration;

use collection::operations::validation;
use tonic::Status;
use validator::Validate;

use crate::actix::api::read_params::HOUR_IN_SECONDS;

/// Validate the given request and fail on error.
///
/// Returns validation error on failure.
fn validate(request: &impl Validate) -> Result<(), Status> {
    request.validate().map_err(|ref err| {
        Status::invalid_argument(validation::label_errors("Validation error in body", err))
    })
}

/// Validate the given request. Returns validation error on failure.
fn validate_and_log(request: &impl Validate) {
    if let Err(ref err) = request.validate() {
        validation::warn_validation_errors("Internal gRPC", err);
    }
}

/// Converts the given `seconds` into `Duration` and limits it to 1h.
pub(crate) fn limit_timeout(seconds: u64) -> Duration {
    Duration::from_secs(std::cmp::min(seconds, HOUR_IN_SECONDS))
}

/// Converts the given `seconds` into `Duration` and limits it to 1h.
pub(crate) fn limit_timeout_opt(seconds: Option<u64>) -> Option<Duration> {
    seconds.map(limit_timeout)
}

#[cfg(test)]
mod tests {
    use validator::Validate;

    use super::*;

    #[derive(Validate, Debug)]
    struct SomeThing {
        #[validate(range(min = 1))]
        pub idx: usize,
    }

    #[derive(Validate, Debug)]
    struct OtherThing {
        #[validate(nested)]
        pub things: Vec<SomeThing>,
    }

    #[test]
    fn test_validation() {
        use tonic::Code;

        let bad_config = OtherThing {
            things: vec![SomeThing { idx: 0 }],
        };

        let validation =
            validate(&bad_config).expect_err("validation of bad request payload should fail");
        assert_eq!(validation.code(), Code::InvalidArgument);
        assert_eq!(
            validation.message(),
            "Validation error in body: [things[0].idx: value 0 invalid, must be 1 or larger]"
        )
    }
}
