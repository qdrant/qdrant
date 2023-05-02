pub mod collections_api;
mod collections_common;
pub mod collections_internal_api;
pub mod points_api;
mod points_common;
pub mod points_internal_api;
pub mod raft_api;
pub mod snapshots_api;

use collection::operations::validation;
use tonic::Status;
use validator::Validate;

/// Validate the given request and fail on error.
///
/// Returns validation error on failure.
fn validate(request: &dyn Validate) -> Result<(), Status> {
    request.validate().map_err(|ref err| {
        Status::invalid_argument(validation::label_errors("Validation error in body", err))
    })
}

/// Validate the given request. Returns validation error on failure.
fn validate_and_log(request: &dyn Validate) {
    if let Err(ref err) = request.validate() {
        validation::warn_validation_errors("Internal gRPC", err);
    }
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
        #[validate]
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
            "Validation error in body: [things[0].idx: value 0 invalid, must be 1.0 or larger]"
        )
    }
}
