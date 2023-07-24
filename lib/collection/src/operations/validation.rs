use actix_web_validator::error::flatten_errors;
use validator::{ValidationError, ValidationErrors};

/// Warn about validation errors in the log.
///
/// Validation errors are pretty printed field-by-field.
pub fn warn_validation_errors(description: &str, errs: &ValidationErrors) {
    log::warn!("{description} has validation errors:");
    describe_errors(errs)
        .into_iter()
        .for_each(|(key, msg)| log::warn!("- {key}: {}", msg));
}

/// Label the given validation errors in a single string.
pub fn label_errors(label: impl AsRef<str>, errs: &ValidationErrors) -> String {
    format!(
        "{}: [{}]",
        label.as_ref(),
        describe_errors(errs)
            .into_iter()
            .map(|(field, err)| format!("{field}: {err}"))
            .collect::<Vec<_>>()
            .join("; ")
    )
}

/// Describe the given validation errors.
///
/// Returns a list of error messages for fields: `(field, message)`
fn describe_errors(errs: &ValidationErrors) -> Vec<(String, String)> {
    flatten_errors(errs)
        .into_iter()
        .map(|(_, name, err)| (name, describe_error(err)))
        .collect()
}

/// Describe a specific validation error.
fn describe_error(
    err @ ValidationError {
        code,
        message,
        params,
    }: &ValidationError,
) -> String {
    // Prefer to return message if set
    if let Some(message) = message {
        return message.to_string();
    }

    // Generate messages based on codes
    match code.as_ref() {
        "range" => {
            let msg = match (params.get("min"), params.get("max")) {
                (Some(min), None) => format!("must be {min} or larger"),
                (Some(min), Some(max)) => format!("must be from {min} to {max}"),
                (None, Some(max)) => format!("must be {max} or smaller"),
                // Should be unreachable
                _ => err.to_string(),
            };
            match params.get("value") {
                Some(value) => format!("value {value} invalid, {msg}"),
                None => msg,
            }
        }
        "length" => {
            let msg = match (params.get("equal"), params.get("min"), params.get("max")) {
                (Some(equal), _, _) => format!("must be exactly {equal} characters"),
                (None, Some(min), None) => format!("must be at least {min} characters"),
                (None, Some(min), Some(max)) => {
                    format!("must be from {min} to {max} characters")
                }
                (None, None, Some(max)) => format!("must be at most {max} characters"),
                // Should be unreachable
                _ => err.to_string(),
            };
            match params.get("value") {
                Some(value) => format!("value {value} invalid, {msg}"),
                None => msg,
            }
        }
        "does_not_contain" => match params.get("pattern") {
            Some(pattern) => format!("cannot contain {pattern}"),
            None => err.to_string(),
        },
        "not_empty" => {
            "value invalid, must not be empty".to_string()
        },
        "closed_ring" => match params.get("index") {
            Some(index) => format!("rings[{}] invalid, the first and the last points should be same to form a closed ring", index),
            None => err.to_string(),
        }
        "min_polygon_length" => match (params.get("min_length"), params.get("length"), params.get("index")) {
            (Some(min_length), Some(length), Some(index)) => format!("rings[{}] invalid, the size must be at least {}, got {}", index, min_length, length),
            _ => err.to_string(),
        },
        // Undescribed error codes
        _ => err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use api::grpc::qdrant::{GeoLineString, GeoPoint, GeoPolygon};
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

    fn build_polygon(lines: Vec<Vec<(f64, f64)>>) -> GeoPolygon {
        let mut polygon = GeoPolygon { rings: vec![] };

        for line in lines {
            let mut points = vec![];
            for (lon, lat) in line {
                points.push(GeoPoint { lon, lat });
            }
            polygon.rings.push(GeoLineString { points });
        }

        polygon
    }

    #[test]
    fn test_validation() {
        let bad_config = OtherThing {
            things: vec![SomeThing { idx: 0 }],
        };
        assert!(
            bad_config.validate().is_err(),
            "validation of bad config should fail"
        );
    }

    #[test]
    fn test_config_validation_render() {
        let bad_config = OtherThing {
            things: vec![
                SomeThing { idx: 0 },
                SomeThing { idx: 1 },
                SomeThing { idx: 2 },
            ],
        };

        let errors = bad_config
            .validate()
            .expect_err("validation of bad config should fail");

        assert_eq!(
            describe_errors(&errors),
            vec![(
                "things[0].idx".into(),
                "value 0 invalid, must be 1.0 or larger".into()
            )]
        );
    }

    #[test]
    fn test_polygon_validation_render() {
        let test_cases = vec![
            (
                build_polygon(vec![]),
                vec![("rings".into(), "value invalid, must not be empty".into())],
            ),
            (
                build_polygon(vec![vec![(1., 1.),(2., 2.),(1., 1.)]]),
                vec![("rings".into(), "rings[0] invalid, the size must be at least 4, got 3".into())],
            ),
            (
                build_polygon(vec![vec![(1., 1.),(2., 2.),(3., 3.),(4., 4.)]]),
                vec![(
                    "rings".into(),
                    "rings[0] invalid, the first and the last points should be same to form a closed ring".into(),
                )],
            ),
            (
                build_polygon(vec![
                    vec![(1., 1.),(2., 2.),(3., 3.),(1., 1.)],
                    vec![(1., 1.),(2., 2.),(1., 1.)],
                ]),
                vec![("rings".into(), "rings[1] invalid, the size must be at least 4, got 3".into())],
            ),
            (
                build_polygon(vec![
                    vec![(1., 1.),(2., 2.),(3., 3.),(1., 1.)],
                    vec![(1., 1.),(2., 2.),(3., 3.),(4., 4.)],
                ]),
                vec![(
                    "rings".into(),
                    "rings[1] invalid, the first and the last points should be same to form a closed ring".into(),
                )],
            ),
        ];

        for (polygon, expected_errors) in test_cases {
            let errors = polygon
                .validate()
                .expect_err("validation of bad polygon should fail");

            assert_eq!(describe_errors(&errors), expected_errors);
        }
    }
}
