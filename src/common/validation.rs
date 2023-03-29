use validator::{ValidationError, ValidationErrors, ValidationErrorsKind};

/// Pretty print validation errors to the log as warning.
pub fn log_validation_errors(description: &str, errs: &ValidationErrors) {
    log::warn!("{description} has validation errors:");
    describe_errors(None, errs)
        .into_iter()
        .for_each(|(key, msg)| log::warn!("- {key}: {}", msg));
}

/// Describe the given validation errors.
///
/// Returns a list of error messages for fields: `(field, message)`
pub fn describe_errors(key: Option<&str>, errs: &ValidationErrors) -> Vec<(String, String)> {
    errs.errors()
        .iter()
        .flat_map(|(field, err)| {
            let key = match key {
                Some(key) => format!("{key}.{field}"),
                None => field.to_string(),
            };

            match err {
                ValidationErrorsKind::Struct(errs) => describe_errors(Some(&key), errs),
                ValidationErrorsKind::List(errs) => errs
                    .iter()
                    .flat_map(|(i, errs)| describe_errors(Some(&format!("{key}.{i}")), errs))
                    .collect(),
                ValidationErrorsKind::Field(errs) => errs
                    .iter()
                    .map(|err| (key.to_string(), describe_error(err)))
                    .collect(),
            }
        })
        .collect()
}

/// Describe a specific validation error.
fn describe_error(err: &ValidationError) -> String {
    let ValidationError {
        code,
        message,
        params,
    } = err;

    // Prefer to return message if set
    if let Some(message) = message {
        return message.to_string();
    }

    // Generate messages based on codes
    match code.as_ref() {
        "range" => {
            let msg = match (params.get("min"), params.get("max")) {
                (Some(min), None) => format!("must be {min} or larger"),
                (Some(min), Some(max)) => format!("must be between {min} and {max}"),
                (None, Some(max)) => format!("must be {max} or smaller"),
                // Should be unreachable
                _ => err.to_string(),
            };
            match params.get("value") {
                Some(value) => format!("value {value} {msg}"),
                None => msg,
            }
        }
        "length" => {
            let msg = match (params.get("equal"), params.get("min"), params.get("max")) {
                (Some(equal), _, _) => format!("must be exactly {equal} characters"),
                (None, Some(min), None) => format!("must be at least {min} characters"),
                (None, Some(min), Some(max)) => {
                    format!("must be between {min} and {max} characters")
                }
                (None, None, Some(max)) => format!("must be at most {max} characters"),
                // Should be unreachable
                _ => err.to_string(),
            };
            match params.get("value") {
                Some(value) => format!("value {value} {msg}"),
                None => msg,
            }
        }
        // Undescribed error codes
        _ => err.to_string(),
    }
}
