mod search;

use super::config_diff::StrictModeConfigDiff;

// Creates a new `VerificationPass` for successful verifications.
// Don't use this, unless you know what you're doing!
pub fn new_pass() -> VerificationPass {
    VerificationPass { inner: () }
}

/// A pass, created on successful verification.
pub struct VerificationPass {
    // Private field, so we can't instantiate it from somewhere else.
    #[allow(dead_code)]
    inner: (),
}

pub trait StrictModeVerification {
    fn check_strict_mode(&self, strict_mode_config: &StrictModeConfigDiff) -> Result<(), String>;
}

pub(crate) fn new_error<S>(description: S, solution: &str) -> String
where
    S: ToString,
{
    format!("{}. Help: {solution}", description.to_string())
}
