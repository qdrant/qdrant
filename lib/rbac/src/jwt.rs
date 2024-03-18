use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Claims {
    /// Expiration time (seconds since UNIX epoch)
    pub exp: Option<u64>,

    /// Write access, default is false. Read access is always enabled
    pub w: Option<bool>,
}
