use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Claims {
    /// Expiration time
    pub exp: Option<u64>,

    /// Read access
    pub r: Option<bool>,

    /// Read-write access
    pub rw: Option<bool>,
}
