use serde::Deserialize;

#[derive(Deserialize)]
pub struct Claims {
    /// Expiration time
    pub exp: Option<u64>,

    /// Read access
    pub r: Option<bool>,

    /// Read-write access
    pub rw: Option<bool>,
}
