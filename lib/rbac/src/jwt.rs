use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Claims {
    /// Expiration time (seconds since UNIX epoch)
    pub exp: Option<u64>,

    /// Write access, default is false. Read access is always enabled
    pub w: Option<bool>,
}

impl Claims {
    /// Write access, default is false. Read access is always enabled
    pub fn write_access(&self) -> bool {
        self.w.unwrap_or(false)
    }
}
