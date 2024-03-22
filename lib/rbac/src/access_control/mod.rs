mod access_token;
mod collection_access_token;
pub mod error;

pub enum AccessLevel {
    ReadOnly,
    ReadWrite,
    Manage,
}

impl AccessLevel {
    pub fn is_read_allowed(&self) -> bool {
        match self {
            AccessLevel::ReadOnly | AccessLevel::ReadWrite | AccessLevel::Manage => true,
        }
    }

    pub fn is_write_allowed(&self) -> bool {
        match self {
            AccessLevel::ReadWrite | AccessLevel::Manage => true,
            AccessLevel::ReadOnly => false,
        }
    }

    pub fn is_manage_allowed(&self) -> bool {
        match self {
            AccessLevel::Manage => true,
            AccessLevel::ReadOnly | AccessLevel::ReadWrite => false,
        }
    }
}
