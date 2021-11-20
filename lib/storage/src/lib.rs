//! Storage - is a crate which contains all service functions, abstracted from the external interface
//!
//! It provides all functions, which could be used from REST (or any other interface), but do not
//! implement any concrete interface.

pub mod content_manager;
pub mod types;
