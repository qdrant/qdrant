//! Crate, which implements all functions required for operations with a single collection

pub mod collection;
pub mod collection_builder;
pub mod collection_manager;
mod common;
pub mod config;
pub mod operations;
mod update_handler;
mod wal;

#[cfg(test)]
mod tests;
