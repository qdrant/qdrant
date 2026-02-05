//! Optimization execution module.
//!
//! This module contains the core optimization execution logic that is agnostic
//! to collection-level policies. The collection layer provides the segment builder
//! configuration via the `SegmentBuilderFactory` trait, and this module handles
//! the actual optimization execution.

mod execute;

pub use execute::{
    OptimizationPaths, OptimizationPolicy, OptimizationResult, execute_optimization,
};
