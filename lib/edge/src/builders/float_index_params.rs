//! Fluent builder for [`FloatIndexParams`].
//!
//! Builder fields mirror [`FloatIndexParams`] explicitly so adding a field
//! to the target struct forces a compile error here.

use segment::data_types::index::{FloatIndexParams, FloatIndexType};
use segment::json_path::JsonPath;
use segment::types::Memory;

/// Fluent builder for [`FloatIndexParams`].
///
/// All fields are optional; fields left unset take the index defaults.
#[derive(Debug, Clone, Default)]
pub struct FloatIndexParamsBuilder {
    is_principal: Option<bool>,
    on_disk: Option<bool>,
    memory: Option<Memory>,
    enable_hnsw: Option<bool>,
    nested: Option<JsonPath>,
}

impl FloatIndexParamsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// If `true`, organize the storage of the collection data by this field,
    /// assuming most filtered requests use it. Default: `false`.
    pub fn is_principal(mut self, is_principal: bool) -> Self {
        self.is_principal = Some(is_principal);
        self
    }

    /// Deprecated: use [`Self::memory`] instead.
    /// If `true`, store the index on disk.
    #[deprecated(since = "1.19.0", note = "Use `memory` instead")]
    pub fn on_disk(mut self, on_disk: bool) -> Self {
        self.on_disk = Some(on_disk);
        self
    }

    /// Memory placement of the index. Overrides the deprecated `on_disk` flag
    /// if both are set.
    pub fn memory(mut self, memory: Memory) -> Self {
        self.memory = Some(memory);
        self
    }

    /// Build additional HNSW links for this payload field. Default: `true`.
    pub fn enable_hnsw(mut self, enable_hnsw: bool) -> Self {
        self.enable_hnsw = Some(enable_hnsw);
        self
    }

    /// Index every object of this array as a separate entry, e.g. `offers` for the
    /// field `offers[].price`. The field must be inside the array's elements, and
    /// `is_empty` on it matches a point when one of its elements lacks the field.
    /// Default: disabled.
    pub fn nested(mut self, array: JsonPath) -> Self {
        self.nested = Some(array);
        self
    }

    #[allow(deprecated)]
    pub fn build(self) -> FloatIndexParams {
        // Exhaustively destructure Self and construct FloatIndexParams:
        // adding a field to either type forces a compile error here.
        let Self {
            is_principal,
            on_disk,
            memory,
            enable_hnsw,
            nested,
        } = self;
        FloatIndexParams {
            r#type: FloatIndexType::Float,
            is_principal,
            on_disk,
            memory,
            enable_hnsw,
            nested,
        }
    }
}
