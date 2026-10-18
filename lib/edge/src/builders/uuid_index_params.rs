//! Fluent builder for [`UuidIndexParams`].
//!
//! Builder fields mirror [`UuidIndexParams`] explicitly so adding a field
//! to the target struct forces a compile error here.

use segment::data_types::index::{UuidIndexParams, UuidIndexType};
use segment::json_path::JsonPath;
use segment::types::Memory;

/// Fluent builder for [`UuidIndexParams`].
///
/// All fields are optional; fields left unset take the index defaults.
#[derive(Debug, Clone, Default)]
pub struct UuidIndexParamsBuilder {
    is_tenant: Option<bool>,
    on_disk: Option<bool>,
    memory: Option<Memory>,
    enable_hnsw: Option<bool>,
    nested: Option<JsonPath>,
}

impl UuidIndexParamsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// If `true`, the field is used for tenant optimization: points sharing a
    /// value are stored together. Default: `false`.
    pub fn is_tenant(mut self, is_tenant: bool) -> Self {
        self.is_tenant = Some(is_tenant);
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
    pub fn build(self) -> UuidIndexParams {
        // Exhaustively destructure Self and construct UuidIndexParams:
        // adding a field to either type forces a compile error here.
        let Self {
            is_tenant,
            on_disk,
            memory,
            enable_hnsw,
            nested,
        } = self;
        UuidIndexParams {
            r#type: UuidIndexType::Uuid,
            is_tenant,
            on_disk,
            memory,
            enable_hnsw,
            nested,
        }
    }
}
