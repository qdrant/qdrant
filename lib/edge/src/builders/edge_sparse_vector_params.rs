//! Fluent builder for [`EdgeSparseVectorParams`].
//!
//! Builder fields mirror [`EdgeSparseVectorParams`] explicitly so adding a
//! field to the target struct forces a compile error here.

use segment::data_types::modifier::Modifier;
use segment::types::VectorStorageDatatype;

use crate::config::vectors::EdgeSparseVectorParams;

/// Fluent builder for [`EdgeSparseVectorParams`].
///
/// All fields are optional; calling [`Self::build`] without setters yields
/// an [`EdgeSparseVectorParams`] with every field `None`.
#[derive(Debug, Clone, Default)]
pub struct EdgeSparseVectorParamsBuilder {
    full_scan_threshold: Option<usize>,
    on_disk: Option<bool>,
    modifier: Option<Modifier>,
    datatype: Option<VectorStorageDatatype>,
    data_integrity_check: Option<bool>,
    magnitude_bound: Option<f32>,
}

impl EdgeSparseVectorParamsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn full_scan_threshold(mut self, full_scan_threshold: usize) -> Self {
        self.full_scan_threshold = Some(full_scan_threshold);
        self
    }

    /// If `true`, sparse index is on disk (mmap); otherwise in RAM.
    pub fn on_disk(mut self, on_disk: bool) -> Self {
        self.on_disk = Some(on_disk);
        self
    }

    pub fn modifier(mut self, modifier: Modifier) -> Self {
        self.modifier = Some(modifier);
        self
    }

    pub fn datatype(mut self, datatype: VectorStorageDatatype) -> Self {
        self.datatype = Some(datatype);
        self
    }

    pub fn data_integrity_check(mut self, data_integrity_check: bool) -> Self {
        self.data_integrity_check = Some(data_integrity_check);
        self
    }

    pub fn magnitude_bound(mut self, magnitude_bound: f32) -> Self {
        self.magnitude_bound = Some(magnitude_bound);
        self
    }

    pub fn build(self) -> EdgeSparseVectorParams {
        // Exhaustively destructure Self and construct EdgeSparseVectorParams:
        // adding a field to either type forces a compile error here.
        let Self {
            full_scan_threshold,
            on_disk,
            modifier,
            datatype,
            data_integrity_check,
            magnitude_bound,
        } = self;
        EdgeSparseVectorParams {
            full_scan_threshold,
            on_disk,
            modifier,
            datatype,
            data_integrity_check,
            magnitude_bound,
        }
    }
}
