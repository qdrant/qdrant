//! Fluent builder for [`Bm25Params`].
//!
//! Builder fields mirror [`Bm25Params`] explicitly so adding a field
//! to the target struct forces a compile error here.

use crate::types::Bm25Params;

/// Fluent builder for [`Bm25Params`].
///
/// All fields are optional; fields left unset take the BM25 defaults.
#[derive(Debug, Clone, Default)]
pub struct Bm25ParamsBuilder {
    k: Option<f32>,
    b: Option<f32>,
}

impl Bm25ParamsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Term frequency saturation, non-negative. Default: 1.2.
    pub fn k(mut self, k: f32) -> Self {
        self.k = Some(k);
        self
    }

    /// Document length normalization, within `[0, 1]`. Default: 0.75.
    pub fn b(mut self, b: f32) -> Self {
        self.b = Some(b);
        self
    }

    pub fn build(self) -> Bm25Params {
        let Self { k, b } = self;
        Bm25Params { k, b }
    }
}
