use segment::index::field_index::full_text_index::Bm25Params as SegmentBm25Params;

/// Parameters of the scorer of a text query, keyed by the scorer. They must
/// match the `scoring` of the field's text index.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TextQueryScoring {
    /// BM25 parameters, for a field scored with BM25.
    Bm25(Bm25Params),
}

/// BM25 parameters of a text query. Each `None` takes its default.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Bm25Params {
    /// Term frequency saturation, non-negative. Default: 1.2.
    pub k: Option<f32>,
    /// Document length normalization, within `[0, 1]`. Default: 0.75.
    pub b: Option<f32>,
}

impl From<Bm25Params> for SegmentBm25Params {
    fn from(params: Bm25Params) -> Self {
        let Bm25Params { k, b } = params;
        let default = SegmentBm25Params::default();
        SegmentBm25Params {
            k1: k.unwrap_or(default.k1),
            b: b.unwrap_or(default.b),
        }
    }
}
