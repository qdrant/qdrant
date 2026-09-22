//! Experimental page-attention API, independent of ordinary top-k search.
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::types::PointIdType;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum AttentionQuery {
    Vector(Vec<f32>),
    /// Use the stored K as Q, not the prefill query at this position.
    Id(PointIdType),
}

fn default_ef() -> usize {
    16
}

#[derive(Clone, Debug, Deserialize, Serialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct AttentionRequest {
    pub query: AttentionQuery,
    #[validate(length(min = 1, max = 255))]
    pub using: String,
    #[serde(default = "default_ef")]
    #[validate(range(min = 1, max = 8192))]
    pub ef: usize,
    #[serde(default)]
    pub rescore: bool,
    /// Best tokens among scanned pages; diagnostic only, does not change attention.
    #[serde(default)]
    #[validate(range(max = 8192))]
    pub return_top_k: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct AttentionBatchRequest {
    #[validate(length(min = 1, max = 64), nested)]
    pub queries: Vec<AttentionRequest>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AttentionResponse {
    pub attention: Vec<f32>,
    pub lse: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_ids: Option<Vec<PointIdType>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_input_variants() {
        for query in [serde_json::json!([1.0, 2.0]), serde_json::json!(42)] {
            let r: AttentionRequest =
                serde_json::from_value(serde_json::json!({"using":"head", "query":query})).unwrap();
            r.validate().unwrap();
            assert_eq!(r.ef, 16);
            assert_eq!(r.return_top_k, 0);
            assert!(!r.rescore);
        }
        let r = AttentionResponse {
            attention: vec![1.0],
            lse: 0.0,
            token_ids: None,
        };
        assert!(serde_json::to_value(r).unwrap().get("token_ids").is_none());
    }

    #[test]
    fn rejects_unsupported_options_and_invalid_bounds() {
        assert!(
            serde_json::from_value::<AttentionRequest>(
                serde_json::json!({"using":"h","query":1,"exact":true})
            )
            .is_err()
        );
        for (ef, k) in [(0, 0), (8193, 0), (16, 8193)] {
            let r: AttentionRequest = serde_json::from_value(
                serde_json::json!({"using":"h","query":1,"ef":ef,"return_top_k":k}),
            )
            .unwrap();
            assert!(r.validate().is_err());
        }
        let r: AttentionBatchRequest =
            serde_json::from_value(serde_json::json!({"queries":[]})).unwrap();
        assert!(r.validate().is_err());
    }
}
