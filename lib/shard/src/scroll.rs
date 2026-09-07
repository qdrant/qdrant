use schemars::JsonSchema;
use segment::data_types::load_profile::LoadProfile;
use segment::data_types::order_by::OrderByInterface;
use segment::types::{Filter, PointIdType, WithPayloadInterface, WithVector};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Scroll request - paginate over all points which matches given condition
#[derive(Clone, Debug, PartialEq, Hash, Deserialize, Serialize, JsonSchema, Validate)]
#[serde(rename_all = "snake_case")]
pub struct ScrollRequestInternal {
    /// Start ID to read points from.
    pub offset: Option<PointIdType>,

    /// Page size. Default: 10
    #[validate(range(min = 1))]
    pub limit: Option<usize>,

    /// Look only for points which satisfies this conditions. If not provided - all points.
    #[validate(nested)]
    pub filter: Option<Filter>,

    /// Select which payload to return with the response. Default is true.
    pub with_payload: Option<WithPayloadInterface>,

    /// Internal optimization hint; never accepted from or exposed through REST.
    /// Explicitly selected top-level indexed fields may be returned as arrays of
    /// indexed values. Other projections retain exact payload retrieval.
    #[serde(skip)]
    #[schemars(skip)]
    pub prefer_payload_index: bool,

    /// Options for specifying which vectors to include into response. Default is false.
    #[serde(default, alias = "with_vectors")]
    pub with_vector: WithVector,

    /// Order the records by a payload field.
    pub order_by: Option<OrderByInterface>,
}

impl Default for ScrollRequestInternal {
    fn default() -> Self {
        ScrollRequestInternal {
            offset: None,
            limit: Some(Self::default_limit()),
            filter: None,
            with_payload: Some(Self::default_with_payload()),
            prefer_payload_index: false,
            with_vector: Self::default_with_vector(),
            order_by: None,
        }
    }
}

impl ScrollRequestInternal {
    /// Request-specific [`LoadProfile`] for opening a read-only shard to serve exactly
    /// this scroll: no vector components are warmed, and only the field indexes the
    /// filter and `order_by` read keep their configured placement.
    pub fn load_profile(&self) -> LoadProfile {
        let Self {
            offset: _,
            limit: _,
            filter,
            with_payload,
            prefer_payload_index,
            with_vector: _,
            order_by,
        } = self;

        let order_by_key = order_by.as_ref().map(|order_by| match order_by {
            OrderByInterface::Key(key) => key,
            OrderByInterface::Struct(order_by) => &order_by.key,
        });
        // The `with_payload` default of a scroll is `true`.
        let with_payload = with_payload.as_ref().is_none_or(|wp| wp.is_required());

        let mut profile = LoadProfile::for_scroll(filter.as_ref(), order_by_key, with_payload);
        if *prefer_payload_index {
            let fields = match &self.with_payload {
                Some(WithPayloadInterface::Fields(fields)) => Some(fields),
                Some(WithPayloadInterface::Selector(segment::types::PayloadSelector::Include(
                    selector,
                ))) => Some(&selector.include),
                _ => None,
            };
            if let Some(fields) = fields {
                profile.include_payload_fields(fields.iter().cloned());
            }
        }
        profile
    }

    pub const fn default_limit() -> usize {
        10
    }

    pub const fn default_with_payload() -> WithPayloadInterface {
        WithPayloadInterface::Bool(true)
    }

    pub const fn default_with_vector() -> WithVector {
        WithVector::Bool(false)
    }
}

#[cfg(test)]
mod tests {
    use common::universal_io::Populate;
    use serde_json::json;

    use super::*;

    #[test]
    fn payload_index_hint_is_internal_only() {
        let request: ScrollRequestInternal = serde_json::from_value(json!({
            "with_payload": ["city"], "prefer_payload_index": true
        }))
        .unwrap();
        assert!(!request.prefer_payload_index);
        let mut internal = request;
        internal.prefer_payload_index = true;
        assert!(
            serde_json::to_value(&internal)
                .unwrap()
                .get("prefer_payload_index")
                .is_none()
        );
        let schema = serde_json::to_value(schemars::schema_for!(ScrollRequestInternal)).unwrap();
        assert!(schema["properties"].get("prefer_payload_index").is_none());
        let schema =
            serde_json::to_value(schemars::schema_for!(segment::types::WithPayload)).unwrap();
        assert!(schema["properties"].get("prefer_payload_index").is_none());
    }

    #[test]
    fn payload_index_hint_keeps_projected_index_placement() {
        let field: segment::json_path::JsonPath = "city".parse().unwrap();
        let mut request = ScrollRequestInternal {
            with_payload: Some(WithPayloadInterface::Fields(vec![field.clone()])),
            ..Default::default()
        };
        assert_eq!(
            request.load_profile().payload_index_placement(&field),
            Some(Populate::No)
        );
        request.prefer_payload_index = true;
        assert_eq!(request.load_profile().payload_index_placement(&field), None);
        assert_eq!(request.load_profile().payload_storage_placement(), None);
    }

    #[test]
    fn payload_index_hint_internal_wire_compatibility() {
        use api::grpc::qdrant::{ScrollPoints, ScrollPointsInternal};
        use prost::Message;

        #[derive(Clone, PartialEq, Message)]
        struct LegacyScrollPointsInternal {
            #[prost(message, optional, tag = "1")]
            scroll_points: Option<ScrollPoints>,
            #[prost(uint32, optional, tag = "2")]
            shard_id: Option<u32>,
        }

        let request = ScrollPointsInternal {
            scroll_points: Some(ScrollPoints {
                collection_name: "test".into(),
                with_payload: Some(
                    WithPayloadInterface::Fields(vec!["city".parse().unwrap()]).into(),
                ),
                ..Default::default()
            }),
            shard_id: Some(1),
            prefer_payload_index: true,
        };
        let bytes = request.encode_to_vec();
        assert_eq!(
            ScrollPointsInternal::decode(bytes.as_slice()).unwrap(),
            request
        );
        let legacy = LegacyScrollPointsInternal::decode(bytes.as_slice()).unwrap();
        assert_eq!(legacy.scroll_points, request.scroll_points);
        let bytes = legacy.encode_to_vec();
        let upgraded = ScrollPointsInternal::decode(bytes.as_slice()).unwrap();
        assert!(!upgraded.prefer_payload_index);
        assert_eq!(upgraded.scroll_points, request.scroll_points);
    }
}
