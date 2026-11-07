use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use storage::quota::QuotaConfig;
use validator::Validate;

// The request replaces the whole configuration, so it is parsed strictly:
// `enabled` must be given and unknown fields are rejected. Otherwise a
// misspelled field or an empty body would deserialize to a disabled quota and
// silently remove every limit in the cluster. `QuotaConfig` itself stays
// lenient, because peers running different versions exchange it through
// consensus and read it back from disk.

/// Cluster-wide limits on node resources, replacing the current configuration.
///
/// Limits are enforced only while `enabled` is true, and an unset limit is not
/// enforced. A node that reaches either limit refuses all writes except point
/// deletes. Unknown fields are rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields)]
pub struct UpdateQuotas {
    /// Whether the limits below are enforced.
    pub enabled: bool,

    /// Limit on process resident memory, as a percentage of total memory (of the
    /// cgroup memory limit on Linux, if one is set). `100` trips only when
    /// memory is full.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1, max = 100))]
    pub max_resident_memory_percent: Option<u8>,

    /// Limit on used space of the filesystem holding the storage directory, as a
    /// percentage of its capacity. `100` trips only when the disk is full.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(range(min = 1, max = 100))]
    pub max_disk_usage_percent: Option<u8>,

    /// How many percentage points below a limit usage must fall before a node
    /// that reached it accepts writes again. `0` accepts writes as soon as usage
    /// is under the limit. Unset uses the built-in default, currently 5.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[validate(range(max = 100))]
    pub release_margin_percent: Option<u8>,
}

impl From<UpdateQuotas> for QuotaConfig {
    fn from(request: UpdateQuotas) -> Self {
        let UpdateQuotas {
            enabled,
            max_resident_memory_percent,
            max_disk_usage_percent,
            release_margin_percent,
        } = request;

        QuotaConfig {
            enabled,
            max_resident_memory_percent,
            max_disk_usage_percent,
            release_margin_percent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_fields() {
        let err =
            serde_json::from_str::<UpdateQuotas>(r#"{ "surelythisiswrong": true }"#).unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");

        // A misspelled limit next to a valid `enabled` must not enable an uncapped quota
        let err =
            serde_json::from_str::<UpdateQuotas>(r#"{ "enabled": true, "max_disk_usage": 90 }"#)
                .unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");
    }

    #[test]
    fn requires_enabled() {
        let err = serde_json::from_str::<UpdateQuotas>("{}").unwrap_err();
        assert!(err.to_string().contains("missing field `enabled`"), "{err}");
    }

    #[test]
    fn converts_every_field() {
        let request: UpdateQuotas = serde_json::from_str(
            r#"{
                "enabled": true,
                "max_resident_memory_percent": 80,
                "max_disk_usage_percent": 90,
                "release_margin_percent": 3
            }"#,
        )
        .unwrap();

        assert_eq!(
            QuotaConfig::from(request),
            QuotaConfig {
                enabled: true,
                max_resident_memory_percent: Some(80),
                max_disk_usage_percent: Some(90),
                release_margin_percent: Some(3),
            },
        );
    }
}
