use std::num::NonZeroU64;
use std::time::Duration;

use collection::operations::consistency_params::ReadConsistency;
use schemars::JsonSchema;
use serde::Deserialize;
use validator::Validate;

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Deserialize, JsonSchema, Validate)]
pub struct ReadParams {
    #[serde(default, deserialize_with = "deserialize_read_consistency")]
    #[validate]
    pub consistency: Option<ReadConsistency>,
    /// If set, overrides global timeout for this request. Unit is seconds.
    pub timeout: Option<NonZeroU64>,
}

impl ReadParams {
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout.map(|num| Duration::from_secs(num.get()))
    }
}

fn deserialize_read_consistency<'de, D>(
    deserializer: D,
) -> Result<Option<ReadConsistency>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Helper<'a> {
        ReadConsistency(ReadConsistency),
        Str(&'a str),
    }

    match Helper::deserialize(deserializer)? {
        Helper::ReadConsistency(read_consistency) => Ok(Some(read_consistency)),
        Helper::Str("") => Ok(None),
        _ => Err(serde::de::Error::custom(
            "failed to deserialize read consistency query parameter value",
        )),
    }
}

#[cfg(test)]
mod test {
    use collection::operations::consistency_params::ReadConsistencyType;

    use super::*;

    #[test]
    fn deserialize_empty_string() {
        test_str("", ReadParams::default());
    }

    #[test]
    fn deserialize_empty_value() {
        test("", ReadParams::default());
    }

    #[test]
    fn deserialize_type() {
        test("all", from_type(ReadConsistencyType::All));
        test("majority", from_type(ReadConsistencyType::Majority));
        test("quorum", from_type(ReadConsistencyType::Quorum));
    }

    #[test]
    fn deserialize_factor() {
        for factor in 1..42 {
            test(&factor.to_string(), from_factor(factor));
        }
    }

    #[test]
    fn try_deserialize_factor_0() {
        assert!(try_deserialize(&str("0")).is_err());
    }

    fn test(value: &str, params: ReadParams) {
        test_str(&str(value), params);
    }

    fn test_str(str: &str, params: ReadParams) {
        assert_eq!(deserialize(str), params);
    }

    fn deserialize(str: &str) -> ReadParams {
        try_deserialize(str).unwrap()
    }

    fn try_deserialize(str: &str) -> Result<ReadParams, serde_urlencoded::de::Error> {
        serde_urlencoded::from_str(str)
    }

    fn str(value: &str) -> String {
        format!("consistency={value}")
    }

    fn from_type(r#type: ReadConsistencyType) -> ReadParams {
        ReadParams {
            consistency: Some(ReadConsistency::Type(r#type)),
            ..Default::default()
        }
    }

    fn from_factor(factor: usize) -> ReadParams {
        ReadParams {
            consistency: Some(ReadConsistency::Factor(factor)),
            ..Default::default()
        }
    }
}
