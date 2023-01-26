use schemars::JsonSchema;
use serde::de::Error;
use serde::{Deserialize, Serialize};

pub struct ValidationError;

impl std::fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Read consistency factor cannot be less than 1")
    }
}

/// * `majority` - send N/2+1 random request and return points, which present on all of them
///
/// * `quorum` - send requests to all nodes and return points which present on majority of nodes
///
/// * `all` - send requests to all nodes and return points which present on all nodes
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadConsistencyType {
    // send N/2+1 random request and return points, which present on all of them
    Majority,
    // send requests to all nodes and return points which present on majority of nodes
    Quorum,
    // send requests to all nodes and return points which present on all nodes
    All,
}

/// Read consistency parameter
///
/// Defines how many replicas should be queried to get the result
///
/// * `N` - send N random request and return points, which present on all of them
///
/// * `majority` - send N/2+1 random request and return points, which present on all of them
///
/// * `quorum` - send requests to all nodes and return points which present on majority of nodes
///
/// * `all` - send requests to all nodes and return points which present on all nodes
///
/// Default value is `Factor(1)`
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged, remote = "ReadConsistency")]
pub enum ReadConsistency {
    // send N random request and return points, which present on all of them
    Factor(usize),
    Type(ReadConsistencyType),
}

impl Default for ReadConsistency {
    fn default() -> Self {
        ReadConsistency::Factor(1)
    }
}

impl<'de> serde::Deserialize<'de> for ReadConsistency {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let consistency = ReadConsistency::deserialize(deserializer)?;
        if let ReadConsistency::Factor(factor) = &consistency {
            if *factor == 0 {
                return Err(D::Error::custom(ValidationError));
            }
        }
        Ok(consistency)
    }
}
impl serde::Serialize for ReadConsistency {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        ReadConsistency::serialize(self, serializer)
    }
}

#[cfg(test)]
mod tests {
    use schemars::schema_for;

    use super::*;

    #[test]
    fn test_read_consistency_deserialization() {
        let consistency = ReadConsistency::Type(ReadConsistencyType::Majority);
        let json = serde_json::to_string(&consistency).unwrap();
        println!("{json}");

        let json = "2";
        let consistency: ReadConsistency = serde_json::from_str(json).unwrap();
        assert_eq!(consistency, ReadConsistency::Factor(2));

        let json = "0";
        let consistency: Result<ReadConsistency, _> = serde_json::from_str(json);
        assert!(consistency.is_err());

        let json = "\"majority\"";
        let consistency: ReadConsistency = serde_json::from_str(json).unwrap();
        assert_eq!(
            consistency,
            ReadConsistency::Type(ReadConsistencyType::Majority)
        );

        let consistency = ReadConsistency::Type(ReadConsistencyType::All);
        let json = serde_json::to_string(&consistency).unwrap();
        assert_eq!(json, "\"all\"");

        let consistency = ReadConsistency::Factor(1);
        let json = serde_json::to_string(&consistency).unwrap();
        assert_eq!(json, "1");

        let json = "\"all\"";
        let consistency: ReadConsistency = serde_json::from_str(json).unwrap();
        assert_eq!(consistency, ReadConsistency::Type(ReadConsistencyType::All));

        let schema = schema_for!(ReadConsistency);
        let schema_str = serde_json::to_string_pretty(&schema).unwrap();
        println!("{schema_str}")
    }
}
