use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub struct ReadConsistencyValidationError;

impl std::fmt::Display for ReadConsistencyValidationError {
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
#[serde(untagged)]
#[serde(try_from = "ReadConsistencyShadow")]
pub enum ReadConsistency {
    // send N random request and return points, which present on all of them
    Factor(usize),
    Type(ReadConsistencyType),
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum ReadConsistencyShadow {
    Factor(usize),
    Type(ReadConsistencyType),
}

impl Default for ReadConsistency {
    fn default() -> Self {
        ReadConsistency::Factor(1)
    }
}

impl TryFrom<ReadConsistencyShadow> for ReadConsistency {
    type Error = ReadConsistencyValidationError;

    fn try_from(value: ReadConsistencyShadow) -> Result<Self, Self::Error> {
        match value {
            ReadConsistencyShadow::Factor(factor) => {
                if factor > 0 {
                    Ok(ReadConsistency::Factor(factor))
                } else {
                    Err(ReadConsistencyValidationError)
                }
            }
            ReadConsistencyShadow::Type(read_consistency_type) => {
                Ok(ReadConsistency::Type(read_consistency_type))
            }
        }
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
        println!("{}", json);

        let json = "2";
        let consistency: ReadConsistency = serde_json::from_str(json).unwrap();
        assert_eq!(consistency, ReadConsistency::Factor(2));

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
        println!("{}", schema_str)
    }
}
