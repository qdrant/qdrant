use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub struct ReadConsistencyValidationError;

impl std::fmt::Display for ReadConsistencyValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Read consistency factor cannot be less than 1")
    }
}

/// Read consistency parameter
///
/// Defines how many replicas should be queried to get the result
/// * `Factor(N)` - send N random request and return points, which present on all of them
/// * `Majority` - send N/2+1 random request and return points, which present on all of them
/// * `Quorum` - send requests to all nodes and return points which present on majority of nodes
/// * `All` - send requests to all nodes and return points which present on all nodes
///
/// Default value is `Factor(1)`
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(try_from = "ReadConsistencyShadow")]
pub enum ReadConsistency {
    // send N random request and return points, which present on all of them
    Factor(usize),
    // send N/2+1 random request and return points, which present on all of them
    Majority,
    // send requests to all nodes and return points which present on majority of nodes
    Quorum,
    // send requests to all nodes and return points which present on all nodes
    All,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum ReadConsistencyShadow {
    Factor(usize),
    Majority,
    Quorum,
    All,
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
            ReadConsistencyShadow::Majority => Ok(ReadConsistency::Majority),
            ReadConsistencyShadow::Quorum => Ok(ReadConsistency::Quorum),
            ReadConsistencyShadow::All => Ok(ReadConsistency::All),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_consistency_deserialization() {
        let json = "2";
        let consistency: ReadConsistency = serde_json::from_str(json).unwrap();
        assert_eq!(consistency, ReadConsistency::Factor(2));

        let json = "\"All\"";
        let consistency: ReadConsistency = serde_json::from_str(json).unwrap();
        assert_eq!(consistency, ReadConsistency::All);
    }
}
