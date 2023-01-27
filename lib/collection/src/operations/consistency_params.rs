use api::grpc::qdrant::{
    read_consistency, ReadConsistency as ReadConsistencyGrpc,
    ReadConsistencyType as ReadConsistencyTypeGrpc,
};
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
#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReadConsistencyType {
    // send N/2+1 random request and return points, which present on all of them
    Majority,
    // send requests to all nodes and return points which present on majority of nodes
    Quorum,
    // send requests to all nodes and return points which present on all nodes
    All,
}

impl TryFrom<i32> for ReadConsistencyType {
    type Error = tonic::Status;

    fn try_from(consistency: i32) -> Result<Self, Self::Error> {
        let consistency = ReadConsistencyTypeGrpc::from_i32(consistency).ok_or_else(|| {
            tonic::Status::invalid_argument(format!(
                "invalid read consistency type value {consistency}",
            ))
        })?;

        Ok(consistency.into())
    }
}

impl From<ReadConsistencyTypeGrpc> for ReadConsistencyType {
    fn from(consistency: ReadConsistencyTypeGrpc) -> Self {
        match consistency {
            ReadConsistencyTypeGrpc::Majority => Self::Majority,
            ReadConsistencyTypeGrpc::Quorum => Self::Quorum,
            ReadConsistencyTypeGrpc::All => Self::All,
        }
    }
}

impl From<ReadConsistencyType> for i32 {
    fn from(consistency: ReadConsistencyType) -> Self {
        ReadConsistencyTypeGrpc::from(consistency) as _
    }
}

impl From<ReadConsistencyType> for ReadConsistencyTypeGrpc {
    fn from(consistency: ReadConsistencyType) -> Self {
        match consistency {
            ReadConsistencyType::Majority => ReadConsistencyTypeGrpc::Majority,
            ReadConsistencyType::Quorum => ReadConsistencyTypeGrpc::Quorum,
            ReadConsistencyType::All => ReadConsistencyTypeGrpc::All,
        }
    }
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
#[derive(Debug, Deserialize, Serialize, JsonSchema, Copy, Clone, PartialEq, Eq)]
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

impl ReadConsistency {
    pub fn try_from_optional(
        consistency: Option<ReadConsistencyGrpc>,
    ) -> Result<Option<Self>, tonic::Status> {
        consistency.map(TryFrom::try_from).transpose()
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

impl TryFrom<Option<ReadConsistencyGrpc>> for ReadConsistency {
    type Error = tonic::Status;

    fn try_from(consistency: Option<ReadConsistencyGrpc>) -> Result<Self, Self::Error> {
        match consistency {
            Some(consistency) => consistency.try_into(),
            None => Ok(Self::Factor(1)),
        }
    }
}

impl TryFrom<ReadConsistencyGrpc> for ReadConsistency {
    type Error = tonic::Status;

    fn try_from(consistency: ReadConsistencyGrpc) -> Result<Self, Self::Error> {
        let value = consistency.value.ok_or_else(|| tonic::Status::invalid_argument(
                "invalid read consistency message: `ReadConsistency::value` field is `None`"
        ))?;

        let consistency = match value {
            read_consistency::Value::Factor(factor) => Self::Factor(factor.try_into().unwrap()),
            read_consistency::Value::Type(consistency) => {
                Self::Type(consistency.try_into().unwrap())
            }
        };

        Ok(consistency)
    }
}

impl From<ReadConsistency> for ReadConsistencyGrpc {
    fn from(consistency: ReadConsistency) -> Self {
        let value = match consistency {
            ReadConsistency::Factor(factor) => {
                read_consistency::Value::Factor(factor.try_into().unwrap())
            }
            ReadConsistency::Type(consistency) => read_consistency::Value::Type(consistency.into()),
        };

        ReadConsistencyGrpc { value: Some(value) }
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
