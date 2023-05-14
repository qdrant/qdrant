use std::collections::HashMap;
use std::hash::Hash;

use segment::types::{GroupId, PointGroup, PointIdType, ScoredPoint};

use crate::grouping::types::AggregatorError::BadKeyType;

#[derive(PartialEq, Debug)]
pub(super) enum AggregatorError {
    BadKeyType,
    KeyNotFound,
}
#[derive(Debug, Clone)]
pub(super) struct Group {
    pub hits: Vec<ScoredPoint>,
    pub key: GroupKey,
}

impl Group {
    pub(super) fn hydrate_from(&mut self, map: &HashMap<PointIdType, ScoredPoint>) {
        self.hits.iter_mut().for_each(|hit| {
            if let Some(point) = map.get(&hit.id) {
                hit.payload = point.payload.clone();
                hit.vector = point.vector.clone();
            }
        });
    }
}

impl From<Group> for PointGroup {
    fn from(group: Group) -> Self {
        Self {
            hits: group.hits,
            id: group.key.0,
        }
    }
}

/// Abstraction over serde_json::Value to be used as a key in a HashMap/HashSet
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub(super) struct GroupKey(GroupId);

impl TryFrom<&serde_json::Value> for GroupKey {
    type Error = AggregatorError;

    /// Only allows Strings and Numbers to be converted into GroupKey
    /// When dealing with arrays, it will consider only the first element
    fn try_from(value: &serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::String(s) => Ok(Self(GroupId::String(s.to_string()))),
            serde_json::Value::Number(n) => Ok(Self(GroupId::Number(n.to_owned()))),
            _ => Err(BadKeyType),
        }
    }
}

#[cfg(test)]
impl From<&str> for GroupKey {
    fn from(s: &str) -> Self {
        Self(GroupId::String(s.to_string()))
    }
}

impl From<GroupKey> for serde_json::Value {
    fn from(key: GroupKey) -> Self {
        match key {
            GroupKey(GroupId::String(s)) => serde_json::Value::String(s),
            GroupKey(GroupId::Number(n)) => serde_json::Value::Number(n),
        }
    }
}

#[cfg(test)]
mod test {
    use segment::types::GroupId;

    #[test]
    fn group_key_from_values() {
        use std::convert::TryFrom;

        use serde_json::json;

        use super::GroupKey;

        let string = GroupKey::try_from(&json!("string")).unwrap();
        let int = GroupKey::try_from(&json!(1)).unwrap();
        let float = GroupKey::try_from(&json!(2.42)).unwrap();
        let int_array = GroupKey::try_from(&json!([5, 6, 7])).unwrap();
        let str_array = GroupKey::try_from(&json!(["a", "b", "c"])).unwrap();

        assert_eq!(string, GroupKey(GroupId::String("string".to_string())));
        assert_eq!(int, GroupKey(GroupId::Number(serde_json::Number::from(1))));
        assert_eq!(
            float,
            GroupKey(GroupId::Number(serde_json::Number::from_f64(2.42).unwrap()))
        );
        assert_eq!(
            int_array,
            GroupKey(GroupId::Number(serde_json::Number::from(5)))
        );
        assert_eq!(str_array, GroupKey(GroupId::String("a".to_string())));

        let bad_key = GroupKey::try_from(&json!(true));
        assert!(bad_key.is_err());

        let empty_array = GroupKey::try_from(&json!([]));
        assert!(empty_array.is_err());

        let empty_object = GroupKey::try_from(&json!({}));
        assert!(empty_object.is_err());

        let null = GroupKey::try_from(&serde_json::Value::Null);
        assert!(null.is_err());

        let nested_array = GroupKey::try_from(&json!([[1, 2, 3], [4, 5, 6]]));
        assert!(nested_array.is_err());

        let nested_object = GroupKey::try_from(&json!({"a": 1, "b": 2}));
        assert!(nested_object.is_err());
    }
}
