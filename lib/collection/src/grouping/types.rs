use std::collections::HashMap;

use segment::data_types::groups::GroupId;
use segment::types::{PointIdType, ScoredPoint};

use crate::operations::types::PointGroup;

#[derive(PartialEq, Debug)]
pub(super) enum AggregatorError {
    BadKeyType,
    KeyNotFound,
}
#[derive(Debug, Clone)]
pub(super) struct Group {
    pub hits: Vec<ScoredPoint>,
    pub key: GroupId,
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
            id: group.key,
            lookup: None,
        }
    }
}

#[cfg(test)]
mod test {
    use segment::data_types::groups::GroupId;

    #[test]
    fn group_key_from_values() {
        use std::convert::TryFrom;

        use serde_json::json;

        let string = GroupId::try_from(&json!("string")).unwrap();
        let int = GroupId::try_from(&json!(1)).unwrap();

        assert!(GroupId::try_from(&json!(2.42)).is_err());

        assert!(GroupId::try_from(&json!([5, 6, 7])).is_err());
        assert!(GroupId::try_from(&json!(["a", "b", "c"])).is_err());

        assert_eq!(string, GroupId::String("string".to_string()));
        assert_eq!(int.as_u64().unwrap(), 1);

        let bad_key = GroupId::try_from(&json!(true));
        assert!(bad_key.is_err());

        let empty_array = GroupId::try_from(&json!([]));
        assert!(empty_array.is_err());

        let empty_object = GroupId::try_from(&json!({}));
        assert!(empty_object.is_err());

        let null = GroupId::try_from(&serde_json::Value::Null);
        assert!(null.is_err());

        let nested_array = GroupId::try_from(&json!([[1, 2, 3], [4, 5, 6]]));
        assert!(nested_array.is_err());

        let nested_object = GroupId::try_from(&json!({"a": 1, "b": 2}));
        assert!(nested_object.is_err());
    }
}
