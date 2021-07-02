use crate::entry::entry_point::OperationResult;
use crate::types::{
    Filter, PayloadInterface, PayloadKeyType, PayloadSchemaType, PayloadType, PointOffsetType,
    TheMap,
};
use serde_json::value::Value;

/// Trait for payload data storage. Should allow filter checks
pub trait PayloadStorage {
    fn assign_all_with_value(
        &mut self,
        point_id: PointOffsetType,
        payload: TheMap<PayloadKeyType, serde_json::value::Value>,
    ) -> OperationResult<()> {
        fn _extract_payloads<'a, I>(
            _payload: I,
            prefix_key: Option<PayloadKeyType>,
        ) -> Vec<(PayloadKeyType, PayloadType)>
        where
            I: Iterator<Item = (&'a PayloadKeyType, &'a serde_json::value::Value)>,
        {
            fn _fn(
                prefix: &Option<PayloadKeyType>,
                k: &PayloadKeyType,
                v: &Value,
            ) -> Vec<(PayloadKeyType, PayloadType)> {
                let key = match &prefix {
                    None => k.to_string(),
                    Some(_k) => (_k.to_owned() + "__" + k),
                };

                let opt_payload_interface: Result<PayloadInterface, _> =
                    serde_json::from_value(v.to_owned());
                match opt_payload_interface {
                    Ok(payload_interface) => vec![(key, PayloadType::from(&payload_interface))],
                    _ => match v {
                        Value::Object(ref x) => _extract_payloads(x.iter(), Some(key)),
                        _ => vec![],
                    },
                }
            }
            _payload
                .flat_map(|(k, value)| _fn(&prefix_key, k, value))
                .collect()
        }
        self.drop(point_id)?;
        let inner_payloads = _extract_payloads(payload.iter(), None);
        for (key, value) in inner_payloads.iter() {
            self.assign(point_id, key, value.to_owned())?;
        }
        Ok(())
    }

    /// Assign same payload to each given point
    fn assign_all(
        &mut self,
        point_id: PointOffsetType,
        payload: TheMap<PayloadKeyType, PayloadType>,
    ) -> OperationResult<()> {
        self.drop(point_id)?;
        for (key, value) in payload {
            self.assign(point_id, &key, value)?;
        }

        Ok(())
    }

    /// Assign payload to a concrete point with a concrete payload value
    fn assign(
        &mut self,
        point_id: PointOffsetType,
        key: &PayloadKeyType,
        payload: PayloadType,
    ) -> OperationResult<()>;

    /// Get payload for point
    fn payload(&self, point_id: PointOffsetType) -> TheMap<PayloadKeyType, PayloadType>;

    /// Delete payload by key
    fn delete(
        &mut self,
        point_id: PointOffsetType,
        key: &PayloadKeyType,
    ) -> OperationResult<Option<PayloadType>>;

    /// Drop all payload of the point
    fn drop(
        &mut self,
        point_id: PointOffsetType,
    ) -> OperationResult<Option<TheMap<PayloadKeyType, PayloadType>>>;

    /// Completely drop payload. Pufff!
    fn wipe(&mut self) -> OperationResult<()>;

    /// Force persistence of current storage state.
    fn flush(&self) -> OperationResult<()>;

    /// Get payload schema, automatically generated from payload
    fn schema(&self) -> TheMap<PayloadKeyType, PayloadSchemaType>;

    /// Iterate all point ids with payload
    fn iter_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_>;
}

pub trait ConditionChecker {
    /// Check if point satisfies filter condition. Return true if satisfies
    fn check(&self, point_id: PointOffsetType, query: &Filter) -> bool;
}
