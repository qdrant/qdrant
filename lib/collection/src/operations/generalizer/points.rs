use crate::operations::generalizer::Generalizer;
use crate::operations::types::{PointRequestInternal, ScrollRequestInternal};

impl Generalizer for ScrollRequestInternal {
    fn remove_details(&self) -> Self {
        let ScrollRequestInternal {
            offset,
            limit,
            filter,
            with_payload,
            with_vector,
            order_by,
        } = self;

        Self {
            offset: *offset,
            limit: *limit,
            filter: filter.clone(),
            with_payload: with_payload.clone(),
            with_vector: with_vector.clone(),
            order_by: order_by.clone(),
        }
    }
}

impl Generalizer for PointRequestInternal {
    fn remove_details(&self) -> Self {
        let PointRequestInternal {
            ids,
            with_payload,
            with_vector,
        } = self;

        Self {
            ids: ids.clone(),
            with_payload: with_payload.clone(),
            with_vector: with_vector.clone(),
        }
    }
}
