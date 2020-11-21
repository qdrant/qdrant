use serde::{Deserialize, Serialize};
use crate::types::{PointOffsetType, FloatPayloadType, IntPayloadType, GeoPoint, PayloadType};
use std::collections::HashSet;
use std::hash::Hash;


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Element<N> {
    pub id: PointOffsetType,
    pub value: N,
}

pub struct IndexBuilder<N> {
    pub ids: HashSet<PointOffsetType>,
    pub elements: Vec<Element<N>>,
}

impl<N> IndexBuilder<N> {
    pub fn new() -> Self {
        IndexBuilder {
            ids: Default::default(),
            elements: vec![],
        }
    }

    pub fn add(&mut self, id: PointOffsetType, value: N) {
        self.ids.insert(id);
        self.elements.push(Element { id, value })
    }
}

pub enum IndexBuilderTypes {
    Float(IndexBuilder<FloatPayloadType>),
    Integer(IndexBuilder<IntPayloadType>),
    Keyword(IndexBuilder<String>),
    Geo(IndexBuilder<GeoPoint>),
}

impl IndexBuilderTypes {
    pub fn add(&mut self, id: PointOffsetType, payload_value: &PayloadType) {
        match self {
            IndexBuilderTypes::Float(builder) => match payload_value {
                PayloadType::Float(x) => x.iter().for_each(|val| builder.add(id, val.to_owned())),
                _ => panic!("Unexpected payload type: {:?}", payload_value)
            },
            IndexBuilderTypes::Integer(builder) => match payload_value {
                PayloadType::Integer(x) => x.iter().for_each(|val| builder.add(id, val.to_owned())),
                _ => panic!("Unexpected payload type: {:?}", payload_value)
            },
            IndexBuilderTypes::Keyword(builder) => match payload_value {
                PayloadType::Keyword(x) => x.iter().for_each(|val| builder.add(id, val.to_owned())),
                _ => panic!("Unexpected payload type: {:?}", payload_value)
            },
            IndexBuilderTypes::Geo(builder) => match payload_value {
                PayloadType::Geo(x) => x.iter().for_each(|val| builder.add(id, val.to_owned())),
                _ => panic!("Unexpected payload type: {:?}", payload_value)
            },
        };
    }
}
