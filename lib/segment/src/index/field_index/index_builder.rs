use serde::{Deserialize, Serialize};
use crate::types::PointOffsetType;
use std::collections::HashSet;


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Element<N> {
    pub id: PointOffsetType,
    pub value: N,
}

pub struct IndexBuilder<N> {
    pub ids: HashSet<PointOffsetType>,
    pub elements: Vec<Element<N>>
}

impl<N> IndexBuilder<N> {
    pub fn new() -> Self {
        IndexBuilder {
            ids: Default::default(),
            elements: vec![]
        }
    }

    pub fn add(&mut self, id: PointOffsetType, value: N) {
        self.ids.insert(id);
        self.elements.push(Element { id, value })
    }
}
