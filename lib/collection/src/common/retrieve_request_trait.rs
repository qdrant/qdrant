use api::rest::schema::ShardKeySelector;
use segment::data_types::vectors::DEFAULT_VECTOR_NAME;
use segment::types::{PointIdType, VectorNameBuf};

use crate::operations::types::{DiscoverRequestInternal, RecommendRequestInternal, UsingVector};
use crate::operations::universal_query::collection_query::{
    CollectionQueryResolveRequest, VectorInputInternal, VectorQuery,
};

const EMPTY_SHARD_KEY_SELECTOR: Option<ShardKeySelector> = None;

pub trait RetrieveRequest {
    fn get_lookup_collection(&self) -> Option<&String>;

    fn get_referenced_point_ids(&self) -> Vec<PointIdType>;

    fn get_lookup_vector_name(&self) -> VectorNameBuf;

    fn get_lookup_shard_key(&self) -> &Option<ShardKeySelector>;
}

impl RetrieveRequest for RecommendRequestInternal {
    fn get_lookup_collection(&self) -> Option<&String> {
        self.lookup_from.as_ref().map(|x| &x.collection)
    }

    fn get_referenced_point_ids(&self) -> Vec<PointIdType> {
        self.positive
            .iter()
            .chain(self.negative.iter())
            .filter_map(|example| example.as_point_id())
            .collect()
    }

    fn get_lookup_vector_name(&self) -> VectorNameBuf {
        match &self.lookup_from {
            None => match &self.using {
                None => DEFAULT_VECTOR_NAME.to_owned(),
                Some(UsingVector::Name(vector_name)) => vector_name.clone(),
            },
            Some(lookup_from) => match &lookup_from.vector {
                None => DEFAULT_VECTOR_NAME.to_owned(),
                Some(vector_name) => vector_name.clone(),
            },
        }
    }

    fn get_lookup_shard_key(&self) -> &Option<ShardKeySelector> {
        self.lookup_from
            .as_ref()
            .map(|x| &x.shard_key)
            .unwrap_or(&EMPTY_SHARD_KEY_SELECTOR)
    }
}

impl RetrieveRequest for DiscoverRequestInternal {
    fn get_lookup_collection(&self) -> Option<&String> {
        self.lookup_from.as_ref().map(|x| &x.collection)
    }

    fn get_referenced_point_ids(&self) -> Vec<PointIdType> {
        let mut res = Vec::new();

        match &self.target {
            None => {}
            Some(example) => {
                if let Some(point_id) = example.as_point_id() {
                    res.push(point_id);
                }
            }
        }

        if let Some(context) = &self.context {
            for pair in context {
                if let Some(pos_id) = pair.positive.as_point_id() {
                    res.push(pos_id);
                }
                if let Some(neg_id) = pair.negative.as_point_id() {
                    res.push(neg_id);
                }
            }
        }

        res
    }

    fn get_lookup_vector_name(&self) -> VectorNameBuf {
        match &self.lookup_from {
            None => match &self.using {
                None => DEFAULT_VECTOR_NAME.to_owned(),
                Some(UsingVector::Name(vector_name)) => vector_name.clone(),
            },
            Some(lookup_from) => match &lookup_from.vector {
                None => DEFAULT_VECTOR_NAME.to_owned(),
                Some(vector_name) => vector_name.clone(),
            },
        }
    }

    fn get_lookup_shard_key(&self) -> &Option<ShardKeySelector> {
        self.lookup_from
            .as_ref()
            .map(|x| &x.shard_key)
            .unwrap_or(&EMPTY_SHARD_KEY_SELECTOR)
    }
}

impl RetrieveRequest for CollectionQueryResolveRequest {
    fn get_lookup_collection(&self) -> Option<&String> {
        self.lookup_from.as_ref().map(|x| &x.collection)
    }

    fn get_referenced_point_ids(&self) -> Vec<PointIdType> {
        self.referenced_ids.clone()
    }

    fn get_lookup_vector_name(&self) -> VectorNameBuf {
        match &self.lookup_from {
            None => self.using.clone(),
            Some(lookup_from) => match &lookup_from.vector {
                None => self.using.clone(),
                Some(vector_name) => vector_name.clone(),
            },
        }
    }

    fn get_lookup_shard_key(&self) -> &Option<ShardKeySelector> {
        self.lookup_from
            .as_ref()
            .map(|x| &x.shard_key)
            .unwrap_or(&EMPTY_SHARD_KEY_SELECTOR)
    }
}
impl VectorQuery<VectorInputInternal> {
    pub fn get_referenced_ids(&self) -> Vec<&PointIdType> {
        self.flat_iter()
            .filter_map(VectorInputInternal::as_id)
            .collect()
    }
}
