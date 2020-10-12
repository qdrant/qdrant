use serde;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;


#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Ok,
    Error(String)
}


#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ApiResponse<D: Serialize + Debug> {
    pub result: Option<D>,
    pub status: Status,
    pub time: f64
}


#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct CollectionDescription {
    pub name: String
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct CollectionsResponse {
    pub collections: Vec<CollectionDescription>
}
