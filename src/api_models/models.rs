use serde;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;


#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Status {
    Ok,
    Error(String)
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
struct ApiResponse<D: Serialize + Deserialize + Debug> {
    result: Option<D>,
    status: Status,
    time: float
}


#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
struct CollectionDescription {
    name: String
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
struct CollectionsResponse {
    collections: Vec<CollectionDescription>
}
