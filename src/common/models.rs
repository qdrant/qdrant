use serde::{Serialize, Deserialize};
use schemars::{JsonSchema};
use std::fmt::Debug;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiStatus {
    Ok,
    Error(String),
}


#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ApiResponse<D: Serialize + Debug> {
    pub result: Option<D>,
    pub status: ApiStatus,
    pub time: f64,
}

