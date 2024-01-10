use http::{HeaderMap, Method, Uri};

use crate::issue::Issue;
use crate::solution::{Action, Solution};

pub struct UnindexedField {
    field_name: String,
    field_type: String,
    collection: String,
}

impl Issue for UnindexedField {
    fn code(&self) -> String {
        format!(
            "UNINDEXED_FIELD,{},{},{}",
            self.collection, self.field_name, self.field_type
        )
    }
    fn description(&self) -> String {
        format!(
            "Unindexed field '{}' of type '{}' is slowing down queries in collection '{}'",
            self.field_name, self.field_type, self.collection
        )
    }

    fn solution(&self) -> Result<Solution, String> {
        let uri = Uri::builder()
            .path_and_query(format!("/collection/{}/indexes", self.collection).as_str())
            .build()
            .map_err(|e| e.to_string())?;

        let request_body = serde_json::json!({
            "field_name": self.field_name,
            "field_schema": self.field_type,
        });

        Ok(Solution {
            message: format!(
                "Create an index on field '{}' of type '{}' in collection '{}'. Check the documentation for more details: https://qdrant.tech/documentation/concepts/indexing/#payload-index",
                self.field_name, self.field_type, self.collection
            ),
            action: Some(Action {
                method: Method::POST,
                uri,
                headers: HeaderMap::new(),
                body: Some(request_body),
            }),
        })
    }
}
