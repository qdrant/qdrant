use http::{HeaderMap, Method, Uri};

use crate::issue::Issue;
use crate::solution::{Action, ImmediateSolution, Solution};

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

    fn solution(&self) -> Solution {
        let uri = match Uri::builder()
            .path_and_query(format!("/collection/{}/indexes", self.collection).as_str())
            .build()
        {
            Ok(uri) => uri,
            Err(e) => {
                log::warn!("Failed to build uri: {}", e);
                return Solution::None;
            }
        };

        let request_body = serde_json::json!({
            "field_name": self.field_name,
            "field_schema": self.field_type,
        });

        Solution::Immediate(ImmediateSolution {
            message: format!(
                "Create an index on field '{}' of type '{}' in collection '{}'. Check the documentation for more details: https://qdrant.tech/documentation/concepts/indexing/#payload-index",
                self.field_name, self.field_type, self.collection
            ),
            action: Action {
                method: Method::POST,
                uri,
                headers: HeaderMap::new(),
                body: Some(request_body),
            },
        })
    }
}
