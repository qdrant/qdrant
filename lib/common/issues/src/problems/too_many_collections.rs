use crate::issue::Issue;
use crate::solution::Solution;

pub struct TooManyCollections;

impl Issue for TooManyCollections {
    fn code(&self) -> String {
        "TOO_MANY_COLLECTIONS".to_string()
    }

    fn description(&self) -> String {
        "It looks like you have too many collections.\nIf your architecture creates collections programmatically, it's probably better to restructure your solution into a fixed number of them. \nLearn more here: https://qdrant.tech/documentation/guides/multiple-partitions/".to_string()
    }

    fn solution(&self) -> Result<Solution, String> {
        Ok(Solution {
            message: "Restructure your solution into a fixed number of collections".to_string(),
            action: None,
        })
    }
}
