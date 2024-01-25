use crate::issue::Issue;
use crate::solution::Solution;

pub struct TooManyCollections;

impl From<TooManyCollections> for Issue {
    fn from(_: TooManyCollections) -> Self {
        Issue {
            code: "TOO_MANY_COLLECTIONS".to_string(),
            description:
            "It looks like you have too many collections.\nIf your architecture creates collections programmatically, it's probably better to restructure your solution into a fixed number of them. \nLearn more here: https://qdrant.tech/documentation/guides/multiple-partitions/".to_string(),
            solution: Solution::Refactor(
                "Restructure your solution into a fixed number of collections".to_string(),
            ),
        }
    }
}
