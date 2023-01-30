use collection::operations::consistency_params::ReadConsistency;
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Deserialize, JsonSchema)]
pub struct ReadParams {
    pub read_consistency: Option<ReadConsistency>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn read_params_deserialization() {
        let params: ReadParams = serde_urlencoded::from_str("read_consistency=1").unwrap();
        assert_eq!(
            params,
            ReadParams {
                read_consistency: Some(ReadConsistency::Factor(1))
            }
        );
    }
}
