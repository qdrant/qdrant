pub type PointIdType = u64;
pub type PayloadKeyType = String;
pub type VectorType = Vec<f64>;

use serde::{Deserialize, Serialize};


#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")] 
pub enum PayloadType {
    Keyword(String),
    Integer(i64),
    Float(f64),
    Geo{
        lon: f64,
        lat: f64,
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use serde_json;

    #[test]
    fn test_name() {
        let label = PayloadType::Keyword("Hello".to_owned());
        let label_json = serde_json::to_string(&label).unwrap();
        println!("{}", label_json);
    }
}