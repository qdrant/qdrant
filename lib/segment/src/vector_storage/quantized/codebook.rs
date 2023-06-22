use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::common::file_operations::read_json;
use crate::entry::entry_point::OperationResult;
use crate::types::CodebooksConfig;

#[derive(Serialize, Deserialize, Clone)]
pub enum Codebook {
    ProductQuantization(ProductQuantizationCodebookEnum),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ProductQuantizationCodebookEnum {
    pub product_quantization: ProductQuantizationCodebook,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ProductQuantizationCodebook {
    pub centroids: Vec<Vec<Vec<f32>>>,
}

impl Codebook {
    pub fn load(name: &str, config: &Option<CodebooksConfig>) -> OperationResult<Self> {
        let path = if let Some(_config) = config {
            // todo(ivan)
            PathBuf::from(name)
        } else {
            PathBuf::from(name)
        };
        Ok(read_json(path.as_path())?)
    }
}
