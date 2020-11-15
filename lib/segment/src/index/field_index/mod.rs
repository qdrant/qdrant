pub mod numeric_index;
pub mod index_builder;


#[derive(Debug)]
pub struct EstimationResult {
    pub min: usize,
    pub exp: usize,
    pub max: usize
}
