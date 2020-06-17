
pub trait Metric<El> {
    fn distance(&self, v1: &Vec<El>, v2: &Vec<El>) -> f32;
    
    /// Necessary vector transformations performed before adding it to the collection (like normalization)
    fn preprocess(&self, vector: Vec<El>) -> Vec<El>;
}