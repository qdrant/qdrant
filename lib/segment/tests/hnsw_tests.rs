mod fixtures;


#[cfg(test)]
mod tests {
    use crate::fixtures::segment::{build_segment_1};
    use tempdir::TempDir;

    #[test]
    #[ignore]
    fn test_hnsw() {
        let dir = TempDir::new("segment_dir").unwrap();

        let segment1 = build_segment_1(dir.path());

        let hnsw_path = dir.path().join("hnsw_index");

    }
}