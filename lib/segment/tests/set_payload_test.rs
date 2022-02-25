#[cfg(test)]
mod tests {
    use segment::entry::entry_point::SegmentEntry;
    use segment::payload_storage::schema_storage::SchemaStorage;
    use segment::segment_constructor::build_segment;
    use segment::types::{
        Distance, Indexes, Payload, PayloadIndexType, PayloadType, SegmentConfig, StorageType,
    };
    use std::sync::Arc;
    use tempdir::TempDir;

    #[test]
    fn test_set_payload_from_json() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "boolean": "true",
            "floating": 30.5,
            "string_array": ["hello", "world"],
            "boolean_array": ["true", "false"],
            "geo_data": {"lon": 1.0, "lat": 1.0},
            "float_array": [1.0, 2.0],
            "integer_array": [1, 2],
            "metadata": {
                "height": 50,
                "width": 60,
                "temperature": 60.5,
                "nested": {
                    "feature": 30.5
                },
                "integer_array": [1, 2]
            }
        }"#;

        let dir = TempDir::new("payload_dir").unwrap();
        let dim = 2;
        let config = SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Plain),
            storage_type: StorageType::InMemory,
            distance: Distance::Dot,
        };

        let mut segment =
            build_segment(dir.path(), &config, Arc::new(SchemaStorage::new())).unwrap();
        segment.upsert_point(0, 0.into(), &[1.0, 1.0]).unwrap();
        let payload_data: Payload = serde_json::from_str(data).unwrap();
        segment
            .set_full_payload(0, 0.into(), &payload_data)
            .unwrap();
        let payload = segment.payload(0.into()).unwrap();
        assert!(payload.get("geo_data").is_some());
        assert!(payload.get("name").is_some());
        assert!(payload.get("age").is_some());
        assert!(payload.get("boolean").is_some());
        assert!(payload.get("floating").is_some());
        assert!(payload.get("metadata.temperature").is_some());
        assert!(payload.get("metadata.width").is_some());
        assert!(payload.get("metadata.height").is_some());
        assert!(payload.get("metadata.nested.feature").is_some());
        assert!(payload.get("string_array").is_some());
        assert!(payload.get("float_array").is_some());
        assert!(payload.get("integer_array").is_some());
        assert!(payload.get("boolean_array").is_some());
        assert!(payload.get("metadata.integer_array").is_some());

        match payload.get("name") {
            Some(PayloadType::Keyword(x)) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], "John Doe".to_string());
            }
            _ => panic!(),
        }
        match payload.get("age") {
            Some(PayloadType::Integer(x)) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 43);
            }
            _ => panic!(),
        }
        match payload.get("floating") {
            Some(PayloadType::Float(x)) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 30.5);
            }
            _ => panic!(),
        }
        match payload.get("boolean") {
            Some(PayloadType::Keyword(x)) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], "true");
            }
            _ => panic!(),
        }
        match payload.get("metadata.temperature") {
            Some(PayloadType::Float(x)) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 60.5);
            }
            _ => panic!(),
        }
        match payload.get("metadata.width") {
            Some(PayloadType::Integer(x)) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 60);
            }
            _ => panic!(),
        }
        match payload.get("metadata.height") {
            Some(PayloadType::Integer(x)) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 50);
            }
            _ => panic!(),
        }
        match payload.get("metadata.nested.feature") {
            Some(PayloadType::Float(x)) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 30.5);
            }
            _ => panic!(),
        }
        match payload.get("string_array") {
            Some(PayloadType::Keyword(x)) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], "hello");
                assert_eq!(x[1], "world");
            }
            _ => panic!(),
        }
        match payload.get("integer_array") {
            Some(PayloadType::Integer(x)) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], 1);
                assert_eq!(x[1], 2);
            }
            _ => panic!(),
        }
        match payload.get("metadata.integer_array") {
            Some(PayloadType::Integer(x)) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], 1);
                assert_eq!(x[1], 2);
            }
            _ => panic!(),
        }
        match payload.get("float_array") {
            Some(PayloadType::Float(x)) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], 1.0);
                assert_eq!(x[1], 2.0);
            }
            _ => panic!(),
        }
        match payload.get("boolean_array") {
            Some(PayloadType::Keyword(x)) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], "true");
                assert_eq!(x[1], "false");
            }
            _ => panic!(),
        }
        match payload.get("geo_data") {
            Some(PayloadType::Geo(x)) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0].lat, 1.0);
                assert_eq!(x[0].lon, 1.0);
            }
            _ => panic!(),
        }
    }
}
