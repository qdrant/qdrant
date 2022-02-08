#[cfg(test)]
mod tests {
    use segment::entry::entry_point::SegmentEntry;
    use segment::payload_storage::schema_storage::SchemaStorage;
    use segment::segment_constructor::build_segment;
    use segment::types::{
        Distance, Indexes, PayloadIndexType, PayloadKeyType, PayloadType, SegmentConfig,
        StorageType,
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
            "geo_data": {"type": "geo", "value": {"lon": 1.0, "lat": 1.0}},
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
        segment
            .set_full_payload_with_json(0, 0.into(), &data.to_string())
            .unwrap();
        let payload = segment.payload(0.into()).unwrap();
        let keys: Vec<PayloadKeyType> = payload.keys().cloned().collect();
        assert!(keys.contains(&"geo_data".to_string()));
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"age".to_string()));
        assert!(keys.contains(&"boolean".to_string()));
        assert!(keys.contains(&"floating".to_string()));
        assert!(keys.contains(&"metadata__temperature".to_string()));
        assert!(keys.contains(&"metadata__width".to_string()));
        assert!(keys.contains(&"metadata__height".to_string()));
        assert!(keys.contains(&"metadata__nested__feature".to_string()));
        assert!(keys.contains(&"string_array".to_string()));
        assert!(keys.contains(&"float_array".to_string()));
        assert!(keys.contains(&"integer_array".to_string()));
        assert!(keys.contains(&"boolean_array".to_string()));
        assert!(keys.contains(&"metadata__integer_array".to_string()));

        match &payload[&"name".to_string()] {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], "John Doe".to_string());
            }
            _ => panic!(),
        }
        match &payload[&"age".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 43);
            }
            _ => panic!(),
        }
        match &payload[&"floating".to_string()] {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 30.5);
            }
            _ => panic!(),
        }
        match &payload[&"boolean".to_string()] {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], "true");
            }
            _ => panic!(),
        }
        match &payload[&"metadata__temperature".to_string()] {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 60.5);
            }
            _ => panic!(),
        }
        match &payload[&"metadata__width".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 60);
            }
            _ => panic!(),
        }
        match &payload[&"metadata__height".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 50);
            }
            _ => panic!(),
        }
        match &payload[&"metadata__nested__feature".to_string()] {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0], 30.5);
            }
            _ => panic!(),
        }
        match &payload[&"string_array".to_string()] {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], "hello");
                assert_eq!(x[1], "world");
            }
            _ => panic!(),
        }
        match &payload[&"integer_array".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], 1);
                assert_eq!(x[1], 2);
            }
            _ => panic!(),
        }
        match &payload[&"metadata__integer_array".to_string()] {
            PayloadType::Integer(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], 1);
                assert_eq!(x[1], 2);
            }
            _ => panic!(),
        }
        match &payload[&"float_array".to_string()] {
            PayloadType::Float(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], 1.0);
                assert_eq!(x[1], 2.0);
            }
            _ => panic!(),
        }
        match &payload[&"boolean_array".to_string()] {
            PayloadType::Keyword(x) => {
                assert_eq!(x.len(), 2);
                assert_eq!(x[0], "true");
                assert_eq!(x[1], "false");
            }
            _ => panic!(),
        }
        match &payload[&"geo_data".to_string()] {
            PayloadType::Geo(x) => {
                assert_eq!(x.len(), 1);
                assert_eq!(x[0].lat, 1.0);
                assert_eq!(x[0].lon, 1.0);
            }
            _ => panic!(),
        }
    }
}
