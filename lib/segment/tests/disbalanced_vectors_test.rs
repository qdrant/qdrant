mod fixtures;

const NUM_VECTORS_1: u64 = 300;
const NUM_VECTORS_2: u64 = 500;

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use segment::data_types::named_vectors::NamedVectors;
    use segment::entry::entry_point::SegmentEntry;
    use segment::segment::Segment;
    use segment::segment_constructor::segment_builder::SegmentBuilder;
    use segment::segment_constructor::simple_segment_constructor::build_multivec_segment;
    use segment::types::Distance;
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_rebuild_with_removed_vectors() {
        let dir = Builder::new().prefix("segment_dir").tempdir().unwrap();
        let temp_dir = Builder::new().prefix("segment_temp_dir").tempdir().unwrap();

        let stopped = AtomicBool::new(false);

        let mut segment1 = build_multivec_segment(dir.path(), 4, 6, Distance::Dot).unwrap();
        let mut segment2 = build_multivec_segment(dir.path(), 4, 6, Distance::Dot).unwrap();

        for i in 0..NUM_VECTORS_1 {
            segment1
                .upsert_vector(
                    1,
                    i.into(),
                    &NamedVectors::from([
                        ("vector1".to_string(), vec![i as f32, 0., 0., 0.]),
                        ("vector2".to_string(), vec![0., i as f32, 0., 0., 0., 0.]),
                    ]),
                )
                .unwrap();
        }

        for i in 0..NUM_VECTORS_2 {
            let vectors = if i % 5 == 0 {
                NamedVectors::from([("vector1".to_string(), vec![0., 0., i as f32, 0.])])
            } else {
                NamedVectors::from([
                    ("vector1".to_string(), vec![0., 0., i as f32, 0.]),
                    ("vector2".to_string(), vec![0., 0., 0., i as f32, 0., 0.]),
                ])
            };

            segment2
                .upsert_vector(1, (NUM_VECTORS_1 + i).into(), &vectors)
                .unwrap();
        }

        for i in 0..NUM_VECTORS_2 {
            if i % 2 == 0 {
                segment2
                    .delete_point(2, (NUM_VECTORS_1 + i).into())
                    .unwrap();
            }
            if i % 3 == 0 {
                segment2
                    .delete_vector(2, (NUM_VECTORS_1 + i).into(), "vector1")
                    .unwrap();
                segment2
                    .delete_vector(2, (NUM_VECTORS_1 + i).into(), "vector2")
                    .unwrap();
            }
            if i % 3 == 1 {
                segment2
                    .delete_vector(2, (NUM_VECTORS_1 + i).into(), "vector2")
                    .unwrap();
            }
        }

        for i in 0..20 {
            if i % 2 == 0 {
                continue;
            }
            let idx = NUM_VECTORS_1 + i;
            let vec = segment2.all_vectors(idx.into()).unwrap();
            eprintln!("vec[{}] = {:?}", idx, vec);
        }

        let mut builder =
            SegmentBuilder::new(dir.path(), temp_dir.path(), &segment1.segment_config).unwrap();

        builder.update_from(&segment1, &stopped).unwrap();
        builder.update_from(&segment2, &stopped).unwrap();

        let merged_segment: Segment = builder.build(&stopped).unwrap();

        eprintln!("-------------------");

        for i in 0..20 {
            if i % 2 == 0 {
                continue;
            }
            let idx = NUM_VECTORS_1 + i;
            let vec = merged_segment.all_vectors(idx.into()).unwrap();
            eprintln!("vec[{}] = {:?}", idx, vec);
        }
    }
}
