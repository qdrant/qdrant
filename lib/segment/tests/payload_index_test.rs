#[cfg(test)]
mod tests {
    use rand::prelude::ThreadRng;
    use rand::seq::SliceRandom;
    use segment::types::{PayloadType, VectorElementType, SegmentConfig, Indexes, PayloadIndexType, Distance, StorageType};
    use rand::Rng;
    use tempdir::TempDir;
    use segment::segment_constructor::segment_constructor::build_segment;

    const ADJECTIVE: &'static [&'static str] = &[
        "jobless",
        "rightful",
        "breakable",
        "impartial",
        "shocking",
        "faded",
        "phobic",
        "overt",
        "like",
        "wide-eyed",
        "broad",
    ];

    const NOUN: &'static [&'static str] =&[
        "territory",
        "jam",
        "neck",
        "chicken",
        "cap",
        "kiss",
        "veil",
        "trail",
        "size",
        "digestion",
        "rod",
        "seed",
    ];

    fn random_keyword(rnd_gen: &mut ThreadRng) -> String {
        let random_adj = ADJECTIVE.choose(rnd_gen).unwrap();
        let random_noun = NOUN.choose(rnd_gen).unwrap();
        format!("{} {}", random_adj, random_noun)
    }

    fn random_keyword_payload(rnd_gen: &mut ThreadRng) -> PayloadType {
        PayloadType::Keyword(vec![random_keyword(rnd_gen)])
    }

    fn random_int_payload(rnd_gen: &mut ThreadRng) -> PayloadType {
        let val1: i64 = rnd_gen.gen_range(0..500);
        let val2: i64 = rnd_gen.gen_range(0..500);
        let val3: i64 = rnd_gen.gen_range(0..500);

        PayloadType::Integer(vec![val1, val2, val3])
    }

    fn random_vector(rnd_gen: &mut ThreadRng, size: usize) -> Vec<VectorElementType> {
        (0..size).map(|_| rnd_gen.gen()).collect()
    }

    #[test]
    fn test_struct_payload_index() {
        // Compare search with plain and struct indexes

        let dir1 = TempDir::new("segment1_dir").unwrap();
        let dir2 = TempDir::new("segment2_dir").unwrap();

        let dim = 5;

        let mut config = SegmentConfig {
            vector_size: dim,
            index: Indexes::Plain {},
            payload_index: Some(PayloadIndexType::Plain),
            storage_type: StorageType::InMemory,
            distance: Distance::Dot,
        };

        let mut plain_segment = build_segment(dir1.path(), &config).unwrap();
        config.payload_index = Some(PayloadIndexType::Struct);
        let mut struct_segment = build_segment(dir2.path(), &config).unwrap();

        // ToDo: Init both segments with same data
        // ToDo: Compare indexed and un-indexed search results
    }
}