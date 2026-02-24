use std::error::Error;
use std::path::Path;

use qdrant_edge::EdgeShard;
use qdrant_edge::shard::facet::FacetRequestInternal;

const SNAPSHOT_PATH: &str = "./test_edge_facet/shard.snapshot";
const DATA_DIR: &str = "./test_edge_facet/shard_data";

fn main() -> Result<(), Box<dyn Error>> {
    println!("---- Unpack snapshot ----");

    // Clean up existing data directory
    if Path::new(DATA_DIR).exists() {
        fs_err::remove_dir_all(DATA_DIR)?;
    }

    // Unpack snapshot
    EdgeShard::unpack_snapshot(Path::new(SNAPSHOT_PATH), Path::new(DATA_DIR))?;
    println!("Snapshot unpacked to {DATA_DIR}");

    println!("---- Load shard ----");
    let shard = EdgeShard::load(Path::new(DATA_DIR), None)?;
    println!("Shard loaded. Points: {}", shard.info().points_count);

    println!("---- Test Facet on 'color' field ----");
    let response = shard.facet(FacetRequestInternal {
        key: "color".try_into().unwrap(),
        limit: 10,
        filter: None,
        exact: false,
    })?;

    println!("Facet results for 'color':");
    for hit in &response.hits {
        println!("  {:?}: {}", hit.value, hit.count);
    }

    println!("---- Test Facet on 'city' field ----");
    let response = shard.facet(FacetRequestInternal {
        key: "city".try_into().unwrap(),
        limit: 10,
        filter: None,
        exact: false,
    })?;

    println!("Facet results for 'city':");
    for hit in &response.hits {
        println!("  {:?}: {}", hit.value, hit.count);
    }

    println!("---- Test Facet with filter ----");
    use qdrant_edge::segment::types::{Condition, FieldCondition, Filter, Match, ValueVariants};

    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        "color".try_into().unwrap(),
        Match::new_value(ValueVariants::String("red".to_string())),
    )));

    let response = shard.facet(FacetRequestInternal {
        key: "city".try_into().unwrap(),
        limit: 10,
        filter: Some(filter),
        exact: false,
    })?;

    println!("Facet results for 'city' where color='red':");
    for hit in &response.hits {
        println!("  {:?}: {}", hit.value, hit.count);
    }

    println!("\n✅ All facet tests passed!");
    Ok(())
}
