use std::error::Error;
use std::path::Path;

use examples::DATA_DIR;
use qdrant_edge::{
    Condition, EdgeShard, FacetRequest, FieldCondition, Filter, Match, ValueVariants,
};

fn main() -> Result<(), Box<dyn Error>> {
    let facet_dir = Path::new(DATA_DIR).join("facet_test");
    let snapshot_path = facet_dir.join("shard.snapshot");
    let shard_data = facet_dir.join("shard_data");

    if !snapshot_path.exists() {
        eprintln!("Snapshot not found at {}", snapshot_path.display());
        eprintln!("Run tools/prepare_facet_snapshot.sh first to create it.");
        std::process::exit(1);
    }

    println!("---- Unpack snapshot ----");

    // Clean up existing data directory
    if shard_data.exists() {
        fs_err::remove_dir_all(&shard_data)?;
    }

    // Unpack snapshot
    EdgeShard::unpack_snapshot(&snapshot_path, &shard_data)?;
    println!("Snapshot unpacked to {}", shard_data.display());

    println!("---- Load shard ----");
    let shard = EdgeShard::load(&shard_data, None)?;
    println!("Shard loaded. Points: {}", shard.info().points_count);

    println!("---- Test Facet on 'color' field ----");
    let response = shard.facet(FacetRequest {
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
    let response = shard.facet(FacetRequest {
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
    let filter = Filter::new_must(Condition::Field(FieldCondition::new_match(
        "color".try_into().unwrap(),
        Match::new_value(ValueVariants::String("red".to_string())),
    )));

    let response = shard.facet(FacetRequest {
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
