use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use clap::{arg, Parser};
use segment::entry::entry_point::OperationResult;
use segment::index::VectorIndexEnum;
use segment::segment_constructor::load_segment;
use segment::types::{ExtendedPointId, PointIdType};
use segment::vector_storage::ScoredPointOffset;
use segment::{self};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    positive: Vec<u64>,

    #[arg(short, long)]
    negative: Vec<u64>,
}

fn main() {
    let args = Args::parse();

    let positive = args
        .positive
        .iter()
        .map(|id| ExtendedPointId::NumId(*id))
        .collect::<Vec<_>>();
    let negative = args
        .negative
        .iter()
        .map(|id| ExtendedPointId::NumId(*id))
        .collect::<Vec<_>>();

    let result = custom_search(&positive, &negative);

    match result {
        Ok(result) => {
            println!("Result: {:?}", result);
        }
        Err(err) => {
            println!("Error: {:?}", err);
        }
    }
}

fn custom_search(
    positives: &[PointIdType],
    negatives: &[PointIdType],
) -> OperationResult<Vec<ScoredPointOffset>> {
    let segment_path = dbg!(get_hnsw_segment_path());

    let segment = load_segment(&segment_path)
        .expect("Should not fail loading")
        .expect("Should have some segment");

    let is_stopped = AtomicBool::new(false);

    let index = segment.vector_data[""].vector_index.borrow();

    let hnsw = match index.deref() {
        VectorIndexEnum::HnswRam(hnsw) => hnsw,
        _ => unimplemented!("Only RAM HNSW is supported for now"),
    };

    let result = hnsw.custom_search_with_graph(positives, negatives, 10, &is_stopped);

    Ok(result)
}

#[allow(dead_code)]
fn search(query: &[f32]) -> OperationResult<Vec<ScoredPointOffset>> {
    let segment_path = dbg!(get_hnsw_segment_path());

    let segment = load_segment(&segment_path)
        .expect("Should not fail loading")
        .expect("Should have some segment");

    let is_stopped = AtomicBool::new(false);

    let index = segment.vector_data[""].vector_index.borrow();

    let hnsw = match index.deref() {
        VectorIndexEnum::HnswRam(hnsw) => hnsw,
        _ => unimplemented!("Only RAM HNSW is supported for now"),
    };

    let result = hnsw.search_with_graph(query, None, 10, None, &is_stopped);

    Ok(result)
}

fn get_hnsw_segment_path() -> PathBuf {
    let segments = Path::new("../../storage/collections/food/0/segments/");
    segments
        .read_dir()
        .unwrap()
        .find(|component| {
            component.as_ref().is_ok_and(|dir_entry| {
                // Open segment config
                let mut buf = String::new();
                if let Ok(mut file) = File::open(dir_entry.path().join("segment.json")) {
                    let _ = file.read_to_string(&mut buf);
                } else {
                    return false;
                };

                // Check if segment index is hnsw
                let segment_config: serde_json::Value = serde_json::from_str(&buf).unwrap();
                let index_type = segment_config["config"]["vector_data"][""]["index"]["type"]
                    .as_str()
                    .unwrap();
                index_type == "hnsw"
            })
        })
        .unwrap()
        .unwrap()
        .path()
}
