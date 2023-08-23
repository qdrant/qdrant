use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

use clap::{arg, Parser};
use segment::entry::entry_point::OperationResult;
use segment::index::VectorIndexEnum;
use segment::segment_constructor::load_segment;
use segment::types::{ExtendedPointId, PointIdType, ScoredPoint};
use segment::vector_storage::ScoredPointOffset;
use segment::{self};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, value_delimiter = ',')]
    positives: Vec<u64>,

    #[arg(short, long, value_delimiter = ',')]
    negatives: Vec<u64>,

    #[arg(long)]
    full_scan: bool,

    #[arg(long)]
    top: Option<usize>,
}

fn main() {
    let args = Args::parse();

    let positives: Vec<_> = args
        .positives
        .into_iter()
        .map(ExtendedPointId::NumId)
        .collect();
    let negatives: Vec<_> = args
        .negatives
        .into_iter()
        .map(ExtendedPointId::NumId)
        .collect();
    let full_scan = args.full_scan;
    let top = args.top.unwrap_or(10);

    let result = custom_search(&positives, &negatives, full_scan, top);

    match result {
        Ok(result) => {
            serde_json::to_writer_pretty(std::io::stdout(), &result).unwrap();
            // println!("Result: {:?}", result);
        }
        Err(err) => {
            println!("Error: {:?}", err);
        }
    }
}

fn custom_search(
    positives: &[PointIdType],
    negatives: &[PointIdType],
    full_scan: bool,
    top: usize,
) -> OperationResult<Vec<ScoredPoint>> {
    if full_scan {
        custom_full_scan_search(positives, negatives, top)
    } else {
        custom_hnsw_search(positives, negatives, top)
    }
}

fn custom_hnsw_search(
    positives: &[PointIdType],
    negatives: &[PointIdType],
    top: usize,
) -> OperationResult<Vec<ScoredPoint>> {
    let segment_path = get_segment_path("hnsw", "food");

    let segment = load_segment(&segment_path)
        .expect("Should not fail loading")
        .expect("Should have some segment");

    let is_stopped = AtomicBool::new(false);

    let index = segment.vector_data[""].vector_index.borrow();

    let hnsw = match index.deref() {
        VectorIndexEnum::HnswRam(hnsw) => hnsw,
        _ => unimplemented!("Only RAM HNSW is supported for now"),
    };

    let offsets = hnsw.custom_search_with_graph(positives, negatives, top, &is_stopped);

    let results = segment.process_search_result(&offsets, &true.into(), &false.into())?;

    Ok(results)
}

fn custom_full_scan_search(
    positives: &[PointIdType],
    negatives: &[PointIdType],
    top: usize,
) -> OperationResult<Vec<ScoredPoint>> {
    let segment_path = get_segment_path("plain", "food-plain");

    let segment = load_segment(&segment_path)
        .expect("Should not fail loading")
        .expect("Should have some segment");

    let is_stopped = AtomicBool::new(false);

    let index = segment.vector_data[""].vector_index.borrow();

    let plain = match index.deref() {
        VectorIndexEnum::Plain(plain) => plain,
        _ => unimplemented!("Only plain index is supported for full scan"),
    };

    let offsets = plain.custom_search(positives, negatives, top, &is_stopped);

    let results = segment.process_search_result(&offsets, &true.into(), &false.into())?;

    Ok(results)
}

#[allow(dead_code)]
/// Only for making sure single segment search works here
fn search(query: &[f32]) -> OperationResult<Vec<ScoredPointOffset>> {
    let segment_path = dbg!(get_segment_path("hnsw", "food"));

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

fn get_segment_path(index_type: &str, collection_name: &str) -> PathBuf {
    let segments_path = format!("../../storage/collections/{collection_name}/0/segments/");
    let segments_path = Path::new(&segments_path);
    segments_path
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
                let config_index_type = segment_config["config"]["vector_data"][""]["index"]
                    ["type"]
                    .as_str()
                    .unwrap();

                config_index_type == index_type
                    && dir_entry
                        .path()
                        .join("vector_storage/vectors/chunk_0.mmap")
                        .exists()
            })
        })
        .unwrap()
        .unwrap()
        .path()
}
