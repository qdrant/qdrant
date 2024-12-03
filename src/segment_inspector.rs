use std::path::Path;
use std::sync::atomic::AtomicBool;

use clap::Parser;
use segment::entry::entry_point::SegmentEntry;
use segment::segment_constructor::load_segment;
use segment::types::PointIdType;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Path to the segment folder. May be a list
    #[clap(short, long, num_args=1..)]
    path: Vec<String>,

    /// Print segment info
    #[clap(long)]
    info: bool,

    /// Point ID to inspect
    #[clap(long)]
    point_id_int: Option<u64>,

    /// Point ID to inspect (UUID)
    #[clap(long)]
    point_id_uuid: Option<String>,
}

fn main() {
    let args: Args = Args::parse();
    for segment_path in args.path {
        let path = Path::new(&segment_path);
        if !path.exists() {
            eprintln!("Path does not exist: {segment_path}");
            continue;
        }
        if !path.is_dir() {
            eprintln!("Path is not a directory: {segment_path}");
            continue;
        }

        // Open segment

        let segment = load_segment(path, &AtomicBool::new(false))
            .unwrap()
            .unwrap();

        eprintln!(
            "path = {:#?}, size-points = {}",
            path,
            segment.available_point_count()
        );

        if args.info {
            let info = segment.info();
            eprintln!("info = {info:#?}");
        }

        if let Some(point_id_int) = args.point_id_int {
            let point_id = PointIdType::NumId(point_id_int);

            let internal_id = segment.get_internal_id(point_id);
            if internal_id.is_some() {
                let version = segment.point_version(point_id);
                let payload = segment.payload(point_id).unwrap();
                // let vectors = segment.all_vectors(point_id).unwrap();

                println!("Internal ID: {internal_id:?}");
                println!("Version: {version:?}");
                println!("Payload: {payload:?}");
                // println!("Vectors: {vectors:?}");
            }
        }
    }
}
