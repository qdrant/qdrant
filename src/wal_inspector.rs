use std::env;
use std::path::Path;

use collection::operations::OperationWithClockTag;
use collection::wal::SerdeWal;
use storage::content_manager::consensus::consensus_wal::ConsensusOpWal;
use storage::content_manager::consensus_ops::ConsensusOperations;
use wal::WalOptions;

/// Executable to inspect the content of a write ahead log folder (collection OR consensus WAL).
/// e.g:
/// `cargo run --bin wal_inspector storage/collections/test-collection/0/wal/ collection`
/// `cargo run --bin wal_inspector -- storage/node4/wal/ consensus` (expects `collections_meta_wal` folder as first child)
fn main() {
    let args: Vec<String> = env::args().collect();
    let wal_path = Path::new(&args[1]);
    let wal_type = args[2].as_str();
    match wal_type {
        "collection" => print_collection_wal(wal_path),
        "consensus" => print_consensus_wal(wal_path),
        _ => eprintln!("Unknown wal type: {}", wal_type),
    }
}

fn print_consensus_wal(wal_path: &Path) {
    // must live within a folder named `collections_meta_wal`
    let wal = ConsensusOpWal::new(wal_path.to_str().unwrap());
    println!("==========================");
    let first_index = wal.first_entry().unwrap();
    println!("First entry: {:?}", first_index);
    let last_index = wal.last_entry().unwrap();
    println!("Last entry: {:?}", last_index);
    println!("Offset of first entry: {:?}", wal.index_offset().unwrap());
    let entries = wal
        .entries(
            first_index.map(|f| f.index).unwrap_or(1),
            last_index.map(|f| f.index).unwrap_or(0) + 1,
            None,
        )
        .unwrap();
    for entry in entries {
        println!("==========================");
        let command = ConsensusOperations::try_from(&entry);
        let data = match command {
            Ok(command) => format!("{:?}", command),
            Err(_) => format!("{:?}", entry.data),
        };
        println!(
            "Entry ID:{}\nterm:{}\nentry_type:{}\ndata:{:?}",
            entry.index, entry.term, entry.entry_type, data
        )
    }
}

fn print_collection_wal(wal_path: &Path) {
    let wal: Result<SerdeWal<OperationWithClockTag>, _> =
        SerdeWal::new(wal_path.to_str().unwrap(), WalOptions::default());

    match wal {
        Err(error) => {
            eprintln!("Unable to open write ahead log in directory {wal_path:?}: {error}.");
        }
        Ok(wal) => {
            // print all entries
            let mut count = 0;
            for (idx, op) in wal.read_all(false) {
                println!("==========================");
                println!("Entry: {idx}");
                println!("Operation: {:?}", op.operation);
                if let Some(clock_tag) = op.clock_tag {
                    println!("Clock: {clock_tag:?}");
                }
                count += 1;
            }
            println!("==========================");
            println!("End of WAL.");
            println!("Found {count} entries.");
        }
    }
}
