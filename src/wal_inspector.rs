use std::env;
use std::path::Path;

use collection::operations::OperationWithClockTag;
use shard::wal::SerdeWal;
use storage::content_manager::consensus::consensus_wal::ConsensusOpWal;
use storage::content_manager::consensus_ops::ConsensusOperations;
use wal::WalOptions;

/// Executable to inspect the content of a write ahead log folder (collection OR consensus WAL).
/// e.g:
/// `cargo run --bin wal_inspector storage/collections/test-collection/0/wal/ collection`
/// `cargo run --bin wal_inspector -- storage/node4/wal/ consensus` (expects `collections_meta_wal` folder as first child)
fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <wal_path> <wal_type>", args[0]);
        eprintln!("  wal_type: 'collection' or 'consensus'");
        eprintln!("Examples:");
        eprintln!(
            "  {} storage/collections/test-collection/0/wal/ collection",
            args[0]
        );
        eprintln!("  {} storage/node4/wal/ consensus", args[0]);
        std::process::exit(1);
    }
    let wal_path = Path::new(&args[1]);
    let wal_type = args[2].as_str();
    match wal_type {
        "collection" => print_collection_wal(wal_path),
        "consensus" => print_consensus_wal(wal_path),
        _ => {
            eprintln!("Unknown wal type: {wal_type}");
            eprintln!("  wal_type must be 'collection' or 'consensus'");
            std::process::exit(1);
        }
    }
}

fn print_consensus_wal(wal_path: &Path) {
    // must live within a folder named `collections_meta_wal`
    let wal = ConsensusOpWal::new(wal_path);
    println!("==========================");
    let first_index = match wal.first_entry() {
        Ok(idx) => idx,
        Err(err) => {
            eprintln!("Error reading first entry: {err}");
            return;
        }
    };
    println!("First entry: {first_index:?}");
    let last_index = match wal.last_entry() {
        Ok(idx) => idx,
        Err(err) => {
            eprintln!("Error reading last entry: {err}");
            return;
        }
    };
    println!("Last entry: {last_index:?}");
    let offset = match wal.index_offset() {
        Ok(offset) => offset,
        Err(err) => {
            eprintln!("Error reading index offset: {err}");
            return;
        }
    };
    println!("Offset of first entry: {:?}", offset.wal_to_raft_offset);
    let entries = wal.entries(
        first_index.map(|f| f.index).unwrap_or(1),
        last_index.map(|f| f.index).unwrap_or(0) + 1,
        None,
    );
    let entries = match entries {
        Ok(entries) => entries,
        Err(err) => {
            eprintln!("Error reading entries: {err}");
            return;
        }
    };
    for entry in entries {
        println!("==========================");
        let command = ConsensusOperations::try_from(&entry);
        let data = match command {
            Ok(command) => format!("{command:?}"),
            Err(_) => format!("{:?}", entry.data),
        };
        println!(
            "Entry ID:{}\nterm:{}\nentry_type:{}\ndata:{:?}",
            entry.index, entry.term, entry.entry_type, data
        )
    }
}

fn print_collection_wal(wal_path: &Path) {
    let wal_result =
        SerdeWal::<OperationWithClockTag>::open_read_only(wal_path, WalOptions::default());

    match wal_result {
        Err(error) => {
            eprintln!("Unable to open write ahead log in directory {wal_path:?}: {error}.");
        }
        Ok(Some(wal)) => {
            // print all entries
            let mut count = 0;
            for (idx, op) in wal.read_all(true) {
                println!("==========================");
                println!(
                    "Entry: {idx} Operation: {:?} Clock: {:?}",
                    op.operation, op.clock_tag
                );
                count += 1;
            }
            println!("==========================");
            println!("End of WAL.");
            println!("Found {count} entries.");
        }
        Ok(None) => {
            eprintln!("WAL directory is empty: {wal_path:?}");
        }
    }
}
