use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process;

use docopt::Docopt;
use serde::Deserialize;
use wal::Wal;

static USAGE: &str = "
Commands:

  check   Checks that the write ahead log is well-formed, and prints the number
          of segments and entries to standard out.

  entry   Reads an entry from the write ahead log, and prints it to standard
          out.

  append  Appends an entry from stdin to the write ahead log, and prints the
          index.

Usage:
  wal-ctl check [--path=<path>]
  wal-ctl entry <index> [--path=<path>]
  wal-ctl append [--path=<path>]
  wal-ctl (-h | --help)

Options:
  --path=<path>     Path to the write ahead log directory, [default: .].
  -h --help         Show this help message.
";

#[derive(Debug, Deserialize)]
struct Args {
    cmd_check: bool,
    cmd_entry: bool,
    cmd_append: bool,

    arg_index: u64,
    flag_path: String,
}

fn main() {
    env_logger::init();
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let path: PathBuf = PathBuf::from(&args.flag_path)
        .canonicalize()
        .unwrap_or_else(|error| {
            eprintln!(
                "Unable to open write ahead log in directory {:?}: {}.",
                &args.flag_path, error
            );
            process::exit(1);
        });

    if !path.is_dir() {
        eprintln!("Unable to open write ahead log: path {path:?} is not a directory.");
        process::exit(1);
    }

    let wal = open_wal(&path);

    if args.cmd_check {
        check(wal);
    } else if args.cmd_entry {
        entry(wal, args.arg_index);
    } else if args.cmd_append {
        let mut buffer = Vec::new();
        io::stdin().read_to_end(&mut buffer).unwrap();

        append(wal, &buffer[..]);
    }
}

fn open_wal(path: &Path) -> Wal {
    Wal::open(path).unwrap_or_else(|error| {
        eprintln!("Unable to open write ahead log in directory {path:?}: {error}.");
        process::exit(1);
    })
}

fn check(wal: Wal) {
    println!("segment count: {}", wal.num_segments());
    println!("entry count: {}", wal.num_entries());
}

fn entry(wal: Wal, index: u64) {
    match wal.entry(index) {
        Some(entry) => {
            io::stdout().write_all(&entry).unwrap();
        }
        None => {
            eprintln!(
                "No entry at index {} in the write ahead log in directory {:?}.",
                index,
                &wal.path()
            );
            process::exit(1);
        }
    }
}

fn append(mut wal: Wal, entry: &[u8]) {
    println!("{}", wal.append(&entry).unwrap());
}
