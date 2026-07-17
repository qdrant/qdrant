//! Inspect the on-disk content of the mutable (appendable) ID tracker.
//!
//! Parses `mutable_id_tracker.mappings` (append-only change log) and
//! `mutable_id_tracker.versions` (flat array of u64 versions indexed by internal ID)
//! without going through `MutableIdTracker::open`, which truncates partial trailing
//! entries as a side effect. This tool is strictly read-only.
//!
//! Usage:
//!   cargo run -p segment --example inspect_mutable_id_tracker -- <SEGMENT_DIR> [FLAGS]
//!
//! Flags:
//!   --log                dump every mapping log entry with its byte offset
//!   --versions           dump the versions array
//!   --resolved           dump the resolved external -> internal mapping
//!   --point <ID>         trace the log history of one external ID (number or UUID)
//!
//! Default (no flags): summary and consistency report.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{BufReader, Read};
use std::path::Path;
use std::process::exit;

use fs_err::File;
use uuid::Uuid;

const FILE_MAPPINGS: &str = "mutable_id_tracker.mappings";
const FILE_VERSIONS: &str = "mutable_id_tracker.versions";
const VERSION_ELEMENT_SIZE: u64 = 8;
const DELETED_POINT_VERSION: u64 = 0;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ExternalId {
    Num(u64),
    Uuid(Uuid),
}

impl fmt::Display for ExternalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Num(num) => write!(f, "{num}"),
            Self::Uuid(uuid) => write!(f, "{uuid}"),
        }
    }
}

#[derive(Clone, Copy)]
enum LogEntry {
    Insert(ExternalId, u32),
    Delete(ExternalId),
}

impl fmt::Display for LogEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Insert(external_id, internal_id) => {
                write!(f, "insert {external_id} -> {internal_id}")
            }
            Self::Delete(external_id) => write!(f, "delete {external_id}"),
        }
    }
}

fn read_u64<R: Read>(reader: &mut R) -> std::io::Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn read_u128<R: Read>(reader: &mut R) -> std::io::Result<u128> {
    let mut buf = [0u8; 16];
    reader.read_exact(&mut buf)?;
    Ok(u128::from_le_bytes(buf))
}

fn read_u32<R: Read>(reader: &mut R) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

/// Read one log entry. Mirrors `mutable_id_tracker::change::read_entry`.
///
/// Returns `Ok(None)` on a clean end of file (no bytes available at all),
/// `Err` on a partial or malformed entry.
fn read_entry<R: Read>(reader: &mut R) -> Result<Option<(LogEntry, u64)>, String> {
    let mut type_byte = [0u8; 1];
    match reader.read_exact(&mut type_byte) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.to_string()),
    }

    let entry = match type_byte[0] {
        1 => {
            let external_id = ExternalId::Num(read_u64(reader).map_err(|e| e.to_string())?);
            let internal_id = read_u32(reader).map_err(|e| e.to_string())?;
            (LogEntry::Insert(external_id, internal_id), 1 + 8 + 4)
        }
        2 => {
            let uuid = Uuid::from_u128_le(read_u128(reader).map_err(|e| e.to_string())?);
            let internal_id = read_u32(reader).map_err(|e| e.to_string())?;
            (
                LogEntry::Insert(ExternalId::Uuid(uuid), internal_id),
                1 + 16 + 4,
            )
        }
        3 => {
            let external_id = ExternalId::Num(read_u64(reader).map_err(|e| e.to_string())?);
            (LogEntry::Delete(external_id), 1 + 8)
        }
        4 => {
            let uuid = Uuid::from_u128_le(read_u128(reader).map_err(|e| e.to_string())?);
            (LogEntry::Delete(ExternalId::Uuid(uuid)), 1 + 16)
        }
        other => return Err(format!("malformed change type byte {other:#04X}")),
    };

    Ok(Some(entry))
}

struct MappingsLog {
    /// (byte offset, entry)
    entries: Vec<(u64, LogEntry)>,
    file_len: u64,
    /// Byte offset at which parsing stopped
    parsed_to: u64,
    /// Parse error, if the log did not end cleanly at EOF
    error: Option<String>,
}

fn read_mappings_log(path: &Path) -> std::io::Result<MappingsLog> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    let mut entries = Vec::new();
    let mut offset = 0u64;
    let mut error = None;

    loop {
        match read_entry(&mut reader) {
            Ok(Some((entry, size))) => {
                entries.push((offset, entry));
                offset += size;
            }
            Ok(None) => break,
            Err(err) => {
                error = Some(err);
                break;
            }
        }
    }

    Ok(MappingsLog {
        entries,
        file_len,
        parsed_to: offset,
        error,
    })
}

fn read_versions(path: &Path) -> std::io::Result<(Vec<u64>, u64)> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let count = file_len / VERSION_ELEMENT_SIZE;
    let mut reader = BufReader::new(file);
    let versions = (0..count)
        .map(|_| read_u64(&mut reader))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((versions, file_len))
}

fn parse_external_id(raw: &str) -> Result<ExternalId, String> {
    if let Ok(num) = raw.parse::<u64>() {
        return Ok(ExternalId::Num(num));
    }
    if let Ok(uuid) = Uuid::parse_str(raw) {
        return Ok(ExternalId::Uuid(uuid));
    }
    Err(format!("`{raw}` is neither a u64 nor a UUID"))
}

fn print_summary(log: &MappingsLog, versions: Option<&(Vec<u64>, u64)>) {
    let mut insert_num = 0usize;
    let mut insert_uuid = 0usize;
    let mut delete_num = 0usize;
    let mut delete_uuid = 0usize;

    // Resolved state: last change per external ID wins
    let mut resolved: BTreeMap<ExternalId, Option<u32>> = BTreeMap::new();
    // How many times each external ID appears in the log
    let mut relinked = 0usize;

    for &(_, entry) in &log.entries {
        match entry {
            LogEntry::Insert(external_id, internal_id) => {
                match external_id {
                    ExternalId::Num(_) => insert_num += 1,
                    ExternalId::Uuid(_) => insert_uuid += 1,
                }
                if resolved.insert(external_id, Some(internal_id)).is_some() {
                    relinked += 1;
                }
            }
            LogEntry::Delete(external_id) => {
                match external_id {
                    ExternalId::Num(_) => delete_num += 1,
                    ExternalId::Uuid(_) => delete_uuid += 1,
                }
                resolved.insert(external_id, None);
            }
        }
    }

    let alive: BTreeMap<&ExternalId, u32> = resolved
        .iter()
        .filter_map(|(external_id, internal_id)| internal_id.map(|i| (external_id, i)))
        .collect();
    let dropped = resolved.len() - alive.len();

    // Internal IDs referenced by more than one alive external ID (shadowed/deferred heads)
    let mut by_internal: BTreeMap<u32, Vec<&ExternalId>> = BTreeMap::new();
    for (&external_id, &internal_id) in &alive {
        by_internal
            .entry(internal_id)
            .or_default()
            .push(external_id);
    }
    let shared_internal: Vec<_> = by_internal
        .iter()
        .filter(|(_, externals)| externals.len() > 1)
        .collect();
    let max_internal = by_internal.keys().next_back().copied();

    println!("== mappings log ==");
    println!("file length:        {} bytes", log.file_len);
    println!(
        "parsed:             {} bytes, {} entries",
        log.parsed_to,
        log.entries.len()
    );
    if log.parsed_to < log.file_len {
        println!(
            "!! trailing bytes:  {} unparsed (partial or corrupt entry)",
            log.file_len - log.parsed_to,
        );
    }
    if let Some(err) = &log.error {
        println!("!! parse stopped:   {err}");
    }
    println!(
        "entries:            insert_num={insert_num} insert_uuid={insert_uuid} delete_num={delete_num} delete_uuid={delete_uuid}",
    );
    println!();
    println!("== resolved state ==");
    println!("distinct external:  {}", resolved.len());
    println!("alive mappings:     {}", alive.len());
    println!("dropped (tombstone): {dropped}");
    println!("re-linked inserts:  {relinked}");
    if let Some(max_internal) = max_internal {
        println!("max internal id:    {max_internal}");
    }
    if !shared_internal.is_empty() {
        println!(
            "!! internal ids referenced by multiple alive external ids: {}",
            shared_internal.len(),
        );
        for (internal_id, externals) in shared_internal.iter().take(10) {
            let externals = externals.iter().map(|e| e.to_string()).collect::<Vec<_>>();
            println!("     {internal_id} <- [{}]", externals.join(", "));
        }
        if shared_internal.len() > 10 {
            println!("     ... and {} more", shared_internal.len() - 10);
        }
    }

    println!();
    println!("== versions ==");
    match versions {
        None => println!("versions file:      missing"),
        Some((versions, file_len)) => {
            println!("file length:        {file_len} bytes");
            if file_len % VERSION_ELEMENT_SIZE != 0 {
                println!(
                    "!! trailing bytes:  {} (partial entry)",
                    file_len % VERSION_ELEMENT_SIZE,
                );
            }
            println!("version entries:    {}", versions.len());
            let deleted = versions
                .iter()
                .filter(|&&v| v == DELETED_POINT_VERSION)
                .count();
            println!("deleted (version 0): {deleted}");
            if let (Some(&min), Some(&max)) = (
                versions.iter().filter(|&&v| v != 0).min(),
                versions.iter().max(),
            ) {
                println!("version range:      {min}..={max} (excluding 0)");
            }

            // Cross-check against mappings
            if let Some(max_internal) = max_internal {
                let expected = max_internal as usize + 1;
                if versions.len() < expected {
                    println!(
                        "!! versions shorter than mappings: {} entries, max alive internal id is {max_internal} (partial flush, WAL recovery expected)",
                        versions.len(),
                    );
                }
            }
            let alive_without_version = alive
                .iter()
                .filter(|&(_, &internal_id)| {
                    versions
                        .get(internal_id as usize)
                        .is_none_or(|&v| v == DELETED_POINT_VERSION)
                })
                .count();
            if alive_without_version > 0 {
                println!("!! alive mappings with missing/deleted version: {alive_without_version}",);
            }
        }
    }
}

fn print_log(log: &MappingsLog) {
    for &(offset, entry) in &log.entries {
        println!("{offset:>10}  {entry}");
    }
    if let Some(err) = &log.error {
        println!("{:>10}  !! {err}", log.parsed_to);
    } else if log.parsed_to < log.file_len {
        println!(
            "{:>10}  !! partial trailing entry ({} bytes)",
            log.parsed_to,
            log.file_len - log.parsed_to,
        );
    }
}

fn print_point(log: &MappingsLog, versions: Option<&(Vec<u64>, u64)>, target: ExternalId) {
    let mut current: Option<u32> = None;
    let mut seen = false;
    for &(offset, entry) in &log.entries {
        let external_id = match entry {
            LogEntry::Insert(external_id, _) | LogEntry::Delete(external_id) => external_id,
        };
        if external_id != target {
            continue;
        }
        seen = true;
        println!("{offset:>10}  {entry}");
        current = match entry {
            LogEntry::Insert(_, internal_id) => Some(internal_id),
            LogEntry::Delete(_) => None,
        };
    }

    if !seen {
        println!("external id {target} not found in mappings log");
        return;
    }

    match current {
        None => println!("resolved: dropped"),
        Some(internal_id) => {
            let version = versions.and_then(|(versions, _)| versions.get(internal_id as usize));
            match version {
                Some(&v) if v == DELETED_POINT_VERSION => {
                    println!("resolved: internal id {internal_id}, version 0 (deleted marker)")
                }
                Some(&v) => println!("resolved: internal id {internal_id}, version {v}"),
                None => println!("resolved: internal id {internal_id}, no version entry"),
            }
        }
    }
}

fn print_versions(versions: &[u64]) {
    for (internal_id, &version) in versions.iter().enumerate() {
        if version == DELETED_POINT_VERSION {
            println!("{internal_id:>10}  0 (deleted)");
        } else {
            println!("{internal_id:>10}  {version}");
        }
    }
}

fn print_resolved(log: &MappingsLog) {
    let mut resolved: BTreeMap<ExternalId, Option<u32>> = BTreeMap::new();
    for &(_, entry) in &log.entries {
        match entry {
            LogEntry::Insert(external_id, internal_id) => {
                resolved.insert(external_id, Some(internal_id));
            }
            LogEntry::Delete(external_id) => {
                resolved.insert(external_id, None);
            }
        }
    }
    for (external_id, internal_id) in resolved {
        match internal_id {
            Some(internal_id) => println!("{external_id} -> {internal_id}"),
            None => println!("{external_id} -> (dropped)"),
        }
    }
}

fn usage() -> ! {
    eprintln!(
        "usage: inspect_mutable_id_tracker <SEGMENT_DIR> [--log] [--versions] [--resolved] [--point <ID>]"
    );
    exit(2)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let Some(segment_dir) = args.next() else {
        usage()
    };

    let mut show_log = false;
    let mut show_versions = false;
    let mut show_resolved = false;
    let mut point: Option<ExternalId> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--log" => show_log = true,
            "--versions" => show_versions = true,
            "--resolved" => show_resolved = true,
            "--point" => {
                let Some(raw) = args.next() else { usage() };
                match parse_external_id(&raw) {
                    Ok(external_id) => point = Some(external_id),
                    Err(err) => {
                        eprintln!("invalid --point value: {err}");
                        exit(2);
                    }
                }
            }
            _ => usage(),
        }
    }

    let segment_dir = Path::new(&segment_dir);
    let mappings_path = segment_dir.join(FILE_MAPPINGS);
    let versions_path = segment_dir.join(FILE_VERSIONS);

    if !mappings_path.is_file() {
        eprintln!("no {FILE_MAPPINGS} in {}", segment_dir.display());
        exit(1);
    }

    let log = match read_mappings_log(&mappings_path) {
        Ok(log) => log,
        Err(err) => {
            eprintln!("failed to read mappings log: {err}");
            exit(1);
        }
    };

    let versions = if versions_path.is_file() {
        match read_versions(&versions_path) {
            Ok(versions) => Some(versions),
            Err(err) => {
                eprintln!("failed to read versions file: {err}");
                exit(1);
            }
        }
    } else {
        None
    };

    if let Some(target) = point {
        print_point(&log, versions.as_ref(), target);
        return;
    }
    if show_log {
        print_log(&log);
        return;
    }
    if show_versions {
        match &versions {
            Some((versions, _)) => print_versions(versions),
            None => eprintln!("versions file missing"),
        }
        return;
    }
    if show_resolved {
        print_resolved(&log);
        return;
    }

    print_summary(&log, versions.as_ref());
}
