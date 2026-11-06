use std::io::Write as _;

use common::universal_io::{MmapFile, MmapFs};
use tempfile::Builder;

use super::log_format::{HEADER_LEN, decode_frames, encode_frame, encode_header, has_header};
use super::{DeleteLogEntry, DeleteLogReader, DeleteLogWriter, delete_log_path};

fn entry(slot: u32, version: u64) -> DeleteLogEntry {
    DeleteLogEntry { slot, version }
}

fn frames(frames: &[&[DeleteLogEntry]]) -> Vec<u8> {
    let mut buffer = Vec::new();
    for frame in frames {
        encode_frame(frame, &mut buffer);
    }
    buffer
}

/// Every entry of a log file, checking that it is whole: a header, then complete frames.
fn read_log(segment: &std::path::Path) -> Vec<DeleteLogEntry> {
    let bytes = fs_err::read(delete_log_path(segment)).unwrap();
    assert!(has_header(&bytes).unwrap());
    let (entries, consumed) = decode_frames(&bytes[HEADER_LEN..]);
    assert_eq!(HEADER_LEN + consumed, bytes.len());
    entries
}

/// A frame is its 8 byte header and 12 bytes per entry.
#[test]
fn frame_round_trip() {
    let first = [entry(0, 0), entry(u32::MAX, u64::MAX)];
    let second = [entry(17, 42)];
    let bytes = frames(&[&first, &second]);
    assert_eq!(bytes.len(), (8 + 2 * 12) + (8 + 12));

    let (decoded, consumed) = decode_frames(&bytes);
    assert_eq!(decoded, [first.as_slice(), &second].concat());
    assert_eq!(consumed, bytes.len());

    // No entries, no frame
    assert!(frames(&[&[]]).is_empty());
}

/// Decoding stops at the first frame that is incomplete, zeroed, or fails its checksum, and
/// reports only the complete frames before it.
#[test]
fn decoding_stops_at_a_torn_tail() {
    let first = [entry(1, 5)];
    let second = [entry(3, 6), entry(4, 6)];
    let first_len = frames(&[&first]).len();
    let bytes = frames(&[&first, &second]);

    // Every cut through the second frame leaves the first one only
    for cut in first_len..bytes.len() {
        let (decoded, consumed) = decode_frames(&bytes[..cut]);
        assert_eq!(decoded, first, "cut at {cut}");
        assert_eq!(consumed, first_len);
    }

    // A zero-filled tail, as a crash can leave on a local file system
    let mut zeroed = bytes[..first_len].to_vec();
    zeroed.extend_from_slice(&[0; 64]);
    assert_eq!(decode_frames(&zeroed), (first.to_vec(), first_len));

    // A flipped payload byte fails the checksum
    let mut corrupt = bytes.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    assert_eq!(decode_frames(&corrupt), (first.to_vec(), first_len));

    // A length that cannot frame whole entries is invalid, whatever follows
    let mut odd = frames(&[&first]);
    odd[0] += 1;
    odd.push(0);
    assert_eq!(decode_frames(&odd), (Vec::new(), 0));
}

/// An incomplete header is a log not started yet, a complete one of another file or format is an
/// error.
#[test]
fn header_checks() {
    let mut header = Vec::new();
    encode_header(&mut header);
    assert_eq!(header.len(), HEADER_LEN);
    assert!(has_header(&header).unwrap());
    for cut in 0..HEADER_LEN {
        assert!(!has_header(&header[..cut]).unwrap(), "cut at {cut}");
    }

    let mut other_file = header.clone();
    other_file[0] ^= 1;
    assert!(has_header(&other_file).is_err());

    let mut other_format = header.clone();
    other_format[HEADER_LEN - 4] = 2;
    assert!(has_header(&other_format).is_err());
}

/// The first append writes the header, later appends and later writers append frames. Nothing to
/// write writes nothing, not even a header.
#[test]
fn writer_appends_frames() {
    let dir = Builder::new().prefix("delete_log").tempdir().unwrap();

    let mut writer = DeleteLogWriter::new(dir.path());
    writer.append(&MmapFs, &[]).unwrap();
    assert!(!delete_log_path(dir.path()).exists());

    writer.append(&MmapFs, &[entry(4, 10)]).unwrap();
    writer
        .append(&MmapFs, &[entry(5, 11), entry(7, 11)])
        .unwrap();

    // A fresh writer finds the end itself
    let mut writer = DeleteLogWriter::new(dir.path());
    writer.append(&MmapFs, &[entry(6, 12)]).unwrap();
    writer.append(&MmapFs, &[]).unwrap();

    assert_eq!(
        read_log(dir.path()),
        [entry(4, 10), entry(5, 11), entry(7, 11), entry(6, 12)],
    );
}

/// A fresh writer over a log with a torn tail cuts it off before appending, so readers, which stop
/// at the torn tail, see the new frame. A torn header starts the log over.
#[test]
fn writer_cuts_off_a_torn_tail() {
    let dir = Builder::new().prefix("delete_log").tempdir().unwrap();
    let path = delete_log_path(dir.path());

    let mut writer = DeleteLogWriter::new(dir.path());
    writer.append(&MmapFs, &[entry(1, 3)]).unwrap();
    let mut file = fs_err::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&frames(&[&[entry(2, 4)]])[..10]).unwrap();
    drop(file);

    let mut writer = DeleteLogWriter::new(dir.path());
    writer.append(&MmapFs, &[entry(3, 5)]).unwrap();
    assert_eq!(read_log(dir.path()), [entry(1, 3), entry(3, 5)]);

    // Only part of the header made it
    let mut header = Vec::new();
    encode_header(&mut header);
    fs_err::write(&path, &header[..5]).unwrap();
    let mut writer = DeleteLogWriter::new(dir.path());
    writer.append(&MmapFs, &[entry(6, 7)]).unwrap();
    assert_eq!(read_log(dir.path()), [entry(6, 7)]);
}

/// A reader consumes each frame once, tolerates a missing file and an incomplete header, and
/// leaves a frame in flight for a later read.
#[test]
fn reader_reads_frames_once() {
    let dir = Builder::new().prefix("delete_log").tempdir().unwrap();
    let path = delete_log_path(dir.path());

    let mut reader = DeleteLogReader::<MmapFile>::new(dir.path());
    assert_eq!(reader.read_new(&MmapFs).unwrap(), []);
    assert!(!reader.is_started());

    // A header in flight
    let mut header = Vec::new();
    encode_header(&mut header);
    let mut file = fs_err::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(&header[..7]).unwrap();
    file.flush().unwrap();
    assert_eq!(reader.read_new(&MmapFs).unwrap(), []);
    assert!(!reader.is_started());
    file.write_all(&header[7..]).unwrap();
    file.write_all(&frames(&[&[entry(1, 3)]])).unwrap();
    file.flush().unwrap();
    assert_eq!(reader.read_new(&MmapFs).unwrap(), [entry(1, 3)]);
    assert!(reader.is_started());
    assert_eq!(reader.read_new(&MmapFs).unwrap(), []);

    // Half a frame is left for later
    let frame = frames(&[&[entry(2, 4)]]);
    file.write_all(&frame[..9]).unwrap();
    file.flush().unwrap();
    assert_eq!(reader.read_new(&MmapFs).unwrap(), []);
    file.write_all(&frame[9..]).unwrap();
    file.flush().unwrap();
    assert_eq!(reader.read_new(&MmapFs).unwrap(), [entry(2, 4)]);
}
