use std::io::Write as _;

use ahash::AHashMap;
use common::bitvec::BitVec;
use common::universal_io::{MmapFile, MmapFs};
use roaring::RoaringBitmap;
use tempfile::Builder;
use uuid::Uuid;

use super::format::{decode_entries, decode_entry, encode_entry};
use super::{
    Hold, MoveEntry, MoveKind, PointMoves, PointMovesView, PointMovesWriter, SlotMoves, SlotRef,
    point_moves_path, read_point_moves_tail,
};

fn segment(id: u128) -> Uuid {
    Uuid::from_u128(id)
}

fn moved_out(peer: u128, pairs: &[(u32, u32)]) -> MoveEntry {
    MoveEntry {
        kind: MoveKind::MovedOut,
        peer: segment(peer),
        pairs: pairs.to_vec(),
    }
}

fn moved_in(peer: u128, pairs: &[(u32, u32)]) -> MoveEntry {
    MoveEntry {
        kind: MoveKind::MovedIn,
        peer: segment(peer),
        pairs: pairs.to_vec(),
    }
}

fn encode(entries: &[MoveEntry]) -> Vec<u8> {
    let mut buffer = Vec::new();
    for entry in entries {
        encode_entry(entry, &mut buffer);
    }
    buffer
}

/// An entry is its header, 20 payload header bytes, and 8 bytes per pair.
#[test]
fn entry_round_trip() {
    let entries = [
        moved_out(7, &[(1033, 88), (1047, 89)]),
        moved_in(u128::MAX - 3, &[(0, u32::MAX)]),
    ];
    let bytes = encode(&entries);
    assert_eq!(bytes.len(), (8 + 20 + 16) + (8 + 20 + 8));

    let (decoded, consumed) = decode_entries(&bytes);
    assert_eq!(decoded, entries);
    assert_eq!(consumed, bytes.len());
}

/// Decoding stops at the first entry that is incomplete, zeroed, or fails its checksum, and
/// reports only the complete entries before it.
#[test]
fn decoding_stops_at_a_torn_tail() {
    let first = moved_out(1, &[(1, 2)]);
    let second = moved_out(1, &[(3, 4), (5, 6)]);
    let first_len = encode(std::slice::from_ref(&first)).len();
    let bytes = encode(&[first.clone(), second]);

    // Every cut through the second entry leaves the first one only
    for cut in first_len..bytes.len() {
        let (decoded, consumed) = decode_entries(&bytes[..cut]);
        assert_eq!(decoded, std::slice::from_ref(&first), "cut at {cut}");
        assert_eq!(consumed, first_len);
    }

    // A zero-filled tail, as a crash can leave on a local file system
    let mut zeroed = bytes[..first_len].to_vec();
    zeroed.extend_from_slice(&[0; 64]);
    assert_eq!(decode_entries(&zeroed), (vec![first.clone()], first_len));

    // A flipped payload byte fails the checksum
    let mut corrupt = bytes.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    assert_eq!(decode_entries(&corrupt), (vec![first], first_len));

    // A length that cannot frame whole pairs is invalid, whatever follows
    let mut odd = encode(&[moved_out(1, &[(1, 2)])]);
    odd[0] += 1;
    odd.push(0);
    assert!(decode_entry(&odd).is_none());
}

/// Appends accumulate in order, across writer instances that each find the end themselves.
#[test]
fn writer_appends_across_instances() {
    let dir = Builder::new().prefix("point_moves").tempdir().unwrap();

    let mut writer = PointMovesWriter::new(dir.path());
    writer
        .append(&MmapFs, &[moved_out(2, &[(1, 10)]), moved_out(3, &[])])
        .unwrap();
    writer.append(&MmapFs, &[moved_out(2, &[(2, 11)])]).unwrap();

    let mut writer = PointMovesWriter::new(dir.path());
    writer
        .append(
            &MmapFs,
            &[moved_in(4, &[(7, 70)]), moved_out(2, &[(3, 12)])],
        )
        .unwrap();

    let bytes = fs_err::read(point_moves_path(dir.path())).unwrap();
    let (entries, consumed) = decode_entries(&bytes);
    assert_eq!(consumed, bytes.len());
    assert_eq!(
        entries,
        [
            moved_out(2, &[(1, 10)]),
            moved_out(2, &[(2, 11)]),
            moved_in(4, &[(7, 70)]),
            moved_out(2, &[(3, 12)]),
        ],
    );
}

/// Nothing to append creates no file.
#[test]
fn writer_skips_empty_entries() {
    let dir = Builder::new().prefix("point_moves").tempdir().unwrap();

    let mut writer = PointMovesWriter::new(dir.path());
    writer.append(&MmapFs, &[]).unwrap();
    writer.append(&MmapFs, &[moved_out(2, &[])]).unwrap();

    assert!(!point_moves_path(dir.path()).exists());
}

/// On a local file a crash can tear the last append. A fresh writer cuts the torn tail off before
/// it appends, so readers do not stop in front of the new entry.
#[test]
fn writer_cuts_a_torn_tail() {
    let dir = Builder::new().prefix("point_moves").tempdir().unwrap();
    let path = point_moves_path(dir.path());

    let mut writer = PointMovesWriter::new(dir.path());
    writer.append(&MmapFs, &[moved_out(2, &[(1, 10)])]).unwrap();

    let torn = encode(&[moved_out(2, &[(2, 11), (3, 12)])]);
    let mut file = fs_err::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&torn[..torn.len() - 3]).unwrap();
    drop(file);

    let mut writer = PointMovesWriter::new(dir.path());
    writer.append(&MmapFs, &[moved_out(2, &[(4, 13)])]).unwrap();

    let bytes = fs_err::read(&path).unwrap();
    let (entries, consumed) = decode_entries(&bytes);
    assert_eq!(consumed, bytes.len());
    assert_eq!(
        entries,
        [moved_out(2, &[(1, 10)]), moved_out(2, &[(4, 13)])],
    );
}

/// A reader consumes new entries per read, leaves an incomplete entry for a later read, and picks
/// up a file that appears after it was opened.
#[test]
fn view_reads_incrementally() {
    let dir = Builder::new().prefix("point_moves").tempdir().unwrap();
    let path = point_moves_path(dir.path());

    let mut view = PointMovesView::<MmapFile>::new(dir.path());
    assert_eq!(view.read_new(&MmapFs).unwrap(), []);

    let mut writer = PointMovesWriter::new(dir.path());
    writer.append(&MmapFs, &[moved_out(2, &[(1, 10)])]).unwrap();
    assert_eq!(view.read_new(&MmapFs).unwrap(), [moved_out(2, &[(1, 10)])]);
    assert_eq!(view.read_new(&MmapFs).unwrap(), []);

    // An entry still being written is not consumed
    let next = encode(&[moved_in(3, &[(5, 6)])]);
    let mut file = fs_err::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&next[..next.len() / 2]).unwrap();
    drop(file);
    assert_eq!(view.read_new(&MmapFs).unwrap(), []);

    let mut file = fs_err::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&next[next.len() / 2..]).unwrap();
    drop(file);
    assert_eq!(view.read_new(&MmapFs).unwrap(), [moved_in(3, &[(5, 6)])]);
    assert_eq!(view.read_to(), fs_err::metadata(&path).unwrap().len());
}

/// The tail read returns what follows the given offset, and nothing for a missing file.
#[test]
fn tail_read_follows_the_offset() {
    let dir = Builder::new().prefix("point_moves").tempdir().unwrap();
    let path = point_moves_path(dir.path());

    assert!(
        read_point_moves_tail::<MmapFile>(&MmapFs, &path, 0)
            .unwrap()
            .is_empty()
    );

    let mut writer = PointMovesWriter::new(dir.path());
    writer.append(&MmapFs, &[moved_out(2, &[(1, 10)])]).unwrap();
    let mut view = PointMovesView::<MmapFile>::new(dir.path());
    assert_eq!(view.read_new(&MmapFs).unwrap().len(), 1);

    writer.append(&MmapFs, &[moved_out(2, &[(2, 11)])]).unwrap();
    let tail = read_point_moves_tail::<MmapFile>(&MmapFs, &path, view.read_to()).unwrap();
    assert_eq!(
        view.consume(view.read_to(), &tail),
        [moved_out(2, &[(2, 11)])],
    );

    // A tail read from anywhere else is ignored
    assert_eq!(view.consume(0, &tail), []);
}

/// Moved-out records name local slots and their targets; moved-in records fold into the
/// per-source index once their local slot settles.
#[test]
fn move_state_settles_and_lists_moves() {
    let mut moves = PointMoves::default();
    moves.ingest([
        moved_out(2, &[(1, 10), (2, 11)]),
        moved_in(3, &[(20, 5), (21, 6)]),
    ]);

    assert!(moves.names(1) && moves.names(2) && !moves.names(3));
    assert!(moves.settled_moved_in().is_empty());
    assert_eq!(moves.unsettled_slots().collect::<Vec<_>>(), [20, 21]);

    moves.settle(&RoaringBitmap::from_iter([20u32]));
    let settled = moves.settled_moved_in();
    assert_eq!(settled.len(), 1);
    assert!(settled[&segment(3)].contains(5) && !settled[&segment(3)].contains(6));
    assert_eq!(moves.unsettled_slots().collect::<Vec<_>>(), [21]);

    moves.settle_all();
    assert!(moves.settled_moved_in()[&segment(3)].contains(6));
    assert_eq!(moves.unsettled_slots().count(), 0);

    let target = |slot| SlotRef {
        segment: segment(2),
        slot,
    };
    let mut moved_out: Vec<_> = moves.moved_out().collect();
    moved_out.sort_unstable();
    assert_eq!(moved_out, [(1, target(10)), (2, target(11))]);

    moves.forget([1]);
    assert!(!moves.names(1));
    assert_eq!(moves.moved_out().collect::<Vec<_>>(), [(2, target(11))]);
}

/// Held tombstones resolve as plain deletes once classified, or as superseded once the shard names
/// their slot; live copies the shard names are superseded too, deleted ones are not.
#[test]
fn slot_moves_resolve_against_superseded_slots() {
    let dir = Builder::new().prefix("moves").tempdir().unwrap();
    let mut moves = SlotMoves::<MmapFile> {
        view: PointMovesView::new(dir.path()),
        state: PointMoves::default(),
        held: AHashMap::from_iter([(1, Hold::Plain), (2, Hold::Move), (3, Hold::Unclassified)]),
    };
    let mut deleted = BitVec::repeat(false, 6);
    deleted.set(5, true);

    let superseded = RoaringBitmap::from_iter([2u32, 4, 5]);
    let resolution = moves.resolve(Some(&superseded), &deleted);
    assert_eq!(resolution.plain, [1]);
    assert_eq!(resolution.superseded, [2, 4]);

    // Without superseded slots only plain deletes go
    let resolution = moves.resolve(None, &deleted);
    assert_eq!(resolution.plain, [1]);
    assert!(resolution.superseded.is_empty());

    // Slots past the deleted state count as deleted
    let resolution = moves.resolve(Some(&RoaringBitmap::from_iter([7u32])), &deleted);
    assert!(resolution.superseded.is_empty());

    moves.forget(&[1, 2]);
    assert_eq!(moves.held.len(), 1);
}

/// A slot two moved-out records name, after an interrupted move and a later rewrite, keeps the
/// target of the last record only.
#[test]
fn moved_out_keeps_the_last_target() {
    let mut moves = PointMoves::default();
    moves.ingest([moved_out(2, &[(1, 10)])]);
    moves.ingest([moved_out(4, &[(1, 40)])]);

    let first = SlotRef {
        segment: segment(2),
        slot: 10,
    };
    let last = SlotRef {
        segment: segment(4),
        slot: 40,
    };
    assert!(moves.names(1));
    assert_ne!(first, last);
    assert_eq!(moves.moved_out().collect::<Vec<_>>(), [(1, last)]);
}
