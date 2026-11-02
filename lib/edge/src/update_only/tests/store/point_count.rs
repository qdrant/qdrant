use super::*;

/// The count is read off the segments rather than summed from batch outcomes, so a replayed batch,
/// which stores nothing, leaves it where the first apply put it.
#[test]
fn available_point_count_survives_replays_and_upserts() {
    let dir = leader_with_ten_points("edge-update-point-count");
    recreate_payload_storages_append_only(dir.path());

    let writer = UpdateOnlyEdgeShard::<MmapFs>::open_mmap(dir.path()).unwrap();
    assert_eq!(writer.available_point_count(), 10);

    let batch = || store_batch(100, vec![point(11)]);
    let (_writer, outcome) = writer.apply_batch(batch()).unwrap();
    assert_eq!(outcome.stored, 1);

    let writer = UpdateOnlyEdgeShard::<MmapFs>::open_mmap(dir.path()).unwrap();
    assert_eq!(writer.available_point_count(), 11);
    let (writer, replayed) = writer.apply_batch(batch()).unwrap();
    assert_eq!(replayed.skipped, 1);
    assert_eq!(writer.available_point_count(), 11);

    let (writer, upserted) = writer
        .apply_batch(store_batch(101, vec![point(5)]))
        .unwrap();
    assert_eq!(upserted.stored, 1);
    assert_eq!(writer.available_point_count(), 11);

    let (writer, deleted) = writer
        .apply_batch([(
            102,
            PointOperation(DeletePoints {
                ids: vec![ExtendedPointId::NumId(3)],
            }),
        )])
        .unwrap();
    assert_eq!(deleted.deleted, 1);
    assert_eq!(writer.available_point_count(), 10);

    assert_eq!(exact_count(&open_follower(dir.path())), 10);
}
