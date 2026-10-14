use common::ambient;

use super::*;

fn elements(map: &ElementMap, points: Range<PointOffsetType>) -> Vec<Vec<u32>> {
    points
        .map(|point| map.point_elements(point).collect())
        .collect()
}

fn owners(map: &ElementMap, elements: Range<ElementOffsetType>) -> Vec<Option<u32>> {
    elements.map(|element| map.element_point(element)).collect()
}

/// Size of the offset space of a map, owned offsets or not.
fn offsets_upper_bound(map: &ElementMap) -> usize {
    map.deleted_elements(BitSlice::empty()).len()
}

#[test]
fn mutable_assign_keeps_and_reuses_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("map");
    let _scope = ambient::test_guard();

    let mut builder = ElementMapBuilder::default();
    assert_eq!(builder.add(0, 2).unwrap(), 0..2);
    assert_eq!(builder.add(1, 3).unwrap(), 2..5);
    let mut map =
        ElementMap::create(path.clone(), IndexMutability::Mutable, &builder, 0, true).unwrap();
    assert_eq!(map.assign(2, 0).unwrap().new, Vec::<u32>::new());

    // Same count keeps the elements
    assert_eq!(
        map.assign(0, 2).unwrap(),
        ElementUpdate {
            old: vec![0, 1],
            new: vec![0, 1],
        },
    );
    // Shrinking keeps the first elements and frees the rest
    let update = map.assign(1, 1).unwrap();
    assert_eq!(update.new, vec![2]);
    assert_eq!(update.dropped(), &[3, 4]);
    assert_eq!(map.element_point(3), None);
    // Growing takes freed offsets, lowest first, then new ones
    assert_eq!(map.assign(0, 3).unwrap().new, vec![0, 1, 3]);
    assert_eq!(map.assign(2, 2).unwrap().new, vec![4, 5]);
    assert_eq!(map.element_point(3), Some(0));

    // Repeated updates reuse the freed offsets instead of growing the offset space
    for _ in 0..100 {
        map.assign(0, 0).unwrap();
        map.assign(1, 4).unwrap();
        map.assign(1, 1).unwrap();
        map.assign(0, 3).unwrap();
    }
    assert_eq!(offsets_upper_bound(&map), 6);

    assert_eq!(map.elements_count(), 6);
    let expected = elements(&map, 0..3);
    assert_eq!(
        expected.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![3, 1, 2]
    );

    let deleted = map.deleted_elements(&BitVec::from_iter([false, true]));
    let owned_by_deleted: Vec<_> = (0..6).filter(|&element| deleted[element]).collect();
    assert_eq!(owned_by_deleted, vec![expected[1][0] as usize]);

    map.flusher()().unwrap();
    drop(map);

    let mut map = ElementMap::open(path, IndexMutability::Mutable)
        .unwrap()
        .unwrap();
    assert_eq!(elements(&map, 0..3), expected);
    assert_eq!(map.elements_count(), 6);
    // Freed offsets are known after reopening too
    map.assign(2, 0).unwrap();
    assert_eq!(map.assign(3, 2).unwrap().new.len(), 2);
    assert_eq!(offsets_upper_bound(&map), 6);
}

#[test]
fn immutable_map_resolves_both_ways() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("map");

    let mut builder = ElementMapBuilder::default();
    builder.add(1, 2).unwrap();
    builder.add(2, 0).unwrap();
    builder.add(3, 3).unwrap();
    builder.add(6, 1).unwrap();
    let mut map =
        ElementMap::create(path.clone(), IndexMutability::Immutable, &builder, 0, true).unwrap();

    let check = |map: &ElementMap| {
        assert_eq!(
            elements(map, 0..8),
            vec![
                vec![],
                vec![0, 1],
                vec![],
                vec![2, 3, 4],
                vec![],
                vec![],
                vec![5],
                vec![],
            ],
        );
        assert_eq!(
            owners(map, 0..7),
            vec![Some(1), Some(1), Some(3), Some(3), Some(3), Some(6), None],
        );
        assert_eq!(map.elements_count(), 6);
        assert_eq!(map.points_upper_bound(), 7);
        assert_eq!(map.max_elements_per_point(), 3);
    };
    check(&map);

    let deleted = map.deleted_elements(&BitVec::from_iter([false, false, false, true]));
    assert_eq!(
        deleted.iter().by_vals().collect::<Vec<_>>(),
        vec![false, false, true, true, true, false],
    );

    // Elements can be kept or dropped, but not changed in number
    assert_eq!(map.assign(3, 3).unwrap().new, vec![2, 3, 4]);
    assert_eq!(map.assign(3, 0).unwrap().new, Vec::<u32>::new());
    assert!(map.assign(3, 2).is_err());
    check(&map);

    drop(map);
    let map = ElementMap::open(path, IndexMutability::Immutable)
        .unwrap()
        .unwrap();
    check(&map);
}

/// Stored bit-packed: two bits per point for points with up to three elements, instead
/// of the four bytes of the start offset.
#[test]
fn immutable_map_is_stored_compact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("map");
    const POINTS: u32 = 10_000;

    let mut builder = ElementMapBuilder::default();
    for point in 0..POINTS {
        builder.add(point, (point % 4) as usize).unwrap();
    }
    let map =
        ElementMap::create(path.clone(), IndexMutability::Immutable, &builder, 0, true).unwrap();
    let stored: u64 = map
        .files()
        .iter()
        .map(|file| fs_err::metadata(file).unwrap().len())
        .sum();
    // The 32-byte header, then per block of 128 starts a width byte and 2 bits per start
    let blocks = u64::from(POINTS + 1).div_ceil(128);
    assert_eq!(stored, 32 + blocks * (1 + 128 * 2 / 8));

    let reopened = ElementMap::open(path, IndexMutability::Immutable)
        .unwrap()
        .unwrap();
    assert_eq!(
        elements(&reopened, 0..POINTS + 1),
        elements(&map, 0..POINTS + 1)
    );
    for element in [0, 7_777, 14_999, 15_000] {
        assert_eq!(reopened.element_point(element), map.element_point(element));
    }
}

/// An immutable map keeps the indexed points of its build, minus deleted points with
/// elements. A mutable map doesn't keep them.
#[test]
fn immutable_map_keeps_indexed_points() {
    let dir = tempfile::tempdir().unwrap();
    let _scope = ambient::test_guard();
    let mut builder = ElementMapBuilder::default();
    builder.add(0, 2).unwrap();
    builder.add(2, 1).unwrap();
    builder.add(3, 3).unwrap();

    let path = dir.path().join("immutable");
    let map =
        ElementMap::create(path.clone(), IndexMutability::Immutable, &builder, 3, true).unwrap();
    drop(map);
    let map = ElementMap::open(path, IndexMutability::Immutable)
        .unwrap()
        .unwrap();
    assert_eq!(map.indexed_points(BitSlice::empty()), Some(3));
    // Point 1 has no elements, points 2 and 3 have
    let deleted = BitVec::from_iter([false, true, true, true]);
    assert_eq!(map.indexed_points(&deleted), Some(1));

    let mutable = ElementMap::create(
        dir.path().join("mutable"),
        IndexMutability::Mutable,
        &builder,
        3,
        true,
    )
    .unwrap();
    assert_eq!(mutable.indexed_points(BitSlice::empty()), None);
}

#[test]
fn immutable_map_requires_point_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut builder = ElementMapBuilder::default();
    builder.add(3, 1).unwrap();
    builder.add(1, 1).unwrap();
    assert!(
        ElementMap::create(
            dir.path().join("map"),
            IndexMutability::Immutable,
            &builder,
            0,
            true
        )
        .is_err()
    );
}

/// Every element of a point resolves to that point, no element is listed by two points,
/// and an offset resolves only to a point that lists it.
fn assert_ownership(map: &ElementMap, points: PointOffsetType) {
    let mut owner = std::collections::HashMap::new();
    for point in 0..points {
        for element in map.point_elements(point) {
            assert_eq!(
                map.element_point(element),
                Some(point),
                "element {element} of point {point}",
            );
            if let Some(other) = owner.insert(element, point) {
                panic!("element {element} is listed by points {other} and {point}");
            }
        }
    }
    for element in 0..offsets_upper_bound(map) as ElementOffsetType {
        assert_eq!(
            map.element_point(element),
            owner.get(&element).copied(),
            "owner of element {element}",
        );
    }
}

/// Random updates never give one offset to two points, before and after reopening.
#[test]
fn mutable_assign_never_shares_offsets() {
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};

    const POINTS: PointOffsetType = 30;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("map");
    let _scope = ambient::test_guard();
    let mut rng = StdRng::seed_from_u64(17);

    let mut builder = ElementMapBuilder::default();
    for point in 0..POINTS {
        builder.add(point, rng.random_range(0..4)).unwrap();
    }
    let mut map =
        ElementMap::create(path.clone(), IndexMutability::Mutable, &builder, 0, true).unwrap();
    assert_ownership(&map, POINTS);

    for _ in 0..2000 {
        let point = rng.random_range(0..POINTS);
        let before: Vec<_> = map.point_elements(point).collect();
        let update = map.assign(point, rng.random_range(0..6)).unwrap();
        assert_eq!(update.old, before);
        // The point keeps its first elements
        let kept = update.old.len().min(update.new.len());
        assert_eq!(update.new[..kept], update.old[..kept]);
        assert_ownership(&map, POINTS);
    }

    let expected = elements(&map, 0..POINTS);
    map.flusher()().unwrap();
    drop(map);

    let mut map = ElementMap::open(path, IndexMutability::Mutable)
        .unwrap()
        .unwrap();
    assert_eq!(elements(&map, 0..POINTS), expected);
    assert_ownership(&map, POINTS);
    for _ in 0..500 {
        let point = rng.random_range(0..POINTS);
        map.assign(point, rng.random_range(0..6)).unwrap();
        assert_ownership(&map, POINTS);
    }
}
