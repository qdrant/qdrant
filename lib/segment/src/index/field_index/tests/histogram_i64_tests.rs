use std::collections::BTreeSet;
use std::collections::Bound::Included;

use rand::prelude::SliceRandom;

use crate::index::field_index::histogram::{Histogram, Point};
use crate::index::field_index::tests::histogram_test_utils::print_results;
use crate::index::field_index::tests::histogram_tests::{build_histogram, count_range};

pub fn histogram_fixture_values() -> Vec<i64> {
    vec![
        818038026092414779,
        817430997309065005,
        814525614605207302,
        814525614605207302,
        814525614605207302,
        814525614605207302,
        814525614605207302,
        814525614605207302,
        814525614605207302,
        814525614605207302,
        814525614605207302,
        814525614605207302,
        814525614605207302,
        814525614605207302,
        814525614605207302,
        813881787313817334,
        813881787313817334,
        813881787313817334,
        813881787313817334,
        813881787313817334,
        797077632540739163,
        797077632540739163,
        797077632540739163,
        797077632540739163,
        797077632540739163,
        797077632540739163,
        797077632540739163,
        796753381031937625,
        796753381031937625,
        796753381031937625,
        796753381031937625,
        796753381031937625,
        796753381031937625,
        794845070900594257,
        794845070900594257,
        794845070900594257,
        793390195280971343,
        793390195280971343,
        793390195280971343,
        793390195280971343,
        793390195280971343,
        793390195280971343,
        793390195280971343,
        793390195280971343,
        793390195280971343,
        793390195280971343,
        793390195280971343,
        793390195280971343,
        793353484098340430,
        793353484098340430,
        793353484098340430,
        793353484098340430,
        793353484098340430,
        793353484098340430,
        793353484098340430,
        793353484098340430,
        793353484098340430,
        793353484098340430,
        793353484098340430,
        793115156044318285,
        793115156044318285,
        793115156044318285,
        793115156044318285,
        793115156044318285,
        793115156044318285,
        793115156044318285,
        792916109618579020,
        792916109618579020,
        792916109618579020,
        789197549817824843,
        789197549817824843,
        789197549817824843,
        789197549817824843,
        789197549817824843,
        789197549817824843,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787850537872655946,
        787649226061383241,
        787647249260742216,
        784221654430516807,
        784221654430516807,
        784221654430516807,
        784221654430516807,
        784221654430516807,
        782529989596677701,
    ]
}

#[test]
fn test_fixture_ok() {
    for x in histogram_fixture_values() {
        assert!(x > 0);
        println!("{} {}", x, x as f64);
    }
}

#[test]
fn test_histogram() {
    let max_bucket_size = 1000;
    let precision = 0.01;

    // let points = (0..100000).map(|i| Point { val: rnd.gen_range(-10.0..10.0), idx: i }).collect_vec();
    let mut points: Vec<_> = histogram_fixture_values();
    points.shuffle(&mut rand::thread_rng());
    let points: Vec<_> = points
        .into_iter()
        .enumerate()
        .map(|(idx, val)| Point { val, idx })
        .collect();

    let (histogram, points_index) = build_histogram(max_bucket_size, precision, points);

    print_results(&points_index, &histogram, None);
}

pub fn request_histogram_i64(histogram: &Histogram<i64>, points_index: &BTreeSet<Point<i64>>) {
    let (est_min, estimation, est_max) = histogram.estimate(Included(0), Included(0));
    let real = count_range(points_index, 0, 0);

    eprintln!(
        "{} / ({}, {}, {}) = {}",
        real,
        est_min,
        estimation,
        est_max,
        estimation as f64 / real as f64
    );
    assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

    let (est_min, estimation, est_max) = histogram.estimate(Included(0), Included(100));
    let real = count_range(points_index, 0, 100);

    eprintln!(
        "{} / ({}, {}, {}) = {}",
        real,
        est_min,
        estimation,
        est_max,
        estimation as f64 / real as f64
    );
    assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

    let (est_min, estimation, est_max) = histogram.estimate(Included(-100), Included(100));
    let real = count_range(points_index, -100, 100);

    eprintln!(
        "{} / ({}, {}, {}) = {}",
        real,
        est_min,
        estimation,
        est_max,
        estimation as f64 / real as f64
    );
    assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

    for _ in 0..100 {
        let from = rand::random::<i64>();
        let to = from.saturating_add(rand::random::<i64>());

        let (est_min, estimation, est_max) = histogram.estimate(Included(from), Included(to));
        let real = count_range(points_index, from, to);

        eprintln!(
            "{} / ({}, {}, {}) = {}",
            real,
            est_min,
            estimation,
            est_max,
            estimation as f64 / real as f64
        );
        assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());
    }
}

#[test]
fn test_build_i64_histogram() {
    let max_bucket_size = 1000;
    let precision = 0.01;
    let num_samples = 100_000;

    // let points = (0..100000).map(|i| Point { val: rnd.gen_range(-10.0..10.0), idx: i }).collect_vec();
    let points: Vec<_> = (0..num_samples)
        .map(|i| Point {
            val: rand::random::<i64>(),
            idx: i,
        })
        .collect();

    let (histogram, points_index) = build_histogram(max_bucket_size, precision, points);
    request_histogram_i64(&histogram, &points_index);
    // test_range_by_cardinality(&histogram);
}
