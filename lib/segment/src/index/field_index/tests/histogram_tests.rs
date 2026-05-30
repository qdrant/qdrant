use std::cell::Cell;
use std::collections::BTreeSet;
use std::collections::Bound::{Excluded, Included, Unbounded};

use itertools::Itertools;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::StandardNormal;

use crate::index::field_index::histogram::{Histogram, Numericable, Point};
use crate::index::field_index::tests::histogram_test_utils::print_results;

pub fn count_range<T: PartialOrd>(points_index: &BTreeSet<Point<T>>, a: T, b: T) -> usize {
    points_index
        .iter()
        .filter(|x| a <= x.val && x.val <= b)
        .count()
}

#[test]
fn test_build_histogram_small() {
    let max_bucket_size = 10;
    let precision = 0.01;
    let num_samples = 1000;
    let mut rnd = StdRng::seed_from_u64(42);

    // let points = (0..100000).map(|i| Point { val: rnd.gen_range(-10.0..10.0), idx: i }).collect_vec();
    let points = (0..num_samples)
        .map(|i| Point {
            val: f64::round(rnd.sample::<f64, _>(StandardNormal) * 10.0),
            idx: i % num_samples / 2,
        })
        .collect_vec();

    let mut points_index: BTreeSet<Point<_>> = Default::default();

    let mut histogram = Histogram::new(max_bucket_size, precision);

    for point in &points {
        points_index.insert(point.clone());
        // print_results(&points_index, &histogram, Some(point.clone()));
        histogram.insert(
            point.clone(),
            |x| {
                points_index
                    .range((Unbounded, Excluded(x)))
                    .next_back()
                    .cloned()
            },
            |x| points_index.range((Excluded(x), Unbounded)).next().cloned(),
        );
    }

    for point in &points {
        print_results(&points_index, &histogram, Some(point.clone()));
        points_index.remove(point);
        histogram.remove(
            point,
            |x| {
                points_index
                    .range((Unbounded, Excluded(x)))
                    .next_back()
                    .cloned()
            },
            |x| points_index.range((Excluded(x), Unbounded)).next().cloned(),
        );
    }
}

pub fn test_range_by_cardinality(histogram: &Histogram<f64>) {
    let from = Unbounded;
    let range_size = 100;
    let to = histogram.get_range_by_size(from, range_size);
    let estimation = histogram.estimate(from, to);
    eprintln!("({from:?} - {to:?}) -> {estimation:?} / {range_size}");
    assert!(
        (estimation.1 as i64 - range_size as i64).abs()
            < 2 * histogram.current_bucket_size() as i64
    );

    let from = Unbounded;
    let range_size = 1000;
    let to = histogram.get_range_by_size(from, range_size);
    let estimation = histogram.estimate(from, to);
    eprintln!("({from:?} - {to:?}) -> {estimation:?} / {range_size}");
    assert!(
        (estimation.1 as i64 - range_size as i64).abs()
            < 2 * histogram.current_bucket_size() as i64
    );

    let from = Excluded(0.1);
    let range_size = 100;
    let to = histogram.get_range_by_size(from, range_size);
    let estimation = histogram.estimate(from, to);
    eprintln!("({from:?} - {to:?}) -> {estimation:?} / {range_size}");
    assert!(
        (estimation.1 as i64 - range_size as i64).abs()
            < 2 * histogram.current_bucket_size() as i64
    );

    let from = Excluded(0.1);
    let range_size = 1000;
    let to = histogram.get_range_by_size(from, range_size);
    let estimation = histogram.estimate(from, to);
    eprintln!("({from:?} - {to:?}) -> {estimation:?} / {range_size}");
    assert!(
        (estimation.1 as i64 - range_size as i64).abs()
            < 2 * histogram.current_bucket_size() as i64
    );

    let from = Excluded(0.1);
    let range_size = 100_000;
    let to = histogram.get_range_by_size(from, range_size);
    let estimation = histogram.estimate(from, to);
    eprintln!("({from:?} - {to:?}) -> {estimation:?} / {range_size}");
    assert!(matches!(to, Unbounded));
}

pub fn request_histogram(histogram: &Histogram<f64>, points_index: &BTreeSet<Point<f64>>) {
    let (est_min, estimation, est_max) = histogram.estimate(Included(0.0), Included(0.0));
    let real = count_range(points_index, 0., 0.);

    eprintln!(
        "{real} / ({est_min}, {estimation}, {est_max}) = {}",
        estimation as f64 / real as f64,
    );
    assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

    let (est_min, estimation, est_max) = histogram.estimate(Included(0.0), Included(0.0001));
    let real = count_range(points_index, 0., 0.0001);

    eprintln!(
        "{real} / ({est_min}, {estimation}, {est_max}) = {}",
        estimation as f64 / real as f64,
    );
    assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

    let (est_min, estimation, est_max) = histogram.estimate(Included(0.0), Included(0.01));
    let real = count_range(points_index, 0., 0.01);

    eprintln!(
        "{real} / ({est_min}, {estimation}, {est_max}) = {}",
        estimation as f64 / real as f64,
    );
    assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

    let (est_min, estimation, est_max) = histogram.estimate(Included(0.), Included(1.));
    let real = count_range(points_index, 0., 1.);

    eprintln!(
        "{real} / ({est_min}, {estimation}, {est_max}) = {}",
        estimation as f64 / real as f64,
    );
    assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

    let (est_min, estimation, est_max) = histogram.estimate(Included(0.), Included(100.));
    let real = count_range(points_index, 0., 100.);

    eprintln!(
        "{real} / ({est_min}, {estimation}, {est_max}) = {}",
        estimation as f64 / real as f64,
    );
    assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

    let (est_min, estimation, est_max) = histogram.estimate(Included(-100.), Included(100.));
    let real = count_range(points_index, -100., 100.);

    eprintln!(
        "{real} / ({est_min}, {estimation}, {est_max}) = {}",
        estimation as f64 / real as f64,
    );
    assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());

    let (est_min, estimation, est_max) = histogram.estimate(Included(20.), Included(100.));
    let real = count_range(points_index, 20., 100.);

    eprintln!(
        "{real} / ({est_min}, {estimation}, {est_max}) = {}",
        estimation as f64 / real as f64,
    );
    assert!(real.abs_diff(estimation) < 2 * histogram.current_bucket_size());
}

pub fn build_histogram<T: Numericable + PartialEq + PartialOrd + Copy + std::fmt::Debug>(
    max_bucket_size: usize,
    precision: f64,
    points: Vec<Point<T>>,
) -> (Histogram<T>, BTreeSet<Point<T>>) {
    let mut points_index: BTreeSet<Point<T>> = Default::default();
    let mut histogram = Histogram::new(max_bucket_size, precision);

    let read_counter = Cell::new(0);
    for point in points {
        points_index.insert(point.clone());
        // print_results(&points_index, &histogram, Some(point.clone()));
        histogram.insert(
            point,
            |x| {
                read_counter.set(read_counter.get() + 1);
                points_index
                    .range((Unbounded, Excluded(x)))
                    .next_back()
                    .cloned()
            },
            |x| {
                read_counter.set(read_counter.get() + 1);
                points_index.range((Excluded(x), Unbounded)).next().cloned()
            },
        );
    }
    eprintln!("read_counter.get() = {:#?}", read_counter.get());
    eprintln!("histogram.borders.len() = {:#?}", histogram.borders().len());
    for border in histogram.borders().iter().take(5) {
        eprintln!("border = {border:?}");
    }
    (histogram, points_index)
}

#[test]
fn test_build_histogram_round() {
    let max_bucket_size = 100;
    let precision = 0.01;
    let num_samples = 100_000;
    let mut rnd = StdRng::seed_from_u64(42);

    // let points = (0..100000).map(|i| Point { val: rnd.gen_range(-10.0..10.0), idx: i }).collect_vec();
    let points = (0..num_samples).map(|i| Point {
        val: f64::round(rnd.sample::<f64, _>(StandardNormal) * 100.0),
        idx: i,
    });
    let (histogram, points_index) = build_histogram(max_bucket_size, precision, points.collect());

    request_histogram(&histogram, &points_index);
}

#[test]
fn test_build_histogram() {
    let max_bucket_size = 1000;
    let precision = 0.01;
    let num_samples = 100_000;
    let mut rnd = StdRng::seed_from_u64(42);

    // let points = (0..100000).map(|i| Point { val: rnd.gen_range(-10.0..10.0), idx: i }).collect_vec();
    let points = (0..num_samples)
        .map(|i| Point {
            val: rnd.sample(StandardNormal),
            idx: i,
        })
        .collect_vec();

    let (histogram, points_index) = build_histogram(max_bucket_size, precision, points);
    request_histogram(&histogram, &points_index);
    test_range_by_cardinality(&histogram);
}
