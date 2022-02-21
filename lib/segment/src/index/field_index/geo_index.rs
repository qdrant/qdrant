use geo::Coordinate;
use geohash::encode;
use crate::types::{GeoPoint, GeoRadius};

type GeoHash = String;

/// Max size of geo-hash used for indexing. size=12 is about 6cm2
const GEOHASH_MAX_LENGTH: usize = 12;

/// Size of geo-hash grid depending of the length of hash.
/// Format:
///   For hash length: N = 4
///   let (lat_length_in_meters, long_length_in_meters) = HASH_GRID_SIZE[N - 1];
/// Source: <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-geohashgrid-aggregation.html#_cell_dimensions_at_the_equator>
const HASH_GRID_SIZE: &'static [(f64, f64)] = &[
    (5009.4 * 1000.0, 4992.6 * 1000.0),
    (1252.3 * 1000.0, 624.1 * 1000.0),
    (156.5 * 1000.0, 156.0 * 1000.0),
    (39.1 * 1000.0, 19.5 * 1000.0),
    (4.9 * 1000.0, 4.9 * 1000.0),
    (1.2 * 1000.0, 609.4),
    (152.9, 152.4),
    (38.2, 19.),
    (4.8, 4.8),
    (1.2, 0.595),
    (0.149, 0.149),
    (0.037, 0.019),
];

impl From<GeoPoint> for Coordinate<f64> {
    fn from(point: GeoPoint) -> Self {
        Self {
            x: point.lat,
            y: point.lon
        }
    }
}

/// Return as-small-as-possible with maximum of `max_regions`
/// number of geo-hash guaranteed to contain whole circle.
fn circle_hashes(circle: GeoRadius, max_regions: usize) -> Vec<GeoHash> {
    // Unwrap is acceptable, as data should be validated before
    let center_full_hash = encode(circle.center.into(), GEOHASH_MAX_LENGTH).unwrap();

    vec![center_full_hash]
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circle_hashes() {
        let near_berlin_circle = GeoRadius {
            center: GeoPoint {
                lat: 52.511,
                lon: 13.423637,
            },
            radius: 1000.0,
        };

        let berlin_hashes = circle_hashes(near_berlin_circle, 8);

        eprintln!("berlin_hashes = {:#?}", berlin_hashes);
    }
}