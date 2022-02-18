use crate::types::{GeoBoundingBox, GeoPoint, GeoRadius};
use geo::algorithm::haversine_distance::HaversineDistance;
use geo::{Coordinate, Point};
use geohash::{decode, encode, neighbor, Direction};
use std::ops::Range;

type GeoHash = String;

/// Max size of geo-hash used for indexing. size=12 is about 6cm2
const GEOHASH_MAX_LENGTH: usize = 12;

/// Size of geo-hash grid depending of the length of hash.
/// Format:
///   For hash length: N = 4
///   let (lat_length_in_meters, long_length_in_meters) = HASH_GRID_SIZE[N - 1];
/// Source: <https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-geohashgrid-aggregation.html#_cell_dimensions_at_the_equator>
const HASH_GRID_SIZE: &[(f64, f64)] = &[
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
            y: point.lon,
        }
    }
}

#[derive(Debug)]
struct FullGeoBoundingBox {
    north_west: Point<f64>,
    south_west: Point<f64>,
    south_east: Point<f64>,
    north_east: Point<f64>,
}

impl From<&GeoBoundingBox> for FullGeoBoundingBox {
    fn from(bounding_box: &GeoBoundingBox) -> Self {
        let GeoPoint {
            lat: max_lat,
            lon: min_lon,
        } = bounding_box.top_left;
        let GeoPoint {
            lat: min_lat,
            lon: max_lon,
        } = bounding_box.bottom_right;

        let north_west = Point::new(min_lon, max_lat);
        let south_west = Point::new(min_lon, min_lat);
        let south_east = Point::new(max_lon, min_lat);
        let north_east = Point::new(max_lon, max_lat);

        Self {
            north_west,
            south_west,
            south_east,
            north_east,
        }
    }
}

impl FullGeoBoundingBox {
    fn shortest_side_length_in_km(&self) -> f64 {
        // the projection on the sphere is a trapeze - calculate 3 distances
        let upper_width = self.north_west.haversine_distance(&self.north_east);
        let lower_width = self.south_west.haversine_distance(&self.south_east);
        // both height sides are equal in a trapeze - pick only one
        let height = self.north_west.haversine_distance(&self.south_west);
        f64::min(f64::min(upper_width, lower_width), height)
    }
}

#[derive(Debug)]
struct RectangleGeoHash {
    north_west: GeoHash,
    south_west: GeoHash,
    south_east: GeoHash,
    north_east: GeoHash,
}

impl RectangleGeoHash {
    /// warning: this could generate a huge result depending on the size of the rectangle and the precision of the geohashes
    fn geohash_regions(&self) -> Vec<GeoHash> {
        let width = self.area_width_region_count();
        let height = self.area_height_region_count();

        let mut seen: Vec<String> = Vec::new();
        let mut top = self.north_west.clone();
        // traverse tiles matrix by columns - top to bottom
        for _ in 0..width {
            let mut current = top.clone();
            for _ in 0..height {
                seen.push(current.clone());
                current = neighbor(&current, Direction::S).unwrap();
            }
            // move top column to the east
            top = neighbor(&top, Direction::E).unwrap();
        }
        seen
    }

    fn area_width_region_count(&self) -> usize {
        let mut width_region = 0;
        // count starting tile
        if self.north_west != self.north_east {
            width_region += 1;
        }
        let mut current = self.north_west.clone();
        while current != self.north_east {
            let east_neighbor = neighbor(&current, Direction::E).unwrap();
            current = east_neighbor;
            width_region += 1;
        }
        width_region
    }

    fn area_height_region_count(&self) -> usize {
        let mut height_region = 0;
        // count starting tile
        if self.north_west != self.south_west {
            height_region += 1;
        }
        let mut current = self.north_west.clone();
        while current != self.south_west {
            let south_neighbor = neighbor(&current, Direction::S).unwrap();
            current = south_neighbor;
            height_region += 1;
        }
        height_region
    }

    /// number of geohashes within the rectangle
    fn area_region_count(&self) -> usize {
        self.area_height_region_count() * self.area_width_region_count()
    }

    fn compute_from_bounding_box(bounding_box: &GeoBoundingBox, geohash_precision: usize) -> Self {
        let GeoPoint {
            lat: max_lat,
            lon: min_lon,
        } = bounding_box.top_left;
        let GeoPoint {
            lat: min_lat,
            lon: max_lon,
        } = bounding_box.bottom_right;

        // Unwrap is acceptable, as data should be validated before
        let north_west = encode((min_lon, max_lat).into(), geohash_precision).unwrap();
        let south_west = encode((min_lon, min_lat).into(), geohash_precision).unwrap();
        let south_east = encode((max_lon, min_lat).into(), geohash_precision).unwrap();
        let north_east = encode((max_lon, max_lat).into(), geohash_precision).unwrap();

        Self {
            north_west,
            south_west,
            south_east,
            north_east,
        }
    }
}

/// Return as-high-as-possible with maximum of `max_regions`
/// number of geo-hash guaranteed to contain the whole circle.
fn circle_hashes(circle: GeoRadius, max_regions: usize) -> Vec<GeoHash> {
    let rectangle = minimum_bounding_rectangle_for_circle(&circle);
    let inner_hashes = rectangle_hashes(rectangle, max_regions);
    filter_hashes_within_circle(inner_hashes, circle)
}

/// filter geo hashes for which the center is within a circle
fn filter_hashes_within_circle(hashes: Vec<GeoHash>, circle: GeoRadius) -> Vec<GeoHash> {
    let center_point = Point::new(circle.center.lon, circle.center.lat);
    hashes
        .into_iter()
        .map(|h| {
            let coord = decode(&h).unwrap().0;
            let geo_point = Point::new(coord.x, coord.y);
            (h, geo_point)
        })
        .filter(|(_, hash_point)| center_point.haversine_distance(hash_point) <= circle.radius)
        .map(|(h, _)| h)
        .collect()
}

/// Return as-high-as-possible with maximum of `max_regions`
/// number of geo-hash guaranteed to contain the whole rectangle.
fn rectangle_hashes(rectangle: GeoBoundingBox, max_regions: usize) -> Vec<GeoHash> {
    let full_geo_bounding_box: FullGeoBoundingBox = (&rectangle).into();
    let shortest_side_km = full_geo_bounding_box.shortest_side_length_in_km();
    let possible_precisions = geohash_precisions_for_distance(shortest_side_km * 1000.0);
    // filter precision which generates less than `max_regions`.
    let rectangle_geohash = possible_precisions
        .into_iter()
        .map(|p| RectangleGeoHash::compute_from_bounding_box(&rectangle, p))
        .take_while(|rectangle_geohash| rectangle_geohash.area_region_count() <= max_regions)
        .last();
    match rectangle_geohash {
        None => Vec::new(),
        Some(r) => r.geohash_regions(),
    }
}

/// Finding the geo precisions fine enough to not completely wrap the `distance_in_meter` with a single tile.
fn geohash_precisions_for_distance(distance_in_meter: f64) -> Range<usize> {
    HASH_GRID_SIZE
        .iter()
        .enumerate()
        .find(|(_, (lat_length_in_meters, _))| distance_in_meter > *lat_length_in_meters) // lat > long for all HASH_GRID_SIZE
        .map(|(index, (_, _))| Range {
            start: index + 1,
            end: GEOHASH_MAX_LENGTH + 1, // The upper bound of the range (exclusive).
        })
        .unwrap_or_else(|| Range { start: 1, end: 1 })
}

/// A globally-average value is usually considered to be 6,371 kilometres (3,959 mi) with a 0.3% variability (±10 km).
/// https://en.wikipedia.org/wiki/Earth_radius.
const EARTH_RADIUS_KM: f64 = 6371.0;

/// Returns the GeoBoundingBox that defines the MBR
/// http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates#Longitude
fn minimum_bounding_rectangle_for_circle(circle: &GeoRadius) -> GeoBoundingBox {
    // circle.radius is in meter
    let angular_radius: f64 = (circle.radius / 1000.0) / EARTH_RADIUS_KM;
    let angular_lat = circle.center.lat.to_radians();
    let min_lat = (angular_lat - angular_radius).to_degrees();
    let max_lat = (angular_lat + angular_radius).to_degrees();

    let angular_lon = circle.center.lon.to_radians();

    let delta_lon = (angular_radius.sin() / angular_lat.cos()).asin();
    let min_lon = (angular_lon - delta_lon).to_degrees();
    let max_lon = (angular_lon + delta_lon).to_degrees();

    let top_left = GeoPoint {
        lat: max_lat,
        lon: min_lon,
    };
    let bottom_right = GeoPoint {
        lat: min_lat,
        lon: max_lon,
    };

    GeoBoundingBox {
        top_left,
        bottom_right,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BERLIN: GeoPoint = GeoPoint {
        lat: 52.52437,
        lon: 13.41053,
    };

    const NYC: GeoPoint = GeoPoint {
        lat: 40.75798,
        lon: -73.991516,
    };

    #[test]
    fn geohash_encode_longitude_first() {
        let center_hash = encode((NYC.lon, NYC.lat).into(), GEOHASH_MAX_LENGTH).unwrap();
        assert_eq!(center_hash, "dr5ru7c02wnv");
        let center_hash = encode((NYC.lon, NYC.lat).into(), 6).unwrap();
        assert_eq!(center_hash, "dr5ru7");
        let center_hash = encode((BERLIN.lon, BERLIN.lat).into(), GEOHASH_MAX_LENGTH).unwrap();
        assert_eq!(center_hash, "u33dc1v0xupz");
        let center_hash = encode((BERLIN.lon, BERLIN.lat).into(), 6).unwrap();
        assert_eq!(center_hash, "u33dc1");
    }

    #[test]
    fn rectangle_geo_hash_nyc() {
        // data from https://www.titanwolf.org/Network/q/a98ba365-14c5-48f4-8839-86a0962e0ab9/y
        let near_nyc_circle = GeoRadius {
            center: NYC,
            radius: 800.0,
        };

        let bounding_box = minimum_bounding_rectangle_for_circle(&near_nyc_circle);
        let rectangle =
            RectangleGeoHash::compute_from_bounding_box(&bounding_box, GEOHASH_MAX_LENGTH);
        assert_eq!(rectangle.north_west, "dr5ruj4477kd");
        assert_eq!(rectangle.south_west, "dr5ru46ne2ux");
        assert_eq!(rectangle.south_east, "dr5ru6ryw0cp");
        assert_eq!(rectangle.north_east, "dr5rumpfq534");
    }

    #[test]
    fn top_level_rectangle_geo_area() {
        let rect = RectangleGeoHash {
            north_west: "u".to_string(),
            south_west: "s".to_string(),
            south_east: "t".to_string(),
            north_east: "v".to_string(),
        };

        assert_eq!(rect.area_width_region_count(), 2);
        assert_eq!(rect.area_height_region_count(), 2);
        assert_eq!(rect.area_region_count(), 4);
        assert_eq!(rect.geohash_regions(), vec!["u", "s", "v", "t"]);
    }

    #[test]
    fn nyc_rectangle_geo_area_high_precision() {
        let rect = RectangleGeoHash {
            north_west: "dr5ruj4477kd".to_string(),
            south_west: "dr5ru46ne2ux".to_string(),
            south_east: "dr5ru6ryw0cp".to_string(),
            north_east: "dr5rumpfq534".to_string(),
        };

        assert_eq!(rect.area_width_region_count(), 56659);
        assert_eq!(rect.area_height_region_count(), 85836);
        assert_eq!(rect.area_region_count(), 56659 * 85836);
        // calling `rect.geohash_regions()` is too expensive
    }

    #[test]
    fn nyc_rectangle_geo_area_medium_precision() {
        let rect = RectangleGeoHash {
            north_west: "dr5ruj4".to_string(),
            south_west: "dr5ru46".to_string(),
            south_east: "dr5ru6r".to_string(),
            north_east: "dr5rump".to_string(),
        };

        assert_eq!(rect.area_width_region_count(), 14);
        assert_eq!(rect.area_height_region_count(), 12);
        let region_count = rect.area_region_count();
        let geo_area = rect.geohash_regions();
        assert_eq!(region_count, 14 * 12);
        assert_eq!(region_count, geo_area.len());
    }

    #[test]
    fn nyc_rectangle_geo_area_low_precision() {
        let rect = RectangleGeoHash {
            north_west: "dr5ruj".to_string(),
            south_west: "dr5ru4".to_string(),
            south_east: "dr5ru6".to_string(),
            north_east: "dr5rum".to_string(),
        };

        assert_eq!(rect.area_width_region_count(), 2);
        assert_eq!(rect.area_height_region_count(), 4);
        let region_count = rect.area_region_count();
        let geo_area = rect.geohash_regions();
        assert_eq!(region_count, 2 * 4);
        assert_eq!(region_count, geo_area.len());
        assert_eq!(
            geo_area,
            vec!["dr5ruj", "dr5ruh", "dr5ru5", "dr5ru4", "dr5rum", "dr5ruk", "dr5ru7", "dr5ru6"]
        );
    }

    #[test]
    fn validate_possible_geohash_precisions_for_distance() {
        // based on the dimensions from GEOHASH_MAX_LENGTH
        assert_eq!(geohash_precisions_for_distance(5010.0 * 1000.0).len(), 12);
        assert_eq!(geohash_precisions_for_distance(1300.0 * 1000.0).len(), 11);
        assert_eq!(geohash_precisions_for_distance(157.0 * 1000.0).len(), 10);
        assert_eq!(geohash_precisions_for_distance(40.0 * 1000.0).len(), 9);
        assert_eq!(geohash_precisions_for_distance(5.0 * 1000.0).len(), 8);
        assert_eq!(geohash_precisions_for_distance(2.0 * 1000.0).len(), 7);
        assert_eq!(geohash_precisions_for_distance(153.0).len(), 6);
        assert_eq!(geohash_precisions_for_distance(39.0).len(), 5);
        assert_eq!(geohash_precisions_for_distance(5.0).len(), 4);
        assert_eq!(geohash_precisions_for_distance(2.0).len(), 3);
        assert_eq!(geohash_precisions_for_distance(0.15).len(), 2);
        assert_eq!(geohash_precisions_for_distance(0.1).len(), 1);
    }

    #[test]
    fn rectangle_hashes_nyc() {
        // conversion to lon/lat http://geohash.co/
        // "dr5ruj4477kd"
        let top_left = GeoPoint {
            lon: -74.00101399,
            lat: 40.76517460,
        };

        // "dr5ru6ryw0cp"
        let bottom_right = GeoPoint {
            lon: -73.98201792,
            lat: 40.75078539,
        };

        let near_nyc_rectangle = GeoBoundingBox {
            top_left,
            bottom_right,
        };

        let nyc_hashes = rectangle_hashes(near_nyc_rectangle.clone(), 200);
        assert_eq!(nyc_hashes.len(), 168);
        assert!(nyc_hashes.iter().all(|h| h.len() == 7)); // geohash precision

        let nyc_hashes = rectangle_hashes(near_nyc_rectangle.clone(), 10);
        assert_eq!(nyc_hashes.len(), 8);
        assert!(nyc_hashes.iter().all(|h| h.len() == 6)); // geohash precision
        assert_eq!(
            nyc_hashes,
            ["dr5ruj", "dr5ruh", "dr5ru5", "dr5ru4", "dr5rum", "dr5ruk", "dr5ru7", "dr5ru6"]
        );

        // Graphical proof using https://www.movable-type.co.uk/scripts/geohash.html

        // dr5rgy dr5run dr5ruq dr5ruw
        // dr5rgv dr5ruj dr5rum dr5rut
        // dr5rgu dr5ruh dr5ruk dr5rus
        // dr5rgg dr5ru5 dr5ru7 dr5rue
        // dr5rgf dr5ru4 dr5ru6 dr5rud
        // dr5rgc dr5ru1 dr5ru3 dr5ru9

        // XXXXXX XXXXXX XXXXXX XXXXXX
        // XXXXXX dr5ruj dr5rum XXXXXX
        // XXXXXX dr5ruh dr5ruk XXXXXX
        // XXXXXX dr5ru5 dr5ru7 XXXXXX
        // XXXXXX dr5ru4 Xr5ru6 XXXXXX
        // XXXXXX XXXXXX XXXXXX XXXXXX

        // empty result if `max_regions` can not be honored
        let nyc_hashes = rectangle_hashes(near_nyc_rectangle, 7);
        assert!(nyc_hashes.is_empty());
    }

    #[test]
    fn circle_hashes_nyc() {
        let near_nyc_circle = GeoRadius {
            center: NYC,
            radius: 800.0,
        };

        let nyc_hashes = circle_hashes(near_nyc_circle.clone(), 200);
        assert_eq!(nyc_hashes.len(), 114);
        assert!(nyc_hashes.iter().all(|h| h.len() == 7)); // geohash precision

        let nyc_hashes = circle_hashes(near_nyc_circle.clone(), 10);
        assert_eq!(nyc_hashes.len(), 4);
        assert!(nyc_hashes.iter().all(|h| h.len() == 6)); // geohash precision
        assert_eq!(nyc_hashes, ["dr5ruh", "dr5ru5", "dr5ruk", "dr5ru7"]);

        // Graphical proof using https://www.movable-type.co.uk/scripts/geohash.html

        // dr5rgy dr5run dr5ruq dr5ruw
        // dr5rgv dr5ruj dr5rum dr5rut
        // dr5rgu dr5ruh dr5ruk dr5rus
        // dr5rgg dr5ru5 dr5ru7 dr5rue
        // dr5rgf dr5ru4 dr5ru6 dr5rud
        // dr5rgc dr5ru1 dr5ru3 dr5ru9

        // XXXXXX XXXXXX XXXXXX XXXXXX
        // XXXXXX XXXXXX XXXXXX XXXXXX
        // XXXXXX dr5ruh dr5ruk XXXXXX
        // XXXXXX dr5ru5 dr5ru7 XXXXXX
        // XXXXXX XXXXXX XXXXXX XXXXXX
        // XXXXXX XXXXXX XXXXXX XXXXXX

        // empty result if `max_regions` can not be honored
        let nyc_hashes = circle_hashes(near_nyc_circle, 7);
        assert!(nyc_hashes.is_empty());
    }
}
