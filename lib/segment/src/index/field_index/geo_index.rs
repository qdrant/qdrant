use crate::index::field_index::geo_index::SensiblePrecision::*;
use crate::types::{GeoBoundingBox, GeoPoint, GeoRadius};
use geo::algorithm::haversine_distance::HaversineDistance;
use geo::{Coordinate, Point};
use geohash::{decode, encode, Direction, GeohashError};
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

/// Fix longitude for spherical overflow
/// lon: 181.0 -> -179.0
fn sphere_lon(lon: f64) -> f64 {
    let max_lon = 180f64;
    let min_lon = -180f64;
    let mut res_lon = lon;
    if res_lon > max_lon {
        res_lon = min_lon + res_lon - max_lon;
    }
    if res_lon < min_lon {
        res_lon = max_lon + res_lon - min_lon;
    }
    res_lon
}

/// Fix latitude for spherical overflow
fn sphere_lat(lat: f64) -> f64 {
    let max_lat = 90f64;
    let min_lat = -90f64;
    let mut res_lat = lat;
    if res_lat > max_lat {
        res_lat = max_lat - 1e-16;
    }
    if res_lat < min_lat {
        res_lat = min_lat + 1e-16;
    }
    res_lat
}

/// Get neighbour geohash even from the other side of coordinates
fn sphere_neighbor(hash_str: &str, direction: Direction) -> Result<String, GeohashError> {
    let (coord, lon_err, lat_err) = decode(hash_str)?;
    let (dlat, dlng) = direction.to_tuple();
    let lon = sphere_lon(coord.x + 2f64 * lon_err.abs() * dlng);
    let lat = sphere_lat(coord.y + 2f64 * lat_err.abs() * dlat);

    let neighbor_coord = Coordinate { x: lon, y: lat };
    encode(neighbor_coord, hash_str.len())
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
                current = sphere_neighbor(&current, Direction::S).unwrap();
            }
            // move top column to the east
            top = sphere_neighbor(&top, Direction::E).unwrap();
        }
        seen
    }

    fn spans_single_tile(&self) -> bool {
        self.north_west == self.north_east
            && self.south_west == self.south_east
            && self.north_west == self.south_west
    }

    fn area_width_region_count(&self) -> usize {
        if self.spans_single_tile() {
            return 1;
        }

        let mut width_region = 0;
        // count starting tile
        width_region += 1;

        let mut current = self.north_west.clone();
        while current != self.north_east {
            let east_neighbor = sphere_neighbor(&current, Direction::E).unwrap();
            current = east_neighbor;
            width_region += 1;
        }
        width_region
    }

    fn area_height_region_count(&self) -> usize {
        if self.spans_single_tile() {
            return 1;
        }

        let mut height_region = 0;
        // count starting tile
        height_region += 1;
        let mut current = self.north_west.clone();
        while current != self.south_west {
            let south_neighbor = sphere_neighbor(&current, Direction::S).unwrap();
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
fn circle_hashes(circle: &GeoRadius, max_regions: usize) -> Vec<GeoHash> {
    assert_ne!(max_regions, 0, "max_regions cannot be equal to zero");
    let geo_bounding_box = minimum_bounding_rectangle_for_circle(circle);
    let full_geo_bounding_box: FullGeoBoundingBox = (&geo_bounding_box).into();
    let shortest_side_km = full_geo_bounding_box.shortest_side_length_in_km();
    let possible_precisions = geohash_precisions_for_distance(shortest_side_km * 1000.0);

    match possible_precisions {
        DistanceLargerThanLowestPrecision if max_regions == 1 => vec!["".to_string()],
        DistanceLargerThanLowestPrecision => {
            let rect = RectangleGeoHash::compute_from_bounding_box(&geo_bounding_box, 1);
            let circle_regions = filter_hashes_within_circle(rect.geohash_regions(), circle, 1);
            if circle_regions.len() <= max_regions {
                circle_regions
            } else {
                vec!["".to_string()]
            }
        }
        PrecisionRange(range) => {
            // filter precisions which generate less than `max_regions`.
            range
                .into_iter()
                .map(|p| {
                    let rect = RectangleGeoHash::compute_from_bounding_box(&geo_bounding_box, p);
                    filter_hashes_within_circle(rect.geohash_regions(), &circle, p)
                })
                .take_while(|circle_geohashes| circle_geohashes.len() <= max_regions)
                .last()
                .expect("not possible by construction")
        }
    }
}

/// Filter geo hashes for which the center is approximately within a circle.
/// Filtering strictly on `circle.radius` would miss all the tiles whose center is not exactly within the radius,
/// to mitigate the issue we add half of the tile's diagonal to the distance.
fn filter_hashes_within_circle(
    hashes: Vec<GeoHash>,
    circle: &GeoRadius,
    geohash_precision: usize,
) -> Vec<GeoHash> {
    let (lat_length_in_meters, long_length_in_meters) = HASH_GRID_SIZE[geohash_precision - 1];
    let tile_diagonal = (lat_length_in_meters.powi(2) + long_length_in_meters.powi(2)).sqrt();
    let distance = circle.radius + tile_diagonal / 2.0;
    let center_point = Point::new(circle.center.lon, circle.center.lat);
    hashes
        .into_iter()
        .map(|h| {
            let coord = decode(&h).unwrap().0;
            let geo_point = Point::new(coord.x, coord.y);
            (h, geo_point)
        })
        .filter(|(_, hash_point)| center_point.haversine_distance(hash_point) <= distance)
        .map(|(h, _)| h)
        .collect()
}

/// Return as-high-as-possible with maximum of `max_regions`
/// number of geo-hash guaranteed to contain the whole rectangle.
fn rectangle_hashes(rectangle: GeoBoundingBox, max_regions: usize) -> Vec<GeoHash> {
    assert_ne!(max_regions, 0, "max_regions cannot be equal to zero");
    let full_geo_bounding_box: FullGeoBoundingBox = (&rectangle).into();
    let shortest_side_km = full_geo_bounding_box.shortest_side_length_in_km();
    let possible_precisions = geohash_precisions_for_distance(shortest_side_km * 1000.0);

    match possible_precisions {
        DistanceLargerThanLowestPrecision if max_regions == 1 => vec!["".to_string()],
        DistanceLargerThanLowestPrecision => {
            let rectangle = RectangleGeoHash::compute_from_bounding_box(&rectangle, 1);
            if rectangle.area_region_count() <= max_regions {
                rectangle.geohash_regions()
            } else {
                vec!["".to_string()]
            }
        }
        PrecisionRange(range) => {
            // filter precisions which generate less than `max_regions`.
            range
                .into_iter()
                .map(|p| RectangleGeoHash::compute_from_bounding_box(&rectangle, p))
                .take_while(|rectangle_geohash| {
                    rectangle_geohash.area_region_count() <= max_regions
                })
                .last()
                .expect("not possible by construction")
                .geohash_regions()
        }
    }
}

#[derive(Debug, PartialEq)]
enum SensiblePrecision {
    DistanceLargerThanLowestPrecision,
    PrecisionRange(Range<usize>),
}

/// Given a distance, finds the geo precisions which cover the distance from a single large tile to several smaller tiles.
fn geohash_precisions_for_distance(distance_in_meter: f64) -> SensiblePrecision {
    let (largest_lat_length_in_meters, _) = HASH_GRID_SIZE[0];
    if distance_in_meter > largest_lat_length_in_meters {
        return DistanceLargerThanLowestPrecision;
    }

    let range = HASH_GRID_SIZE
        .iter()
        .enumerate()
        .find(|(_, (lat_length_in_meters, _))| distance_in_meter > *lat_length_in_meters) // lat > long for all HASH_GRID_SIZE
        .map(|(index, (_, _))| Range {
            start: index, // Start from the previous precision which covers the distance with a single tile
            end: GEOHASH_MAX_LENGTH + 1, // The upper bound of the range (exclusive).
        })
        .unwrap_or_else(|| Range {
            start: GEOHASH_MAX_LENGTH,
            end: GEOHASH_MAX_LENGTH + 1,
        }); // Unwrap means the distance is smaller than highest resolution
    PrecisionRange(range)
}

/// A globally-average value is usually considered to be 6,371 kilometres (3,959 mi) with a 0.3% variability (±10 km).
/// https://en.wikipedia.org/wiki/Earth_radius.
const EARTH_RADIUS_METERS: f64 = 6371.0 * 1000.;

/// Returns the GeoBoundingBox that defines the MBR
/// http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates#Longitude
fn minimum_bounding_rectangle_for_circle(circle: &GeoRadius) -> GeoBoundingBox {
    // circle.radius is in meter
    let angular_radius: f64 = circle.radius / EARTH_RADIUS_METERS;

    let angular_lat = circle.center.lat.to_radians();
    let min_lat = (angular_lat - angular_radius).to_degrees();
    let max_lat = (angular_lat + angular_radius).to_degrees();

    let angular_lon = circle.center.lon.to_radians();

    let delta_lon = (angular_radius.sin() / angular_lat.cos()).asin();
    let min_lon = (angular_lon - delta_lon).to_degrees();
    let max_lon = (angular_lon + delta_lon).to_degrees();

    let top_left = GeoPoint {
        lat: sphere_lat(max_lat),
        lon: sphere_lon(min_lon),
    };
    let bottom_right = GeoPoint {
        lat: sphere_lat(min_lat),
        lon: sphere_lon(max_lon),
    };

    GeoBoundingBox {
        top_left,
        bottom_right,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

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
        fn precisions_for_distance_range(distance_in_meter: f64) -> Range<usize> {
            let o = match geohash_precisions_for_distance(distance_in_meter) {
                DistanceLargerThanLowestPrecision => None,
                PrecisionRange(r) => Some(r),
            };
            o.unwrap_or_else(|| panic!("expected Range for {}", distance_in_meter))
        }

        assert_eq!(
            geohash_precisions_for_distance(5010.0 * 1000.0),
            DistanceLargerThanLowestPrecision
        );
        assert_eq!(precisions_for_distance_range(1300.0 * 1000.0).len(), 12);
        assert_eq!(precisions_for_distance_range(157.0 * 1000.0).len(), 11);
        assert_eq!(precisions_for_distance_range(40.0 * 1000.0).len(), 10);
        assert_eq!(precisions_for_distance_range(5.0 * 1000.0).len(), 9);
        assert_eq!(precisions_for_distance_range(2.0 * 1000.0).len(), 8);
        assert_eq!(precisions_for_distance_range(153.0).len(), 7);
        assert_eq!(precisions_for_distance_range(39.0).len(), 6);
        assert_eq!(precisions_for_distance_range(5.0).len(), 5);
        assert_eq!(precisions_for_distance_range(2.0).len(), 4);
        assert_eq!(precisions_for_distance_range(0.15).len(), 3);
        assert_eq!(precisions_for_distance_range(0.1).len(), 2);
        assert_eq!(precisions_for_distance_range(0.01).len(), 1);
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

        // falls back to finest region that encompasses the whole area
        let nyc_hashes = rectangle_hashes(near_nyc_rectangle, 7);
        assert_eq!(nyc_hashes, ["dr5ru"]);
    }

    #[test]
    fn random_circles() {
        let mut rnd = StdRng::seed_from_u64(42);
        for _ in 0..1000 {
            let r_meters = rnd.gen_range(1.0..10000.0);
            let query = GeoRadius {
                center: GeoPoint {
                    lon: rnd.gen_range(-180.0..180.0),
                    lat: rnd.gen_range(-90.0..90.0),
                },
                radius: r_meters,
            };
            eprintln!("query = {:#?}", query);
            let max_hashes = rnd.gen_range(1..32);
            let hashes = circle_hashes(&query, max_hashes);
            eprintln!("hashes = {:#?}", hashes);

            assert!(hashes.len() <= max_hashes);
            assert!(
                hashes.len() > 0,
                "max_hashes: {} query: {:?}",
                max_hashes,
                query
            );
        }
    }

    #[test]
    fn circle_meridian() {
        let query = GeoRadius {
            center: GeoPoint {
                lon: -17.81718188959701,
                lat: 89.9811609411936,
            },
            radius: 9199.481636468849,
        };

        let max_hashes = 10;
        let hashes = circle_hashes(&query, max_hashes);
    }

    #[test]
    fn circle_south_pole() {
        let query = GeoRadius {
            center: GeoPoint {
                lon: 155.85591760141335,
                lat: -74.19418872656166,
            },
            radius: 7133.775526733084,
        };
        let max_hashes = 10;
        let hashes = circle_hashes(&query, max_hashes);
        assert!(hashes.len() <= max_hashes);
        assert!(
            hashes.len() > 0,
            "max_hashes: {} query: {:?}",
            max_hashes,
            query
        );
    }

    #[test]
    fn circle_hashes_nyc() {
        let near_nyc_circle = GeoRadius {
            center: NYC,
            radius: 800.0,
        };

        let nyc_hashes = circle_hashes(&near_nyc_circle, 200);
        assert_eq!(nyc_hashes.len(), 140);
        assert!(nyc_hashes.iter().all(|h| h.len() == 7)); // geohash precision

        let nyc_hashes = circle_hashes(&near_nyc_circle, 10);
        assert_eq!(nyc_hashes.len(), 8);
        assert!(nyc_hashes.iter().all(|h| h.len() == 6)); // geohash precision
        assert_eq!(
            nyc_hashes,
            ["dr5ruj", "dr5ruh", "dr5ru5", "dr5ru4", "dr5rum", "dr5ruk", "dr5ru7", "dr5ru6"]
        );

        // falls back to finest region that encompasses the whole area
        let nyc_hashes = circle_hashes(&near_nyc_circle, 7);
        assert_eq!(nyc_hashes, ["dr5ru"]);
    }

    #[test]
    fn go_north() {
        let mut geohash = sphere_neighbor("ww8p", Direction::N).unwrap();
        for _ in 0..1000 {
            geohash = sphere_neighbor(&geohash, Direction::N).unwrap();
        }
    }

    #[test]
    fn go_west() {
        let mut geohash = sphere_neighbor("ww8p", Direction::W).unwrap();
        for _ in 0..1000 {
            geohash = sphere_neighbor(&geohash, Direction::W).unwrap();
        }
    }

    #[test]
    fn sphere_neighbor_corner_cases() {
        assert_eq!(
            sphere_neighbor("ww8p1r4t8", Direction::SE).unwrap(),
            geohash::neighbor("ww8p1r4t8", Direction::SE).unwrap()
        );

        assert_eq!(&sphere_neighbor("8", Direction::W).unwrap(), "x");
        assert_eq!(&sphere_neighbor("8h", Direction::W).unwrap(), "xu");
        assert_eq!(&sphere_neighbor("r", Direction::E).unwrap(), "2");
        assert_eq!(&sphere_neighbor("ru", Direction::E).unwrap(), "2h");
        assert_eq!(&sphere_neighbor("z", Direction::NE).unwrap(), "b");
        assert_eq!(&sphere_neighbor("zz", Direction::NE).unwrap(), "bp");
        assert_eq!(&sphere_neighbor("0", Direction::SW).unwrap(), "p");
        assert_eq!(&sphere_neighbor("00", Direction::SW).unwrap(), "pb");
    }

    #[test]
    fn long_overflow_distance() {
        let dist = Point::new(-179.999, 66.0).haversine_distance(&Point::new(179.999, 66.0));
        eprintln!("dist` = {:#?}", dist);
        let dist = Point::new(0.99, 90.).haversine_distance(&Point::new(0.99, -90.0));
        eprintln!("dist` = {:#?}", dist);
    }
}
