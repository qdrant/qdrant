use std::ops::Range;

use geo::algorithm::haversine_distance::HaversineDistance;
use geo::{Coord, Intersects, LineString, Point, Polygon};
use geohash::{decode, decode_bbox, encode, Direction, GeohashError};
use itertools::Itertools;
use smol_str::SmolStr;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{GeoBoundingBox, GeoPoint, GeoPolygon, GeoRadius};

pub type GeoHash = SmolStr;

/// Max size of geo-hash used for indexing. size=12 is about 6cm2
const GEOHASH_MAX_LENGTH: usize = 12;

const LON_RANGE: Range<f64> = -180.0..180.0;
const LAT_RANGE: Range<f64> = -90.0..90.0;
const COORD_EPS: f64 = 1e-12;

impl From<GeoPoint> for Coord<f64> {
    fn from(point: GeoPoint) -> Self {
        Self {
            x: point.lat,
            y: point.lon,
        }
    }
}

pub fn common_hash_prefix(geo_hashes: &[GeoHash]) -> GeoHash {
    let first = &geo_hashes[0];
    let mut prefix: usize = first.len();
    for geo_hash in geo_hashes.iter().skip(1) {
        for i in 0..prefix {
            if first.as_bytes()[i] != geo_hash.as_bytes()[i] {
                prefix = i;
                break;
            }
        }
    }
    first[0..prefix].into()
}

/// Fix longitude for spherical overflow
/// lon: 181.0 -> -179.0
fn sphere_lon(lon: f64) -> f64 {
    let mut res_lon = lon;
    if res_lon > LON_RANGE.end {
        res_lon = LON_RANGE.start + res_lon - LON_RANGE.end;
    }
    if res_lon < LON_RANGE.start {
        res_lon = LON_RANGE.end + res_lon - LON_RANGE.start;
    }
    res_lon
}

/// Fix latitude for spherical overflow
fn sphere_lat(lat: f64) -> f64 {
    let mut res_lat = lat;
    if res_lat > LAT_RANGE.end {
        res_lat = LAT_RANGE.end - COORD_EPS;
    }
    if res_lat < LAT_RANGE.start {
        res_lat = LAT_RANGE.start + COORD_EPS;
    }
    res_lat
}

/// Get neighbour geohash even from the other side of coordinates
fn sphere_neighbor(hash_str: &str, direction: Direction) -> Result<GeoHash, GeohashError> {
    let (coord, lon_err, lat_err) = decode(hash_str)?;
    let (dlat, dlng) = direction.to_tuple();
    let lon = sphere_lon(coord.x + 2f64 * lon_err.abs() * dlng);
    let lat = sphere_lat(coord.y + 2f64 * lat_err.abs() * dlat);

    let neighbor_coord = Coord { x: lon, y: lat };
    encode(neighbor_coord, hash_str.len()).map(Into::into)
}

pub fn encode_max_precision(lon: f64, lat: f64) -> Result<GeoHash, GeohashError> {
    encode((lon, lat).into(), GEOHASH_MAX_LENGTH).map(Into::into)
}

pub fn geo_hash_to_box(geo_hash: &GeoHash) -> GeoBoundingBox {
    let rectangle = decode_bbox(geo_hash).unwrap();
    let top_left = GeoPoint {
        lon: rectangle.min().x,
        lat: rectangle.max().y,
    };
    let bottom_right = GeoPoint {
        lon: rectangle.max().x,
        lat: rectangle.min().y,
    };

    GeoBoundingBox {
        top_left,
        bottom_right,
    }
}

#[derive(Debug)]
struct GeohashBoundingBox {
    north_west: GeoHash,
    south_west: GeoHash,
    #[allow(dead_code)]
    south_east: GeoHash, // field is not involved in the calculations, but is kept for symmetry
    north_east: GeoHash,
}

impl GeohashBoundingBox {
    /// Calculate geo-hashes covering the rectangular region with given precision
    ///
    /// # Arguments
    ///
    /// * `precision` - precision of cover
    /// * `max_regions` - stop early if maximal amount of regions exceeded
    ///
    /// # Result
    ///
    /// * None - if there are more regions than a limit
    /// * Some(list of geo-hashes covering the region
    ///
    fn geohash_regions(&self, precision: usize, max_regions: usize) -> Option<Vec<GeoHash>> {
        let mut seen: Vec<GeoHash> = Vec::new();

        let mut from_row: GeoHash = self.north_west[..precision].into();
        let mut to_row: GeoHash = self.north_east[..precision].into();

        let to_column = self.south_west[..precision].to_owned();

        loop {
            let mut current = from_row.clone();
            loop {
                seen.push(current.clone());

                if seen.len() > max_regions {
                    return None;
                }

                if current == to_row {
                    break;
                }
                current = sphere_neighbor(current.as_str(), Direction::E).unwrap();
            }
            if from_row == to_column {
                break;
            }

            from_row = sphere_neighbor(&from_row, Direction::S).unwrap();
            to_row = sphere_neighbor(&to_row, Direction::S).unwrap();
        }

        Some(seen)
    }
}

impl From<GeoBoundingBox> for GeohashBoundingBox {
    fn from(bounding_box: GeoBoundingBox) -> Self {
        let GeoPoint {
            lat: max_lat,
            lon: min_lon,
        } = bounding_box.top_left;
        let GeoPoint {
            lat: min_lat,
            lon: max_lon,
        } = bounding_box.bottom_right;

        // Unwrap is acceptable, as data should be validated before
        let north_west = encode_max_precision(min_lon, max_lat).unwrap();
        let south_west = encode_max_precision(min_lon, min_lat).unwrap();
        let south_east = encode_max_precision(max_lon, min_lat).unwrap();
        let north_east = encode_max_precision(max_lon, max_lat).unwrap();

        Self {
            north_west,
            south_west,
            south_east,
            north_east,
        }
    }
}

/// Check if geohash tile intersects the circle
fn check_circle_intersection(geohash: &str, circle: &GeoRadius) -> bool {
    let precision = geohash.len();
    if precision == 0 {
        return true;
    }
    let rect = decode_bbox(geohash).unwrap();
    let c0 = rect.min();
    let c1 = rect.max();

    let bbox_center = Point::new((c0.x + c1.x) / 2f64, (c0.y + c1.y) / 2f64);
    let half_diagonal = bbox_center.haversine_distance(&Point(c0));

    half_diagonal + circle.radius
        > bbox_center.haversine_distance(&Point::new(circle.center.lon, circle.center.lat))
}

/// Check if geohash tile intersects the polygon
fn check_polygon_intersection(geohash: &str, polygon: &Polygon) -> bool {
    let precision = geohash.len();
    if precision == 0 {
        return true;
    }
    let rect = decode_bbox(geohash).unwrap();

    rect.intersects(polygon)
}

fn create_hashes(
    mapping_fn: impl Fn(usize) -> Option<Vec<GeoHash>>,
) -> OperationResult<Vec<GeoHash>> {
    (0..=GEOHASH_MAX_LENGTH)
        .map(mapping_fn)
        .take_while(|hashes| hashes.is_some())
        .last()
        .ok_or_else(|| OperationError::service_error("no hash coverage for any precision"))?
        .ok_or_else(|| OperationError::service_error("geo-hash coverage is empty"))
}

/// Return as-high-as-possible with maximum of `max_regions`
/// number of geo-hash guaranteed to contain the whole circle.
pub fn circle_hashes(circle: &GeoRadius, max_regions: usize) -> OperationResult<Vec<GeoHash>> {
    if max_regions == 0 {
        return Err(OperationError::service_error(
            "max_regions cannot be equal to zero",
        ));
    }

    let geo_bounding_box = minimum_bounding_rectangle_for_circle(circle);
    if geo_bounding_box.top_left.lat.is_nan()
        || geo_bounding_box.top_left.lon.is_nan()
        || geo_bounding_box.bottom_right.lat.is_nan()
        || geo_bounding_box.bottom_right.lat.is_nan()
    {
        return Err(OperationError::service_error("Invalid circle"));
    }
    let full_geohash_bounding_box: GeohashBoundingBox = geo_bounding_box.into();

    let mapping_fn = |precision| {
        full_geohash_bounding_box
            .geohash_regions(precision, max_regions)
            .map(|hashes| {
                hashes
                    .into_iter()
                    .filter(|hash| check_circle_intersection(hash, circle))
                    .collect_vec()
            })
    };
    create_hashes(mapping_fn)
}

/// Return as-high-as-possible with maximum of `max_regions`
/// number of geo-hash guaranteed to contain the whole rectangle.
pub fn rectangle_hashes(
    rectangle: &GeoBoundingBox,
    max_regions: usize,
) -> OperationResult<Vec<GeoHash>> {
    if max_regions == 0 {
        return Err(OperationError::service_error(
            "max_regions cannot be equal to zero",
        ));
    }
    let full_geohash_bounding_box: GeohashBoundingBox = rectangle.clone().into();

    let mapping_fn = |precision| full_geohash_bounding_box.geohash_regions(precision, max_regions);
    create_hashes(mapping_fn)
}

/// Return as-high-as-possible with maximum of `max_regions`
/// number of geo-hash guaranteed to contain a boundary defined by closed LineString.
fn boundary_hashes(boundary: &LineString, max_regions: usize) -> OperationResult<Vec<GeoHash>> {
    let geo_bounding_box = minimum_bounding_rectangle_for_boundary(boundary);
    let full_geohash_bounding_box: GeohashBoundingBox = geo_bounding_box.into();
    let polygon = Polygon::new(boundary.clone(), vec![]);

    let mapping_fn = |precision| {
        full_geohash_bounding_box
            .geohash_regions(precision, max_regions)
            .map(|hashes| {
                hashes
                    .into_iter()
                    .filter(|hash| check_polygon_intersection(hash, &polygon))
                    .collect_vec()
            })
    };
    create_hashes(mapping_fn)
}

/// A function used for cardinality estimation
/// The first return value is as-high-as-possible with maximum of `max_regions`
/// number of geo-hash guaranteed to contain the polygon's exterior.
/// The second return value is all as-high-as-possible with maximum of `max_regions`
/// number of geo-hash guaranteed to contain each polygon's interior.
pub fn polygon_hashes_estimation(
    polygon: &GeoPolygon,
    max_regions: usize,
) -> (Vec<GeoHash>, Vec<Vec<GeoHash>>) {
    assert_ne!(max_regions, 0, "max_regions cannot be equal to zero");
    let polygon_wrapper = polygon.convert().polygon;
    let exterior_hashes = boundary_hashes(&polygon_wrapper.exterior().clone(), max_regions);
    let interiors_hashes = polygon_wrapper
        .interiors()
        .iter()
        .map(|interior| boundary_hashes(interior, max_regions).unwrap())
        .collect_vec();

    (exterior_hashes.unwrap(), interiors_hashes)
}

/// Return as-high-as-possible with maximum of `max_regions`
/// number of geo-hash guaranteed to contain the whole polygon.
pub fn polygon_hashes(polygon: &GeoPolygon, max_regions: usize) -> OperationResult<Vec<GeoHash>> {
    if max_regions == 0 {
        return Err(OperationError::service_error(
            "max_regions cannot be equal to zero",
        ));
    }
    let polygon_wrapper = polygon.convert().polygon;
    let geo_bounding_box = minimum_bounding_rectangle_for_boundary(polygon_wrapper.exterior());
    let full_geohash_bounding_box: GeohashBoundingBox = geo_bounding_box.into();

    let mapping_fn = |precision| {
        full_geohash_bounding_box
            .geohash_regions(precision, max_regions)
            .map(|hashes| {
                hashes
                    .into_iter()
                    .filter(|hash| check_polygon_intersection(hash, &polygon_wrapper))
                    .collect_vec()
            })
    };
    create_hashes(mapping_fn)
}

/// A globally-average value is usually considered to be 6,371 kilometres (3,959 mi) with a 0.3% variability (±10 km).
/// <https://en.wikipedia.org/wiki/Earth_radius>.
const EARTH_RADIUS_METERS: f64 = 6371.0 * 1000.;

/// Returns the GeoBoundingBox that defines the MBR
/// <http://janmatuschek.de/LatitudeLongitudeBoundingCoordinates#Longitude>
fn minimum_bounding_rectangle_for_circle(circle: &GeoRadius) -> GeoBoundingBox {
    // circle.radius is in meter
    let angular_radius: f64 = circle.radius / EARTH_RADIUS_METERS;

    let angular_lat = circle.center.lat.to_radians();
    let mut min_lat = (angular_lat - angular_radius).to_degrees();
    let mut max_lat = (angular_lat + angular_radius).to_degrees();

    let (min_lon, max_lon) = if LAT_RANGE.start < min_lat && max_lat < LAT_RANGE.end {
        // Poles are not within the query, default scenario
        let angular_lon = circle.center.lon.to_radians();
        let delta_lon = (angular_radius.sin() / angular_lat.cos()).asin();

        let min_lon = (angular_lon - delta_lon).to_degrees();
        let max_lon = (angular_lon + delta_lon).to_degrees();

        (min_lon, max_lon)
    } else {
        // poles are within circle - use whole cup
        if LAT_RANGE.start > min_lat {
            min_lat = LAT_RANGE.start + COORD_EPS;
        }
        if max_lat > LAT_RANGE.end {
            max_lat = LAT_RANGE.end - COORD_EPS;
        }

        (LON_RANGE.start + COORD_EPS, LON_RANGE.end - COORD_EPS)
    };

    let top_left = GeoPoint {
        lat: max_lat,
        lon: sphere_lon(min_lon),
    };
    let bottom_right = GeoPoint {
        lat: min_lat,
        lon: sphere_lon(max_lon),
    };

    GeoBoundingBox {
        top_left,
        bottom_right,
    }
}

fn minimum_bounding_rectangle_for_boundary(boundary: &LineString) -> GeoBoundingBox {
    let mut min_lon = f64::MAX;
    let mut max_lon = f64::MIN;
    let mut min_lat = f64::MAX;
    let mut max_lat = f64::MIN;

    for point in boundary.coords() {
        if point.x < min_lon {
            min_lon = point.x;
        }
        if point.x > max_lon {
            max_lon = point.x;
        }
        if point.y < min_lat {
            min_lat = point.y;
        }
        if point.y > max_lat {
            max_lat = point.y;
        }
    }

    let top_left = GeoPoint {
        lon: min_lon,
        lat: max_lat,
    };
    let bottom_right = GeoPoint {
        lon: max_lon,
        lat: min_lat,
    };

    GeoBoundingBox {
        top_left,
        bottom_right,
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::types::test_utils::{build_polygon, build_polygon_with_interiors};

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
        let rectangle: GeohashBoundingBox = bounding_box.into();
        assert_eq!(rectangle.north_west, "dr5ruj4477kd");
        assert_eq!(rectangle.south_west, "dr5ru46ne2ux");
        assert_eq!(rectangle.south_east, "dr5ru6ryw0cp");
        assert_eq!(rectangle.north_east, "dr5rumpfq534");
    }

    #[test]
    fn top_level_rectangle_geo_area() {
        let rect = GeohashBoundingBox {
            north_west: "u".into(),
            south_west: "s".into(),
            south_east: "t".into(),
            north_east: "v".into(),
        };
        let mut geo_area = rect.geohash_regions(1, 100).unwrap();
        let mut expected = vec!["u", "s", "v", "t"];

        geo_area.sort_unstable();
        expected.sort_unstable();
        assert_eq!(geo_area, expected);
    }

    #[test]
    fn nyc_rectangle_geo_area_high_precision() {
        let rect = GeohashBoundingBox {
            north_west: "dr5ruj4477kd".into(),
            south_west: "dr5ru46ne2ux".into(),
            south_east: "dr5ru6ryw0cp".into(),
            north_east: "dr5rumpfq534".into(),
        };

        // calling `rect.geohash_regions()` is too expensive
        assert!(rect.geohash_regions(12, 100).is_none());
    }

    #[test]
    fn nyc_rectangle_geo_area_medium_precision() {
        let rect = GeohashBoundingBox {
            north_west: "dr5ruj4".into(),
            south_west: "dr5ru46".into(),
            south_east: "dr5ru6r".into(),
            north_east: "dr5rump".into(),
        };

        let geo_area = rect.geohash_regions(7, 1000).unwrap();
        assert_eq!(14 * 12, geo_area.len());
    }

    #[test]
    fn nyc_rectangle_geo_area_low_precision() {
        let rect = GeohashBoundingBox {
            north_west: "dr5ruj".into(),
            south_west: "dr5ru4".into(),
            south_east: "dr5ru6".into(),
            north_east: "dr5rum".into(),
        };

        let mut geo_area = rect.geohash_regions(6, 100).unwrap();
        let mut expected = vec![
            "dr5ru4", "dr5ru5", "dr5ru6", "dr5ru7", "dr5ruh", "dr5ruj", "dr5rum", "dr5ruk",
        ];

        expected.sort_unstable();
        geo_area.sort_unstable();
        assert_eq!(geo_area, expected);
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

        let nyc_hashes_result = rectangle_hashes(&near_nyc_rectangle, 200);
        let nyc_hashes = nyc_hashes_result.unwrap();
        assert_eq!(nyc_hashes.len(), 168);
        assert!(nyc_hashes.iter().all(|h| h.len() == 7)); // geohash precision

        let mut nyc_hashes_result = rectangle_hashes(&near_nyc_rectangle, 10);
        nyc_hashes_result.as_mut().unwrap().sort_unstable();
        let mut expected = vec![
            "dr5ruj", "dr5ruh", "dr5ru5", "dr5ru4", "dr5rum", "dr5ruk", "dr5ru7", "dr5ru6",
        ];
        expected.sort_unstable();

        assert_eq!(nyc_hashes_result.unwrap(), expected);

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
        let nyc_hashes_result = rectangle_hashes(&near_nyc_rectangle, 7);
        assert_eq!(nyc_hashes_result.unwrap(), ["dr5ru"]);
    }

    #[test]
    fn polygon_hashes_nyc() {
        // conversion to lon/lat http://geohash.co/
        // "dr5ruj4477kd"
        let near_nyc_polygon = build_polygon(vec![
            (-74.00101399, 40.76517460),
            (-73.98201792, 40.76517460),
            (-73.98201792, 40.75078539),
            (-74.00101399, 40.75078539),
            (-74.00101399, 40.76517460),
        ]);

        let nyc_hashes_result = polygon_hashes(&near_nyc_polygon, 200);
        let nyc_hashes = nyc_hashes_result.unwrap();
        assert_eq!(nyc_hashes.len(), 168);
        assert!(nyc_hashes.iter().all(|h| h.len() == 7)); // geohash precision

        let mut nyc_hashes_result = polygon_hashes(&near_nyc_polygon, 10);
        nyc_hashes_result.as_mut().unwrap().sort_unstable();
        let mut expected = vec![
            "dr5ruj", "dr5ruh", "dr5ru5", "dr5ru4", "dr5rum", "dr5ruk", "dr5ru7", "dr5ru6",
        ];
        expected.sort_unstable();

        assert_eq!(nyc_hashes_result.unwrap(), expected);

        // falls back to finest region that encompasses the whole area
        let nyc_hashes_result = polygon_hashes(&near_nyc_polygon, 7);
        assert_eq!(nyc_hashes_result.unwrap(), ["dr5ru"]);
    }

    #[test]
    fn random_circles() {
        let mut rnd = StdRng::seed_from_u64(42);
        for _ in 0..1000 {
            let r_meters = rnd.gen_range(1.0..10000.0);
            let query = GeoRadius {
                center: GeoPoint {
                    lon: rnd.gen_range(LON_RANGE),
                    lat: rnd.gen_range(LAT_RANGE),
                },
                radius: r_meters,
            };
            let max_hashes = rnd.gen_range(1..32);
            let hashes = circle_hashes(&query, max_hashes);
            assert!(hashes.unwrap().len() <= max_hashes);
        }
    }

    #[test]
    fn test_check_polygon_intersection() {
        fn check_intersection(geohash: &str, polygon: &GeoPolygon, expected: bool) {
            let intersect = check_polygon_intersection(geohash, &polygon.convert().polygon);
            assert_eq!(intersect, expected);
        }

        // Create a geohash as (-56.2, 33.75), (-56.2, 39.375), (-45, 39.375), (-45, 33.75)
        let geohash = encode(Coord { x: -50.0, y: 35.0 }, 2).unwrap();

        // Test a polygon intersect with the geohash
        check_intersection(
            &geohash,
            &build_polygon(vec![
                (-60.0, 37.0),
                (-60.0, 45.0),
                (-50.0, 45.0),
                (-50.0, 37.0),
                (-60.0, 37.0),
            ]),
            true,
        );

        // Test a polygon does not intersect with the geohash
        check_intersection(
            &geohash,
            &build_polygon(vec![
                (-70.2, 50.8),
                (-70.2, 55.9),
                (-65.6, 55.9),
                (-65.6, 50.8),
                (-70.2, 50.8),
            ]),
            false,
        );

        // Test a polygon that overlap with the geohash
        check_intersection(
            &geohash,
            &build_polygon(vec![
                (-56.2, 33.75),
                (-56.2, 39.375),
                (-45.0, 39.375),
                (-45.0, 33.75),
                (-56.2, 33.75),
            ]),
            true,
        );

        // Test a polygon that only share one edge with the geohash
        check_intersection(
            &geohash,
            &build_polygon(vec![
                (-45.0, 39.375),
                (-45.0, 45.0),
                (-30.9, 45.0),
                (-30.9, 39.375),
                (-45.0, 39.375),
            ]),
            true,
        );

        // Test a polygon that is within the geohash
        check_intersection(
            &geohash,
            &build_polygon(vec![
                (-55.7, 34.3),
                (-55.7, 38.0),
                (-46.8, 38.0),
                (-46.8, 34.3),
                (-55.7, 34.3),
            ]),
            true,
        );

        // Test a polygon that contains the geohash
        check_intersection(
            &geohash,
            &build_polygon(vec![
                (-60.0, 33.0),
                (-60.0, 40.0),
                (-44.0, 40.0),
                (-44.0, 33.0),
                (-60.0, 33.0),
            ]),
            true,
        );

        // The geohash is in the exterior of the polygon
        check_intersection(
            &geohash,
            &build_polygon_with_interiors(
                vec![
                    (-70.0, 13.0),
                    (-70.0, 50.0),
                    (-34.0, 50.0),
                    (-34.0, 13.0),
                    (-70.0, 13.0),
                ],
                vec![vec![
                    (-60.0, 33.0),
                    (-60.0, 40.0),
                    (-44.0, 40.0),
                    (-44.0, 33.0),
                    (-60.0, 33.0),
                ]],
            ),
            false,
        );
    }

    #[test]
    fn test_lon_threshold() {
        let query = GeoRadius {
            center: GeoPoint {
                lon: 179.987181,
                lat: 44.9811609411936,
            },
            radius: 100000.,
        };

        let max_hashes = 10;
        let hashes = circle_hashes(&query, max_hashes);
        assert_eq!(hashes.unwrap(), vec!["zbp", "b00", "xzz", "8pb"]);
    }

    #[test]
    fn wide_circle_meridian() {
        let query = GeoRadius {
            center: GeoPoint {
                lon: -17.81718188959701,
                lat: 89.9811609411936,
            },
            radius: 9199.481636468849,
        };

        let max_hashes = 10;
        let hashes = circle_hashes(&query, max_hashes);
        let vec = hashes.unwrap();
        assert!(vec.len() <= max_hashes);
        assert_eq!(vec, ["b", "c", "f", "g", "u", "v", "y", "z"]);
    }

    #[test]
    fn tight_circle_meridian() {
        let query = GeoRadius {
            center: GeoPoint {
                lon: -17.81718188959701,
                lat: 89.9811609411936,
            },
            radius: 1000.0,
        };

        let max_hashes = 10;
        let hashes_result = circle_hashes(&query, max_hashes);
        let hashes = hashes_result.unwrap();
        assert!(hashes.len() <= max_hashes);
        assert_eq!(hashes, ["fz", "gp", "gr", "gx", "gz", "up"]);
    }

    #[test]
    fn wide_circle_south_pole() {
        let query = GeoRadius {
            center: GeoPoint {
                lon: 155.85591760141335,
                lat: -74.19418872656166,
            },
            radius: 7133.775526733084,
        };
        let max_hashes = 10;
        let hashes_result = circle_hashes(&query, max_hashes);
        let hashes = hashes_result.unwrap();
        assert!(hashes.len() <= max_hashes);
        assert_eq!(hashes, ["p6yd", "p6yf", "p6y9", "p6yc"]);
    }

    #[test]
    fn tight_circle_south_pole() {
        let query = GeoRadius {
            center: GeoPoint {
                lon: 155.85591760141335,
                lat: -74.19418872656166,
            },
            radius: 1000.0,
        };
        let max_hashes = 10;
        let hashes_result = circle_hashes(&query, max_hashes);
        let hashes = hashes_result.unwrap();
        assert!(hashes.len() <= max_hashes);
        assert_eq!(hashes, ["p6ycc", "p6ycf", "p6ycg"]);
    }

    #[test]
    fn circle_hashes_nyc() {
        let near_nyc_circle = GeoRadius {
            center: NYC,
            radius: 800.0,
        };

        let nyc_hashes_result = circle_hashes(&near_nyc_circle, 200).unwrap();
        assert!(nyc_hashes_result.iter().all(|h| h.len() == 7)); // geohash precision

        let mut nyc_hashes_result = circle_hashes(&near_nyc_circle, 10);
        nyc_hashes_result.as_mut().unwrap().sort_unstable();
        let mut expected = [
            "dr5ruj", "dr5ruh", "dr5ru5", "dr5ru4", "dr5rum", "dr5ruk", "dr5ru7", "dr5ru6",
        ];
        expected.sort_unstable();
        assert_eq!(nyc_hashes_result.unwrap(), expected);

        // falls back to finest region that encompasses the whole area
        let nyc_hashes_result = circle_hashes(&near_nyc_circle, 7);
        assert_eq!(nyc_hashes_result.unwrap(), ["dr5ru"]);
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
        let starting_hash = "ww8";
        let mut geohash = sphere_neighbor(starting_hash, Direction::W).unwrap();
        let mut is_earth_round = false;
        for _ in 0..1000 {
            geohash = sphere_neighbor(&geohash, Direction::W).unwrap();
            if geohash == starting_hash {
                is_earth_round = true;
            }
        }
        assert!(is_earth_round)
    }

    #[test]
    fn sphere_neighbor_corner_cases() {
        assert_eq!(&sphere_neighbor("z", Direction::NE).unwrap(), "b");
        assert_eq!(&sphere_neighbor("zz", Direction::NE).unwrap(), "bp");
        assert_eq!(&sphere_neighbor("0", Direction::SW).unwrap(), "p");
        assert_eq!(&sphere_neighbor("00", Direction::SW).unwrap(), "pb");

        assert_eq!(&sphere_neighbor("8", Direction::W).unwrap(), "x");
        assert_eq!(&sphere_neighbor("8h", Direction::W).unwrap(), "xu");
        assert_eq!(&sphere_neighbor("r", Direction::E).unwrap(), "2");
        assert_eq!(&sphere_neighbor("ru", Direction::E).unwrap(), "2h");

        assert_eq!(
            sphere_neighbor("ww8p1r4t8", Direction::SE).unwrap(),
            geohash::neighbor("ww8p1r4t8", Direction::SE).unwrap()
        );
    }

    #[test]
    fn long_overflow_distance() {
        let dist = Point::new(-179.999, 66.0).haversine_distance(&Point::new(179.999, 66.0));
        eprintln!("dist` = {dist:#?}");
        assert_eq!(dist, 90.45422731917998);
        let dist = Point::new(0.99, 90.).haversine_distance(&Point::new(0.99, -90.0));
        assert_eq!(dist, 20015114.442035925);
    }

    #[test]
    fn turn_geo_hash_to_box() {
        let geo_box = geo_hash_to_box(&"dr5ruj4477kd".into());
        let center = GeoPoint {
            lat: 40.76517460,
            lon: -74.00101399,
        };
        assert!(geo_box.check_point(&center));
    }

    #[test]
    fn common_prefix() {
        let geo_hashes = vec![
            "abcd123".into(),
            "abcd2233".into(),
            "abcd3213".into(),
            "abcd533".into(),
        ];

        let common_prefix = common_hash_prefix(&geo_hashes);

        assert_eq!(&common_prefix, "abcd");

        let geo_hashes = vec![
            "abcd123".into(),
            "bbcd2233".into(),
            "cbcd3213".into(),
            "dbcd533".into(),
        ];

        let common_prefix = common_hash_prefix(&geo_hashes);

        assert_eq!(&common_prefix, "");
    }

    #[test]
    fn max_regions_cannot_be_equal_to_zero() {
        let invalid_max_hashes = 0;

        // circle
        let sample_circle = GeoRadius {
            center: GeoPoint {
                lon: 179.987181,
                lat: 44.9811609411936,
            },
            radius: 100000.,
        };
        let circle_hashes = circle_hashes(&sample_circle, invalid_max_hashes);
        assert!(circle_hashes.is_err());

        // rectangle
        let top_left = GeoPoint {
            lon: -74.00101399,
            lat: 40.76517460,
        };

        let bottom_right = GeoPoint {
            lon: -73.98201792,
            lat: 40.75078539,
        };

        let sample_rectangle = GeoBoundingBox {
            top_left: top_left.clone(),
            bottom_right: bottom_right.clone(),
        };
        let rectangle_hashes = rectangle_hashes(&sample_rectangle, invalid_max_hashes);
        assert!(rectangle_hashes.is_err());

        // polygon
        let sample_polygon = build_polygon(vec![
            (-74.00101399, 40.76517460),
            (-73.98201792, 40.75078539),
        ]);

        let polygon_hashes = polygon_hashes(&sample_polygon, invalid_max_hashes);
        assert!(polygon_hashes.is_err());
    }

    #[test]
    fn geo_radius_zero_division() {
        let circle = GeoRadius {
            center: GeoPoint {
                lon: 45.0,
                lat: 80.0,
            },
            radius: 1000.0,
        };
        let hashes = circle_hashes(&circle, GEOHASH_MAX_LENGTH);
        assert!(hashes.is_ok());

        let circle2 = GeoRadius {
            center: GeoPoint {
                lon: 45.0,
                lat: 90.0,
            },
            radius: -1.0,
        };
        let hashes2 = circle_hashes(&circle2, GEOHASH_MAX_LENGTH);
        assert!(hashes2.is_err());
    }
}
