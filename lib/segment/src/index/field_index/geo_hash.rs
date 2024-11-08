use std::ops::{Index, Range};

use geo::{Coord, Distance, Haversine, Intersects, LineString, Point, Polygon};
use geohash::{decode, decode_bbox, encode, Direction, GeohashError};
use itertools::Itertools;
use smol_str::SmolStr;

use crate::common::operation_error::{OperationError, OperationResult};
use crate::types::{GeoBoundingBox, GeoPoint, GeoPolygon, GeoRadius};

/// Packed representation of a geohash string.
///
/// Geohash string is a base32 encoded string.
/// It means that each character can be represented with 5 bits.
/// Also, the length of the string is encoded as 4 bits (because max size is `GEOHASH_MAX_LENGTH = 12`).
/// So, the packed representation is 64 bits long: 5bits * 12chars + 4bits = 64 bits.
///
/// Characters are stored in reverse order to keep lexicographical order.
/// Length is stored in the last 4 bits.
/// Unused bits are set to 0.
///
/// Example for `dr5ruj447`:
///
/// ```text
/// Bit no.     63                                                                    4  3  0
///             |                                                                     |  |  |
/// Bit value   01100 10111 00101 10111 11010 10001 00100 00100 00111 00000 00000 00000  1001
/// Decoded     'd'   'r'   '5'   'r'   'u'   'j'   '4'   '4'   '7'                      9
/// Meaning     s[0]  s[1]  s[2]  s[3]  s[4]  s[5]  s[6]  s[7]  s[8]  s[9]  s[10] s[11]  length
/// ```
#[repr(C)]
#[derive(Default, Clone, Copy, Debug, PartialEq, Hash, Ord, PartialOrd, Eq)]
pub struct GeoHash {
    packed: u64,
}

// code from geohash crate
// the alphabet for the base32 encoding used in geohashing
#[rustfmt::skip]
const BASE32_CODES: [char; 32] = [
    '0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
    'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
    's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

/// Max size of geo-hash used for indexing. size=12 is about 6cm2
pub const GEOHASH_MAX_LENGTH: usize = 12;

const LON_RANGE: Range<f64> = -180.0..180.0;
const LAT_RANGE: Range<f64> = -90.0..90.0;
const COORD_EPS: f64 = 1e-12;

impl Index<usize> for GeoHash {
    type Output = char;

    fn index(&self, i: usize) -> &Self::Output {
        assert!(i < self.len());
        let index = (self.packed >> Self::shift_value(i)) & 0b11111;
        &BASE32_CODES[index as usize]
    }
}

impl TryFrom<SmolStr> for GeoHash {
    type Error = GeohashError;

    fn try_from(hash: SmolStr) -> Result<Self, Self::Error> {
        Self::new(hash.as_str())
    }
}

impl TryFrom<String> for GeoHash {
    type Error = GeohashError;

    fn try_from(hash: String) -> Result<Self, Self::Error> {
        Self::new(hash.as_str())
    }
}

impl From<GeoHash> for SmolStr {
    fn from(hash: GeoHash) -> Self {
        hash.iter().collect()
    }
}

pub struct GeoHashIterator {
    packed_chars: u64,
}

impl Iterator for GeoHashIterator {
    type Item = char;

    fn next(&mut self) -> Option<Self::Item> {
        let len = self.packed_chars & 0b1111;
        if len > 0 {
            // take first character from the packed value
            let char_index = (self.packed_chars >> 59) & 0b11111;

            // shift packed value to the left to get the next character
            self.packed_chars = (self.packed_chars << 5) | (len - 1);

            // get character from the base32 alphabet
            Some(BASE32_CODES[char_index as usize])
        } else {
            None
        }
    }
}

impl GeoHash {
    pub fn new(s: &str) -> Result<Self, GeohashError> {
        if s.len() > GEOHASH_MAX_LENGTH {
            return Err(GeohashError::InvalidLength(s.len()));
        }
        let mut packed: u64 = 0;
        for (i, c) in s.chars().enumerate() {
            let index = BASE32_CODES.iter().position(|&x| x == c).unwrap() as u64;
            packed |= index << Self::shift_value(i);
        }
        packed |= s.len() as u64;
        Ok(Self { packed })
    }

    pub fn iter(&self) -> GeoHashIterator {
        if !self.is_empty() {
            GeoHashIterator {
                packed_chars: self.packed,
            }
        } else {
            GeoHashIterator { packed_chars: 0 }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        (self.packed & 0b1111) as usize
    }

    pub fn truncate(&self, new_len: usize) -> Self {
        assert!(new_len <= self.len());
        if new_len == self.len() {
            return *self;
        }
        if new_len == 0 {
            return Self { packed: 0 };
        }

        let mut packed = self.packed;
        // Clear all bits after `new_len`-th character and clear length bits
        let shift = Self::shift_value(new_len - 1);
        packed = (packed >> shift) << shift;
        packed |= new_len as u64; // set new length
        Self { packed }
    }

    pub fn starts_with(&self, other: GeoHash) -> bool {
        if self.len() < other.len() {
            // other is longer than self
            return false;
        }
        if other.is_empty() {
            // empty string is a prefix of any string
            return true;
        }

        let self_shifted = self.packed >> Self::shift_value(other.len() - 1);
        let other_shifted = other.packed >> Self::shift_value(other.len() - 1);
        self_shifted == other_shifted
    }

    // Returns the shift value. If we apply this shift to the packed value, we get the value of the `i`-th character.
    fn shift_value(i: usize) -> usize {
        assert!(i < GEOHASH_MAX_LENGTH);
        // first 4 bits is size, then 5 bits per character in reverse order (for lexicographical order)
        5 * (GEOHASH_MAX_LENGTH - 1 - i) + 4
    }
}

impl From<GeoPoint> for Coord<f64> {
    fn from(point: GeoPoint) -> Self {
        Self {
            x: point.lat,
            y: point.lon,
        }
    }
}

pub fn common_hash_prefix(geo_hashes: &[GeoHash]) -> Option<GeoHash> {
    if geo_hashes.is_empty() {
        return None;
    }
    let first = &geo_hashes[0];
    let mut prefix: usize = first.len();
    for geo_hash in geo_hashes.iter().skip(1) {
        for i in 0..prefix {
            if first[i] != geo_hash[i] {
                prefix = i;
                break;
            }
        }
    }
    Some(first.truncate(prefix))
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
fn sphere_neighbor(hash: GeoHash, direction: Direction) -> Result<GeoHash, GeohashError> {
    let hash_str = SmolStr::from(hash);
    let (coord, lon_err, lat_err) = decode(hash_str.as_str())?;
    let (dlat, dlng) = direction.to_tuple();
    let lon = sphere_lon(coord.x + 2f64 * lon_err.abs() * dlng);
    let lat = sphere_lat(coord.y + 2f64 * lat_err.abs() * dlat);

    let neighbor_coord = Coord { x: lon, y: lat };
    let encoded_string = encode(neighbor_coord, hash_str.len())?;
    GeoHash::try_from(encoded_string)
}

pub fn encode_max_precision(lon: f64, lat: f64) -> Result<GeoHash, GeohashError> {
    let encoded_string = encode((lon, lat).into(), GEOHASH_MAX_LENGTH)?;
    GeoHash::try_from(encoded_string)
}

pub fn geo_hash_to_box(geo_hash: GeoHash) -> GeoBoundingBox {
    let rectangle = decode_bbox(SmolStr::from(geo_hash).as_str()).unwrap();
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

        let mut from_row: GeoHash = self.north_west.truncate(precision);
        let mut to_row: GeoHash = self.north_east.truncate(precision);

        let to_column = self.south_west.truncate(precision);

        loop {
            let mut current = from_row;
            loop {
                seen.push(current);

                if seen.len() > max_regions {
                    return None;
                }

                if current == to_row {
                    break;
                }
                current = sphere_neighbor(current, Direction::E).unwrap();
            }
            if from_row == to_column {
                break;
            }

            from_row = sphere_neighbor(from_row, Direction::S).unwrap();
            to_row = sphere_neighbor(to_row, Direction::S).unwrap();
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
    let half_diagonal = Haversine::distance(bbox_center, Point(c0));

    half_diagonal + circle.radius
        > Haversine::distance(
            bbox_center,
            Point::new(circle.center.lon, circle.center.lat),
        )
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
                    .filter(|hash| check_circle_intersection(SmolStr::from(*hash).as_str(), circle))
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
                    .filter(|hash| {
                        check_polygon_intersection(SmolStr::from(*hash).as_str(), &polygon)
                    })
                    .collect_vec()
            })
    };
    create_hashes(mapping_fn)
}

/// A function used for cardinality estimation.
///
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
                    .filter(|hash| {
                        check_polygon_intersection(SmolStr::from(*hash).as_str(), &polygon_wrapper)
                    })
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
    fn geohash_ordering() {
        let mut v = vec![
            "dr5ru",
            "uft56",
            "hhbcd",
            "uft560000000",
            "h",
            "hbcd",
            "887hh1234567",
            "",
            "hwx98",
            "hbc",
            "dr5rukz",
        ];
        let mut hashes = v.iter().map(|s| GeoHash::new(s).unwrap()).collect_vec();
        hashes.sort_unstable();
        v.sort_unstable();
        assert_eq!(hashes.iter().map(|h| SmolStr::from(*h)).collect_vec(), v);

        // special case for hash which ends with 0
        // "uft56" and "uft560000000" have the same encoded chars, but different length
        assert_eq!(
            GeoHash::new("uft5600")
                .unwrap()
                .cmp(&GeoHash::new("uft560000000").unwrap()),
            "uft5600".cmp("uft560000000"),
        );
        assert_eq!(
            GeoHash::new("")
                .unwrap()
                .cmp(&GeoHash::new("000000000000").unwrap()),
            "".cmp("000000000000"),
        );
    }

    #[test]
    fn geohash_starts_with() {
        let samples = [
            "",
            "uft5601",
            "uft560100000",
            "uft56010000r",
            "uft5602",
            "uft560200000",
        ];
        for a_str in samples.iter() {
            let a_hash = GeoHash::new(a_str).unwrap();
            for b_str in samples.iter() {
                let b_hash = GeoHash::new(b_str).unwrap();
                if a_str.starts_with(b_str) {
                    assert!(
                        a_hash.starts_with(b_hash),
                        "{a_str:?} expected to start with {b_str:?}",
                    );
                } else {
                    assert!(
                        !a_hash.starts_with(b_hash),
                        "{a_str:?} expected to not start with {b_str:?}",
                    );
                }
            }
        }
    }

    #[test]
    fn geohash_encode_longitude_first() {
        let center_hash =
            GeoHash::new(&encode((NYC.lon, NYC.lat).into(), GEOHASH_MAX_LENGTH).unwrap());
        assert_eq!(center_hash.ok(), GeoHash::new("dr5ru7c02wnv").ok());
        let center_hash = GeoHash::new(&encode((NYC.lon, NYC.lat).into(), 6).unwrap());
        assert_eq!(center_hash.ok(), GeoHash::new("dr5ru7").ok());
        let center_hash =
            GeoHash::new(&encode((BERLIN.lon, BERLIN.lat).into(), GEOHASH_MAX_LENGTH).unwrap());
        assert_eq!(center_hash.ok(), GeoHash::new("u33dc1v0xupz").ok());
        let center_hash = GeoHash::new(&encode((BERLIN.lon, BERLIN.lat).into(), 6).unwrap());
        assert_eq!(center_hash.ok(), GeoHash::new("u33dc1").ok());
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
        assert_eq!(rectangle.north_west, GeoHash::new("dr5ruj4477kd").unwrap());
        assert_eq!(rectangle.south_west, GeoHash::new("dr5ru46ne2ux").unwrap());
        assert_eq!(rectangle.south_east, GeoHash::new("dr5ru6ryw0cp").unwrap());
        assert_eq!(rectangle.north_east, GeoHash::new("dr5rumpfq534").unwrap());
    }

    #[test]
    fn top_level_rectangle_geo_area() {
        let rect = GeohashBoundingBox {
            north_west: GeoHash::new("u").unwrap(),
            south_west: GeoHash::new("s").unwrap(),
            south_east: GeoHash::new("t").unwrap(),
            north_east: GeoHash::new("v").unwrap(),
        };
        let mut geo_area = rect.geohash_regions(1, 100).unwrap();
        let mut expected = vec![
            GeoHash::new("u").unwrap(),
            GeoHash::new("s").unwrap(),
            GeoHash::new("v").unwrap(),
            GeoHash::new("t").unwrap(),
        ];

        geo_area.sort_unstable();
        expected.sort_unstable();
        assert_eq!(geo_area, expected);
    }

    #[test]
    fn nyc_rectangle_geo_area_high_precision() {
        let rect = GeohashBoundingBox {
            north_west: GeoHash::new("dr5ruj4477kd").unwrap(),
            south_west: GeoHash::new("dr5ru46ne2ux").unwrap(),
            south_east: GeoHash::new("dr5ru6ryw0cp").unwrap(),
            north_east: GeoHash::new("dr5rumpfq534").unwrap(),
        };

        // calling `rect.geohash_regions()` is too expensive
        assert!(rect.geohash_regions(12, 100).is_none());
    }

    #[test]
    fn nyc_rectangle_geo_area_medium_precision() {
        let rect = GeohashBoundingBox {
            north_west: GeoHash::new("dr5ruj4").unwrap(),
            south_west: GeoHash::new("dr5ru46").unwrap(),
            south_east: GeoHash::new("dr5ru6r").unwrap(),
            north_east: GeoHash::new("dr5rump").unwrap(),
        };

        let geo_area = rect.geohash_regions(7, 1000).unwrap();
        assert_eq!(14 * 12, geo_area.len());
    }

    #[test]
    fn nyc_rectangle_geo_area_low_precision() {
        let rect = GeohashBoundingBox {
            north_west: GeoHash::new("dr5ruj").unwrap(),
            south_west: GeoHash::new("dr5ru4").unwrap(),
            south_east: GeoHash::new("dr5ru6").unwrap(),
            north_east: GeoHash::new("dr5rum").unwrap(),
        };

        let mut geo_area = rect.geohash_regions(6, 100).unwrap();
        let mut expected = vec![
            GeoHash::new("dr5ru4").unwrap(),
            GeoHash::new("dr5ru5").unwrap(),
            GeoHash::new("dr5ru6").unwrap(),
            GeoHash::new("dr5ru7").unwrap(),
            GeoHash::new("dr5ruh").unwrap(),
            GeoHash::new("dr5ruj").unwrap(),
            GeoHash::new("dr5rum").unwrap(),
            GeoHash::new("dr5ruk").unwrap(),
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
            GeoHash::new("dr5ruj").unwrap(),
            GeoHash::new("dr5ruh").unwrap(),
            GeoHash::new("dr5ru5").unwrap(),
            GeoHash::new("dr5ru4").unwrap(),
            GeoHash::new("dr5rum").unwrap(),
            GeoHash::new("dr5ruk").unwrap(),
            GeoHash::new("dr5ru7").unwrap(),
            GeoHash::new("dr5ru6").unwrap(),
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
        assert_eq!(nyc_hashes_result.unwrap(), [GeoHash::new("dr5ru").unwrap()]);
    }

    #[test]
    fn rectangle_hashes_crossing_antimeridian() {
        // conversion to lon/lat http://geohash.co/
        // "ztnv2hjxn03k"
        let top_left = GeoPoint {
            lat: 74.071028,
            lon: 167.0,
        };

        // "dr5ru7c02wnv"
        let bottom_right = GeoPoint {
            lat: 40.75798,
            lon: -73.991516,
        };

        let crossing_usa_rectangle = GeoBoundingBox {
            top_left,
            bottom_right,
        };

        let usa_hashes_result = rectangle_hashes(&crossing_usa_rectangle, 200);
        let usa_hashes = usa_hashes_result.unwrap();
        assert_eq!(usa_hashes.len(), 84);
        assert!(usa_hashes.iter().all(|h| h.len() == 2)); // low geohash precision

        let mut usa_hashes_result = rectangle_hashes(&crossing_usa_rectangle, 10);
        usa_hashes_result.as_mut().unwrap().sort_unstable();
        let mut expected = vec![
            GeoHash::new("8").unwrap(),
            GeoHash::new("9").unwrap(),
            GeoHash::new("b").unwrap(),
            GeoHash::new("c").unwrap(),
            GeoHash::new("d").unwrap(),
            GeoHash::new("f").unwrap(),
            GeoHash::new("x").unwrap(),
            GeoHash::new("z").unwrap(),
        ];
        expected.sort_unstable();

        assert_eq!(usa_hashes_result.unwrap(), expected);

        // Graphical proof using https://www.movable-type.co.uk/scripts/geohash.html

        // n p 0 1 4 5
        // y z b c f g
        // w x 8 9 d e
        // q r 2 3 6 7

        // - - - - - -
        // | z b c f |
        // | x 8 9 d |
        // - - - - - -
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
            GeoHash::new("dr5ruj").unwrap(),
            GeoHash::new("dr5ruh").unwrap(),
            GeoHash::new("dr5ru5").unwrap(),
            GeoHash::new("dr5ru4").unwrap(),
            GeoHash::new("dr5rum").unwrap(),
            GeoHash::new("dr5ruk").unwrap(),
            GeoHash::new("dr5ru7").unwrap(),
            GeoHash::new("dr5ru6").unwrap(),
        ];
        expected.sort_unstable();

        assert_eq!(nyc_hashes_result.unwrap(), expected);

        // falls back to finest region that encompasses the whole area
        let nyc_hashes_result = polygon_hashes(&near_nyc_polygon, 7);
        assert_eq!(nyc_hashes_result.unwrap(), [GeoHash::new("dr5ru").unwrap()]);
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
        assert_eq!(
            hashes.unwrap(),
            vec![
                GeoHash::new("zbp").unwrap(),
                GeoHash::new("b00").unwrap(),
                GeoHash::new("xzz").unwrap(),
                GeoHash::new("8pb").unwrap(),
            ]
        );
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
        assert_eq!(
            vec,
            [
                GeoHash::new("b").unwrap(),
                GeoHash::new("c").unwrap(),
                GeoHash::new("f").unwrap(),
                GeoHash::new("g").unwrap(),
                GeoHash::new("u").unwrap(),
                GeoHash::new("v").unwrap(),
                GeoHash::new("y").unwrap(),
                GeoHash::new("z").unwrap(),
            ]
        );
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
        assert_eq!(
            hashes,
            [
                GeoHash::new("fz").unwrap(),
                GeoHash::new("gp").unwrap(),
                GeoHash::new("gr").unwrap(),
                GeoHash::new("gx").unwrap(),
                GeoHash::new("gz").unwrap(),
                GeoHash::new("up").unwrap(),
            ]
        );
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
        assert_eq!(
            hashes,
            [
                GeoHash::new("p6yd").unwrap(),
                GeoHash::new("p6yf").unwrap(),
                GeoHash::new("p6y9").unwrap(),
                GeoHash::new("p6yc").unwrap(),
            ]
        );
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
        assert_eq!(
            hashes,
            [
                GeoHash::new("p6ycc").unwrap(),
                GeoHash::new("p6ycf").unwrap(),
                GeoHash::new("p6ycg").unwrap(),
            ]
        );
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
            GeoHash::new("dr5ruj").unwrap(),
            GeoHash::new("dr5ruh").unwrap(),
            GeoHash::new("dr5ru5").unwrap(),
            GeoHash::new("dr5ru4").unwrap(),
            GeoHash::new("dr5rum").unwrap(),
            GeoHash::new("dr5ruk").unwrap(),
            GeoHash::new("dr5ru7").unwrap(),
            GeoHash::new("dr5ru6").unwrap(),
        ];
        expected.sort_unstable();
        assert_eq!(nyc_hashes_result.unwrap(), expected);

        // falls back to finest region that encompasses the whole area
        let nyc_hashes_result = circle_hashes(&near_nyc_circle, 7);
        assert_eq!(nyc_hashes_result.unwrap(), [GeoHash::new("dr5ru").unwrap()]);
    }

    #[test]
    fn go_north() {
        let mut geohash = sphere_neighbor(GeoHash::new("ww8p").unwrap(), Direction::N).unwrap();
        for _ in 0..1000 {
            geohash = sphere_neighbor(geohash, Direction::N).unwrap();
        }
    }

    #[test]
    fn go_west() {
        let starting_hash = GeoHash::new("ww8").unwrap();
        let mut geohash = sphere_neighbor(starting_hash, Direction::W).unwrap();
        let mut is_earth_round = false;
        for _ in 0..1000 {
            geohash = sphere_neighbor(geohash, Direction::W).unwrap();
            if geohash == starting_hash {
                is_earth_round = true;
            }
        }
        assert!(is_earth_round)
    }

    #[test]
    fn sphere_neighbor_corner_cases() {
        assert_eq!(
            &SmolStr::from(sphere_neighbor(GeoHash::new("z").unwrap(), Direction::NE).unwrap()),
            "b"
        );
        assert_eq!(
            &SmolStr::from(sphere_neighbor(GeoHash::new("zz").unwrap(), Direction::NE).unwrap()),
            "bp"
        );
        assert_eq!(
            &SmolStr::from(sphere_neighbor(GeoHash::new("0").unwrap(), Direction::SW).unwrap()),
            "p"
        );
        assert_eq!(
            &SmolStr::from(sphere_neighbor(GeoHash::new("00").unwrap(), Direction::SW).unwrap()),
            "pb"
        );

        assert_eq!(
            &SmolStr::from(sphere_neighbor(GeoHash::new("8").unwrap(), Direction::W).unwrap()),
            "x"
        );
        assert_eq!(
            &SmolStr::from(sphere_neighbor(GeoHash::new("8h").unwrap(), Direction::W).unwrap()),
            "xu"
        );
        assert_eq!(
            &SmolStr::from(sphere_neighbor(GeoHash::new("r").unwrap(), Direction::E).unwrap()),
            "2"
        );
        assert_eq!(
            &SmolStr::from(sphere_neighbor(GeoHash::new("ru").unwrap(), Direction::E).unwrap()),
            "2h"
        );

        assert_eq!(
            SmolStr::from(
                sphere_neighbor(GeoHash::new("ww8p1r4t8").unwrap(), Direction::SE).unwrap()
            ),
            SmolStr::from(&geohash::neighbor("ww8p1r4t8", Direction::SE).unwrap())
        );
    }

    #[test]
    fn long_overflow_distance() {
        let dist = Haversine::distance(Point::new(-179.999, 66.0), Point::new(179.999, 66.0));
        eprintln!("dist` = {dist:#?}");
        assert_eq!(dist, 90.45422731917998);
        let dist = Haversine::distance(Point::new(0.99, 90.), Point::new(0.99, -90.0));
        assert_eq!(dist, 20015114.442035925);
    }

    #[test]
    fn turn_geo_hash_to_box() {
        let geo_box = geo_hash_to_box(GeoHash::new("dr5ruj4477kd").unwrap());
        let center = GeoPoint {
            lat: 40.76517460,
            lon: -74.00101399,
        };
        assert!(geo_box.check_point(&center));
    }

    #[test]
    fn common_prefix() {
        let geo_hashes = vec![
            GeoHash::new("zbcd123").unwrap(),
            GeoHash::new("zbcd2233").unwrap(),
            GeoHash::new("zbcd3213").unwrap(),
            GeoHash::new("zbcd533").unwrap(),
        ];

        let common_prefix = common_hash_prefix(&geo_hashes).unwrap();
        println!("common_prefix = {:?}", SmolStr::from(common_prefix));

        //assert_eq!(common_prefix, GeoHash::new("zbcd").unwrap());

        let geo_hashes = vec![
            GeoHash::new("zbcd123").unwrap(),
            GeoHash::new("bbcd2233").unwrap(),
            GeoHash::new("cbcd3213").unwrap(),
            GeoHash::new("dbcd533").unwrap(),
        ];

        let common_prefix = common_hash_prefix(&geo_hashes).unwrap();
        println!("common_prefix = {:?}", SmolStr::from(common_prefix));

        assert_eq!(common_prefix, GeoHash::new("").unwrap());
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
            top_left,
            bottom_right,
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
