//! Geo conversion utilies

const MIN_LATITUDE: f64 = -85.05112878;
const MAX_LATITUDE: f64 = 85.05112878;
const MIN_LONGITUDE: f64 = -180.0;
const MAX_LONGITUDE: f64 = 180.0;

const LATITUDE_RANGE: f64 = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE: f64 = MAX_LONGITUDE - MIN_LONGITUDE;

const NORMALIZE: f64 = 67_108_864.0 as f64; // 2^26

pub fn validate_lon(lon: f64) -> bool {
    MIN_LONGITUDE <= lon && lon <= MAX_LONGITUDE
}

pub fn validate_lat(lat: f64) -> bool {
    MIN_LATITUDE <= lat && lat <= MAX_LATITUDE
}

pub fn coord_to_score((lon, lat): (f64, f64)) -> u64 {
    // normalize and truncate
    let normalized_lon = (NORMALIZE * (lon - MIN_LONGITUDE) / LONGITUDE_RANGE) as u32;
    let normalized_lat = (NORMALIZE * (lat - MIN_LATITUDE) / LATITUDE_RANGE) as u32;

    // interleave: spread both ints to u64
    let x_lon = spread_u32_to_u64(normalized_lon);
    let y_lat = spread_u32_to_u64(normalized_lat);

    // shift x value 1 bit to the left
    let x_shifted = x_lon << 1;

    // combine x and y with bitwise OR
    y_lat | x_shifted
}

pub fn score_to_coord(score: u64) -> (f64, f64) {
    // Extract longitude (shifted) and latitude bits
    let x_lon = score >> 1;
    let y_lat = score;

    // Compact to u32 to get grid cell
    let grid_lon = compact_u64_to_u32(x_lon) as f64;
    let grid_lat = compact_u64_to_u32(y_lat) as f64;

    // Calculate grid boundaries
    let grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (grid_lon / NORMALIZE);
    let grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_lon + 1.0) / NORMALIZE);
    let grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (grid_lat / NORMALIZE);
    let grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((grid_lat + 1.0) / NORMALIZE);

    // Calculate center point of grid cell
    let lon = (grid_longitude_min + grid_longitude_max) / 2.0;
    let lat = (grid_latitude_min + grid_latitude_max) / 2.0;

    (lon, lat)
}

/// Spreads a 32-bit integer to a 64-bit integer by inserting
/// 32 zero bits in-between.
///
/// Before spread: `x1  x2  ...  x31  x32`
///
/// After spread:  `0   x1  ...   0   x16  ... 0  x31  0  x32`
fn spread_u32_to_u64(v: u32) -> u64 {
    //  Ensure only lower 32 bits are non-zero, and cast to 64.
    let mut v: u64 = (v & 0xFFFFFFFF).into();

    // # Bitwise operations to spread 32 bits into 64 bits with zeros in-between
    v = (v | (v << 16)) & 0x0000FFFF0000FFFF;
    v = (v | (v << 8)) & 0x00FF00FF00FF00FF;
    v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F;
    v = (v | (v << 2)) & 0x3333333333333333;
    v = (v | (v << 1)) & 0x5555555555555555;

    v
}

fn compact_u64_to_u32(v: u64) -> u32 {
    // # Keep only the bits in even positions
    let mut v = v & 0x5555555555555555;

    // # Before masking: w1   v1  ...   w2   v16  ... w31  v31  w32  v32
    // # After masking: 0   v1  ...   0   v16  ... 0  v31  0  v32

    // # Where w1, w2,..w31 are the digits from longitude if we're compacting latitude, or digits from latitude if we're compacting longitude
    // # So, we mask them out and only keep the relevant bits that we wish to compact

    // # ------
    // # Reverse the spreading process by shifting and masking
    v = (v | (v >> 1)) & 0x3333333333333333;
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F;
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF;
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF;
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF;

    v as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score() {
        let paris_coord = (2.3488, 48.8534);
        let paris_score = 3663832752681684;
        assert_eq!(coord_to_score(paris_coord), paris_score);

        let bangkok_coord = (100.5252, 13.7220);
        let bangkok_score = 3962257306574459;
        assert_eq!(coord_to_score(bangkok_coord), bangkok_score);

        let ny_coord = (-74.0060, 40.7128);
        let ny_score = 1791873974549446;
        assert_eq!(coord_to_score(ny_coord), ny_score);
    }
}
