use bytes::Bytes;

use super::{sorted_set::SortedSetStorage, MemoryStorage, StorageResult as Result};

mod geo_utils;
pub use geo_utils::{validate_lat, validate_lon};

/// Geo interface
pub trait GeoStorage {
    fn geoadd(&mut self, key: Bytes, members: Vec<((f64, f64), Bytes)>) -> Result<i64>;
}

impl GeoStorage for MemoryStorage {
    fn geoadd(&mut self, key: Bytes, members: Vec<((f64, f64), Bytes)>) -> Result<i64> {
        let members = members
            .into_iter()
            .map(|(coord, member)| (geo_utils::coord_to_score(coord) as f64, member))
            .collect();
        self.zadd(key, members)
    }
}
