use bytes::Bytes;

use crate::storage::geo::geo_utils::score_to_coord;

use super::{
    sorted_set::{SortedSet, SortedSetStorage},
    MemoryStorage, StorageResult as Result,
};

mod geo_utils;
pub use geo_utils::{validate_lat, validate_lon};

/// Geo interface
pub trait GeoStorage {
    fn geoadd(&mut self, key: Bytes, members: Vec<((f64, f64), Bytes)>) -> Result<i64>;
    fn geopos(&self, key: &Bytes, members: Vec<Bytes>) -> Result<Vec<Option<(f64, f64)>>>;
}

impl GeoStorage for MemoryStorage {
    fn geoadd(&mut self, key: Bytes, members: Vec<((f64, f64), Bytes)>) -> Result<i64> {
        let members = members
            .into_iter()
            .map(|(coord, member)| (geo_utils::coord_to_score(coord) as f64, member))
            .collect();

        self.zadd(key, members)
    }

    fn geopos(&self, key: &Bytes, members: Vec<Bytes>) -> Result<Vec<Option<(f64, f64)>>> {
        let Some(SortedSet(hash, _)) = self.get_sorted_set(key)? else {
            return Ok(vec![None; members.len()]);
        };
        let member_coords = members
            .iter()
            .map(|member| hash.get(member).map(|score| score_to_coord(*score as u64)))
            .collect();

        Ok(member_coords)
    }
}
