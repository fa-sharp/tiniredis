use bytes::Bytes;

use crate::storage::{
    geo::geo_utils::{coord_to_score, haversine_dist_meters, score_to_coord},
    sorted_set::RankedItem,
};

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
    fn geodist(&self, key: &Bytes, member1: &Bytes, member2: &Bytes) -> Result<Option<f64>>;
    fn geosearch(&self, key: &Bytes, from: (f64, f64), radius: f64) -> Result<Vec<Bytes>>;
}

impl GeoStorage for MemoryStorage {
    fn geoadd(&mut self, key: Bytes, members: Vec<((f64, f64), Bytes)>) -> Result<i64> {
        let members = members
            .into_iter()
            .map(|(coord, member)| (coord_to_score(coord) as f64, member))
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

    fn geodist(&self, key: &Bytes, member1: &Bytes, member2: &Bytes) -> Result<Option<f64>> {
        let Some(SortedSet(hash, _)) = self.get_sorted_set(key)? else {
            return Ok(None);
        };
        Ok(match (hash.get(member1), hash.get(member2)) {
            (Some(score1), Some(score2)) => {
                let origin = score_to_coord(*score1 as u64);
                let dest = score_to_coord(*score2 as u64);
                Some(haversine_dist_meters(origin, dest))
            }
            _ => None,
        })
    }

    fn geosearch(&self, key: &Bytes, from_coords: (f64, f64), radius: f64) -> Result<Vec<Bytes>> {
        let Some(SortedSet(_, ranked)) = self.get_sorted_set(key)? else {
            return Ok(Vec::new());
        };
        let is_within_radius = |location: &&RankedItem| -> bool {
            haversine_dist_meters(from_coords, score_to_coord(location.score as u64)) < radius
        };
        let members_within_radius = ranked
            .iter()
            .filter(is_within_radius)
            .map(|item| item.member.clone())
            .collect();

        Ok(members_within_radius)
    }
}
