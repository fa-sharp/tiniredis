use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap},
};

use bytes::Bytes;
use tracing::debug;

use super::{MemoryStorage, RedisDataType, RedisObject, StorageResult as Result};

/// Sorted set storage:
/// - HashMap of `member -> score`
/// - BTreeSet of `{ member, score }` items ranked by score
#[derive(Debug, Default)]
pub struct SortedSet(HashMap<Bytes, f64>, BTreeSet<RankedItem>);

/// Ranked item stored in the BTreeSet
#[derive(Debug)]
pub struct RankedItem {
    member: Bytes,
    score: f64,
}

/// Margin for f64 equality
const F64_MARGIN: f64 = 1e-10;

// Ordering by score then member
impl Ord for RankedItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // ((self.score - F64_MARGIN)..(self.score + F64_MARGIN)).contains(&other.score)
        match self.score.total_cmp(&other.score) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => self.member.cmp(&other.member),
        }
    }
}
impl PartialOrd for RankedItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Eq for RankedItem {}
impl PartialEq for RankedItem {
    fn eq(&self, other: &Self) -> bool {
        self.member == other.member
    }
}

/// Sorted set interface
pub trait SortedSetStorage {
    fn zadd(&mut self, key: Bytes, members: Vec<(f64, Bytes)>) -> Result<i64>;
    fn zrank(&self, key: &Bytes, member: Bytes) -> Result<Option<i64>>;
}

impl SortedSetStorage for MemoryStorage {
    fn zadd(&mut self, key: Bytes, members: Vec<(f64, Bytes)>) -> Result<i64> {
        let SortedSet(hash, ranked) = self.get_sorted_set_entry(key)?;
        let mut num_added = 0;
        for (score, member) in members {
            match hash.insert(member.clone(), score) {
                None => {
                    // New member
                    ranked.insert(RankedItem { member, score });
                    num_added += 1;
                }
                Some(old_score) => {
                    // Update member in ranked tree
                    let old_rank = ranked
                        .take(&RankedItem {
                            member,
                            score: old_score,
                        })
                        .expect("existing member & score should be in ranked tree");
                    ranked.insert(RankedItem {
                        member: old_rank.member,
                        score,
                    });
                }
            }
        }

        debug!("{:?}", (hash, ranked));
        Ok(num_added)
    }

    fn zrank(&self, key: &Bytes, member: Bytes) -> Result<Option<i64>> {
        let Some(SortedSet(hash, ranked)) = self.get_sorted_set(key)? else {
            return Ok(None);
        };
        // get member score, and lowest ranked member
        let (Some(score), Some(lowest)) = (hash.get(&member), ranked.first()) else {
            return Ok(None);
        };
        // calculate rank by iterating from lowest member
        let rank = ranked
            .range(
                lowest..&RankedItem {
                    member,
                    score: *score,
                },
            )
            .count();

        debug!("{:?}", (hash, ranked));
        Ok(Some(rank.try_into().unwrap_or_default()))
    }
}

const NOT_SORTED_SET: Bytes = Bytes::from_static(b"Not a sorted set");

impl MemoryStorage {
    fn get_sorted_set(&self, key: &Bytes) -> Result<Option<&SortedSet>> {
        let Some(data) = self.get(key) else {
            return Ok(None);
        };
        let RedisDataType::SortedSet(set) = data else {
            return Err(NOT_SORTED_SET);
        };
        Ok(Some(set))
    }

    fn get_sorted_set_entry(&mut self, key: Bytes) -> Result<&mut SortedSet> {
        let entry = self.get_entry_with_default(key, RedisObject::new_sorted_set);
        let RedisDataType::SortedSet(ref mut set) = entry.data else {
            return Err(NOT_SORTED_SET);
        };
        Ok(set)
    }
}

impl RedisObject {
    fn new_sorted_set() -> Self {
        Self::new(RedisDataType::SortedSet(SortedSet::default()))
    }
}
