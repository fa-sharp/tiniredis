use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap},
};

use bytes::Bytes;

use super::{MemoryStorage, RedisDataType, RedisObject, StorageResult as Result};

/// Sorted set storage:
/// - HashMap of `member -> score`
/// - BTreeSet of `{ member, score }` items ranked by score
#[derive(Debug, Default)]
pub struct SortedSet(
    pub(super) HashMap<Bytes, f64>,
    pub(super) BTreeSet<RankedItem>,
);

/// Ranked item stored in the BTreeSet
#[derive(Debug)]
pub struct RankedItem {
    pub(super) member: Bytes,
    pub(super) score: f64,
}

// Ordering by score then member
impl Ord for RankedItem {
    fn cmp(&self, other: &Self) -> Ordering {
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
// Equality by member only
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
    fn zrange(&self, key: &Bytes, start: i64, stop: i64) -> Result<Vec<Bytes>>;
    fn zcard(&self, key: &Bytes) -> Result<i64>;
    fn zscore(&self, key: &Bytes, member: &Bytes) -> Result<Option<f64>>;
    fn zrem(&mut self, key: &Bytes, member: Vec<Bytes>) -> Result<i64>;
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

        Ok(Some(rank.try_into().unwrap_or_default()))
    }

    fn zrange(&self, key: &Bytes, start: i64, stop: i64) -> Result<Vec<Bytes>> {
        let Some(SortedSet(_, ranked)) = self.get_sorted_set(key)? else {
            return Ok(Vec::new());
        };
        let length = ranked.len();

        let beg: usize = if start < 0 {
            length
                .checked_add_signed(start.try_into().unwrap_or_default())
                .unwrap_or_default()
        } else {
            start as usize
        };
        if beg >= length {
            return Ok(Vec::new());
        }

        let mut end: usize = if stop < 0 {
            length
                .checked_add_signed(stop.try_into().unwrap_or_default())
                .unwrap_or_default()
        } else {
            stop as usize
        };
        if end >= length {
            end = length - 1;
        }
        if beg > end {
            return Ok(Vec::new());
        }

        Ok(ranked
            .iter()
            .skip(beg)
            .take(end - beg + 1)
            .map(|item| item.member.clone())
            .collect())
    }

    fn zcard(&self, key: &Bytes) -> Result<i64> {
        Ok(match self.get_sorted_set(key)? {
            Some(SortedSet(hash, _)) => hash.len().try_into().unwrap_or_default(),
            None => 0,
        })
    }

    fn zscore(&self, key: &Bytes, member: &Bytes) -> Result<Option<f64>> {
        let Some(SortedSet(hash, _)) = self.get_sorted_set(key)? else {
            return Ok(None);
        };
        Ok(hash.get(member).copied())
    }

    fn zrem(&mut self, key: &Bytes, members: Vec<Bytes>) -> Result<i64> {
        let Some(SortedSet(hash, ranked)) = self.get_sorted_set_mut(&key)? else {
            return Ok(0);
        };

        let mut num_removed = 0;
        for member in members {
            if let Some((member, score)) = hash.remove_entry(&member) {
                ranked.remove(&RankedItem { member, score });
                num_removed += 1;
            }
        }
        if hash.is_empty() {
            self.data.remove(key);
        }

        Ok(num_removed)
    }
}

const NOT_SORTED_SET: Bytes = Bytes::from_static(b"Not a sorted set");
const MALFORMED: Bytes = Bytes::from_static(b"Sorted set data is malformed");

impl MemoryStorage {
    pub(super) fn get_sorted_set(&self, key: &Bytes) -> Result<Option<&SortedSet>> {
        let Some(data) = self.get(key) else {
            return Ok(None);
        };
        let RedisDataType::SortedSet(set) = data else {
            return Err(NOT_SORTED_SET);
        };
        if set.0.len() != set.1.len() {
            return Err(MALFORMED);
        }

        Ok(Some(set))
    }

    pub(super) fn get_sorted_set_mut(&mut self, key: &Bytes) -> Result<Option<&mut SortedSet>> {
        match self.get_mut(key) {
            Some(RedisDataType::SortedSet(set)) => {
                if set.0.len() != set.1.len() {
                    return Err(MALFORMED);
                }
                Ok(Some(set))
            }
            Some(_) => Err(NOT_SORTED_SET),
            None => Ok(None),
        }
    }

    pub(super) fn get_sorted_set_entry(&mut self, key: Bytes) -> Result<&mut SortedSet> {
        let entry = self.get_entry_with_default(key, RedisObject::new_sorted_set);
        let RedisDataType::SortedSet(ref mut set) = entry.data else {
            return Err(NOT_SORTED_SET);
        };
        if set.0.len() != set.1.len() {
            return Err(MALFORMED);
        }
        Ok(set)
    }
}

impl RedisObject {
    fn new_sorted_set() -> Self {
        Self::new(RedisDataType::SortedSet(SortedSet::default()))
    }
}
