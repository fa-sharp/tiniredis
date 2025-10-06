use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    time::Duration,
};

use bytes::Bytes;
use tokio::time::Instant;

mod base;
pub use base::*;
pub mod geo;
pub mod list;
pub mod rdb;
pub mod set;
pub mod sorted_set;
pub mod stream;

/// Common result type for some storage operations
pub type StorageResult<T> = Result<T, Bytes>;

/// Memory storage implementation using a HashMap
#[derive(Debug, Default)]
pub struct MemoryStorage {
    data: HashMap<Bytes, RedisObject>,
}

/// Redis object stored in memory
#[derive(Debug)]
pub struct RedisObject {
    expiration: Option<Instant>,
    data: RedisDataType,
}

/// Contains the data of the object stored in memory
#[derive(Debug, PartialEq)]
pub enum RedisDataType {
    String(Bytes),
    List(VecDeque<Bytes>),
    Stream(BTreeMap<stream::StreamId, Vec<(Bytes, Bytes)>>),
    Set(HashSet<Bytes>),
    SortedSet(sorted_set::SortedSet),
}

impl MemoryStorage {
    /// Get a reference for the object data at the given key. Will return `None` if missing or expired.
    fn get(&self, key: &Bytes) -> Option<&RedisDataType> {
        self.data
            .get(key)
            .and_then(|o| o.is_current().then_some(&o.data))
    }

    /// Get a mutable reference for the object data at the given key. Will return `None` if missing or expired.
    fn get_mut(&mut self, key: &Bytes) -> Option<&mut RedisDataType> {
        self.data
            .get_mut(key)
            .and_then(|o| o.is_current().then_some(&mut o.data))
    }

    /// Get a mutable reference for the object at the given key. If there was no entry or it was expired,
    /// use the provided default function to initialize it.
    fn get_entry_with_default<F>(&mut self, key: Bytes, default_fn: F) -> &mut RedisObject
    where
        F: Fn() -> RedisObject,
    {
        let entry = self
            .data
            .entry(key)
            .and_modify(|o| {
                if !o.is_current() {
                    std::mem::swap(o, &mut default_fn())
                }
            })
            .or_insert_with(default_fn);
        entry
    }
}

impl RedisObject {
    pub fn new(data: RedisDataType) -> Self {
        Self {
            expiration: None,
            data,
        }
    }

    pub fn new_list() -> Self {
        Self::new(RedisDataType::List(VecDeque::new()))
    }

    pub fn new_set() -> Self {
        Self::new(RedisDataType::Set(HashSet::new()))
    }

    pub fn new_stream() -> Self {
        Self::new(RedisDataType::Stream(BTreeMap::new()))
    }

    pub fn new_with_ttl(data: RedisDataType, ttl_millis: Option<u64>) -> Self {
        Self {
            expiration: ttl_millis.map(|ttl| (Instant::now() + Duration::from_millis(ttl))),
            data,
        }
    }

    fn is_current(&self) -> bool {
        if let Some(expiration) = self.expiration {
            Instant::now() <= expiration
        } else {
            true
        }
    }

    fn is_persist_supported(&self) -> bool {
        matches!(
            self.data,
            RedisDataType::String(_) | RedisDataType::List(_) | RedisDataType::Set(_)
        )
    }
}
