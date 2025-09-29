use std::collections::{BTreeMap, VecDeque};

use bytes::Bytes;
use tokio::time::Instant;

/// Redis object stored in memory
#[derive(Debug)]
pub struct RedisObject {
    /// Instant when object was created
    created: Instant,
    /// TTL in milliseconds
    ttl_millis: Option<u64>,
    /// The data of the object
    data: RedisDataType,
}

/// Contains the data of the object stored in memory
#[derive(Debug)]
pub enum RedisDataType {
    String(Bytes),
    List(VecDeque<Bytes>),
}

pub trait Storage {
    fn get(&self, key: &Bytes) -> Option<Bytes>;
    fn set(&mut self, key: Bytes, val: Bytes, ttl_millis: Option<u64>);
    fn rpush(&mut self, key: Bytes, elem: Bytes, elems: Vec<Bytes>) -> Result<i64, Bytes>;
    fn lrange(&self, key: Bytes, start: i64, stop: i64) -> Vec<Bytes>;
    fn cleanup(&mut self);
}

#[derive(Default)]
pub struct MemoryStorage {
    data: BTreeMap<Bytes, RedisObject>,
}

impl Storage for MemoryStorage {
    fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.data
            .get(key)
            .filter(|o| o.is_current())
            .and_then(|o| match &o.data {
                RedisDataType::String(bytes) => Some(bytes.clone()),
                _ => None,
            })
    }

    fn set(&mut self, key: Bytes, val: Bytes, ttl_millis: Option<u64>) {
        let object = RedisObject::new_with_ttl(RedisDataType::String(val), ttl_millis);
        self.data.insert(key, object);
    }

    fn rpush(&mut self, key: Bytes, elem: Bytes, elems: Vec<Bytes>) -> Result<i64, Bytes> {
        let entry = self
            .data
            .entry(key)
            .and_modify(|o| {
                if !o.is_current() {
                    std::mem::swap(o, &mut RedisObject::new_list())
                }
            })
            .or_insert_with(|| RedisObject::new_list());
        if let RedisDataType::List(ref mut vec) = entry.data {
            vec.push_back(elem);
            vec.extend(elems);
            Ok(vec.len().try_into().unwrap_or_default())
        } else {
            Err(Bytes::from_static(b"Not a list"))
        }
    }

    fn lrange(&self, key: Bytes, start: i64, stop: i64) -> Vec<Bytes> {
        let Some(obj) = self.data.get(&key) else {
            return Vec::new();
        };
        let RedisDataType::List(ref list) = obj.data else {
            return Vec::new();
        };

        let beg: usize = if start < 0 {
            list.len()
                .checked_add_signed(start.try_into().unwrap_or_default())
                .unwrap_or_default()
        } else {
            start as usize
        };
        if beg >= list.len() {
            return Vec::new();
        }

        let mut end: usize = if stop < 0 {
            list.len()
                .checked_add_signed(stop.try_into().unwrap_or_default())
                .unwrap_or_default()
        } else {
            stop as usize
        };
        if end >= list.len() {
            end = list.len() - 1;
        }
        if beg > end {
            return Vec::new();
        }

        list.range(beg..=end).cloned().collect()
    }

    fn cleanup(&mut self) {
        self.data.retain(|_, o| o.is_current());
    }
}

impl RedisObject {
    pub fn new(data: RedisDataType) -> Self {
        Self {
            created: Instant::now(),
            ttl_millis: None,
            data,
        }
    }

    pub fn new_list() -> Self {
        Self::new(RedisDataType::new_list())
    }

    pub fn new_with_ttl(data: RedisDataType, ttl_millis: Option<u64>) -> Self {
        Self {
            created: Instant::now(),
            ttl_millis,
            data,
        }
    }

    fn is_current(&self) -> bool {
        if let Some(ttl) = self.ttl_millis {
            Instant::now().duration_since(self.created).as_millis() <= ttl.into()
        } else {
            true
        }
    }
}

impl RedisDataType {
    fn new_list() -> Self {
        Self::List(VecDeque::with_capacity(1))
    }
}
