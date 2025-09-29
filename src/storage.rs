use std::collections::BTreeMap;

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
}

pub trait Storage {
    fn get(&self, key: &Bytes) -> Option<Bytes>;
    fn set(&mut self, key: Bytes, val: Bytes, ttl_millis: Option<u64>);
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
            .map(|o| match &o.data {
                RedisDataType::String(bytes) => bytes.clone(),
            })
    }

    fn set(&mut self, key: Bytes, val: Bytes, ttl_millis: Option<u64>) {
        let object = RedisObject {
            created: Instant::now(),
            ttl_millis,
            data: RedisDataType::String(val),
        };
        self.data.insert(key, object);
    }

    fn cleanup(&mut self) {
        self.data.retain(|_, o| o.is_current());
    }
}

impl RedisObject {
    fn is_current(&self) -> bool {
        if let Some(ttl) = self.ttl_millis {
            Instant::now().duration_since(self.created).as_millis() <= ttl.into()
        } else {
            true
        }
    }
}
