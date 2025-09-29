use std::collections::BTreeMap;

use bytes::Bytes;

/// Redis data type stored in memory
#[derive(Debug)]
pub enum RedisDataType {
    String(Bytes),
}

pub trait Storage {
    fn get(&self, key: &Bytes) -> Option<Bytes>;
    fn set(&mut self, key: Bytes, val: Bytes);
}

#[derive(Default)]
pub struct MemoryStorage {
    data: BTreeMap<Bytes, RedisDataType>,
}

impl Storage for MemoryStorage {
    fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.data.get(key).map(|d| match d {
            RedisDataType::String(bytes) => bytes.clone(),
        })
    }

    fn set(&mut self, key: Bytes, val: Bytes) {
        self.data.insert(key, RedisDataType::String(val));
    }
}
