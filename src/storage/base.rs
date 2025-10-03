use bytes::Bytes;
use tokio::time::Instant;

use super::{MemoryStorage, RedisDataType, RedisObject, StorageResult};

/// Base storage interface
pub trait Storage {
    fn get(&self, key: &Bytes) -> Option<Bytes>;
    fn set(&mut self, key: Bytes, val: Bytes, ttl_millis: Option<u64>);
    fn ttl(&self, key: &Bytes) -> i64;
    fn kind(&self, key: &Bytes) -> Bytes;
    fn del(&mut self, key: &Bytes) -> bool;
    fn incr(&mut self, key: Bytes) -> StorageResult<i64>;
    fn size(&self) -> i64;
    fn flush(&mut self);
    fn cleanup_expired(&mut self) -> usize;
}

impl Storage for MemoryStorage {
    fn get(&self, key: &Bytes) -> Option<Bytes> {
        match self.get(key) {
            Some(RedisDataType::String(bytes)) => Some(bytes.clone()),
            _ => None,
        }
    }

    fn set(&mut self, key: Bytes, val: Bytes, ttl_millis: Option<u64>) {
        let object = RedisObject::new_with_ttl(RedisDataType::String(val), ttl_millis);
        self.data.insert(key, object);
    }

    fn kind(&self, key: &Bytes) -> Bytes {
        match self.get(key) {
            Some(data_type) => match data_type {
                RedisDataType::String(_) => Bytes::from_static(b"string"),
                RedisDataType::List(_) => Bytes::from_static(b"list"),
                RedisDataType::Stream(_) => Bytes::from_static(b"stream"),
                RedisDataType::Set(_) => Bytes::from_static(b"set"),
                RedisDataType::SortedSet(_) => Bytes::from_static(b"zset"),
            },
            None => Bytes::from_static(b"none"),
        }
    }

    fn ttl(&self, key: &Bytes) -> i64 {
        match self.data.get(key).filter(|o| o.is_current()) {
            Some(obj) => match obj.ttl_millis {
                Some(ttl) => {
                    let ttl = ttl as u128 - Instant::now().duration_since(obj.created).as_millis();
                    (ttl / 1000).try_into().unwrap_or_default()
                }
                None => -1,
            },
            None => -2,
        }
    }

    fn del(&mut self, key: &Bytes) -> bool {
        self.data.remove(key).is_some()
    }

    fn incr(&mut self, key: Bytes) -> StorageResult<i64> {
        const INCR_ERROR: Bytes =
            Bytes::from_static(b"ERR value is not an integer or out of range");

        let val = self.get_entry_with_default(key.clone(), || {
            RedisObject::new(RedisDataType::String(Bytes::from_static(b"0")))
        });
        let bytes = match val.data {
            RedisDataType::String(ref bytes) => bytes,
            _ => Err(INCR_ERROR)?,
        };
        let mut int: i64 = std::str::from_utf8(bytes)
            .map_err(|_| INCR_ERROR)?
            .parse()
            .map_err(|_| INCR_ERROR)?;

        int += 1;
        self.set(key, Bytes::from(int.to_string()), None);

        Ok(int)
    }

    fn size(&self) -> i64 {
        let count = self.data.values().filter(|o| o.is_current()).count();
        count.try_into().unwrap_or_default()
    }

    fn flush(&mut self) {
        self.data.clear();
    }

    fn cleanup_expired(&mut self) -> usize {
        let expired_keys: Vec<_> = self
            .data
            .iter()
            .filter(|(_, obj)| !obj.is_current())
            .map(|(key, _)| key.clone())
            .collect();
        for key in &expired_keys {
            self.data.remove(key);
        }

        expired_keys.len()
    }
}
