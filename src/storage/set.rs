use bytes::Bytes;

use super::{MemoryStorage, RedisDataType, RedisObject};

/// Set interface
pub trait SetStorage {
    fn sadd(&mut self, key: Bytes, members: Vec<Bytes>) -> Result<i64, Bytes>;
    fn srem(&mut self, key: Bytes, members: Vec<Bytes>) -> Result<i64, Bytes>;
    fn scard(&self, key: &Bytes) -> Result<i64, Bytes>;
    fn smembers(&self, key: &Bytes) -> Result<Vec<Bytes>, Bytes>;
    fn sismember(&self, key: &Bytes, member: &Bytes) -> Result<bool, Bytes>;
}

const NOT_SET: Bytes = Bytes::from_static(b"Not a set");

impl SetStorage for MemoryStorage {
    fn sadd(&mut self, key: Bytes, members: Vec<Bytes>) -> Result<i64, Bytes> {
        let entry = self.get_entry_with_default(key, RedisObject::new_set);
        let RedisDataType::Set(ref mut set) = entry.data else {
            return Err(NOT_SET);
        };

        let num_inserted = members
            .into_iter()
            .map(|m| set.insert(m))
            .filter(|inserted| *inserted)
            .count();
        Ok(num_inserted.try_into().unwrap_or_default())
    }

    fn srem(&mut self, key: Bytes, members: Vec<Bytes>) -> Result<i64, Bytes> {
        let entry = self.get_entry_with_default(key, RedisObject::new_set);
        let RedisDataType::Set(ref mut set) = entry.data else {
            return Err(NOT_SET);
        };

        let num_removed = members
            .iter()
            .map(|m| set.remove(m))
            .filter(|removed| *removed)
            .count();
        Ok(num_removed.try_into().unwrap_or_default())
    }

    fn scard(&self, key: &Bytes) -> Result<i64, Bytes> {
        let Some(data) = self.get(key) else {
            return Ok(0);
        };
        let RedisDataType::Set(set) = data else {
            return Err(NOT_SET);
        };
        Ok(set.len().try_into().unwrap_or_default())
    }

    fn smembers(&self, key: &Bytes) -> Result<Vec<Bytes>, Bytes> {
        let Some(data) = self.get(key) else {
            return Ok(Vec::new());
        };
        let RedisDataType::Set(set) = data else {
            return Err(NOT_SET);
        };
        Ok(set.iter().cloned().collect())
    }

    fn sismember(&self, key: &Bytes, member: &Bytes) -> Result<bool, Bytes> {
        let Some(data) = self.get(key) else {
            return Ok(false);
        };
        let RedisDataType::Set(set) = data else {
            return Err(NOT_SET);
        };
        Ok(set.contains(member))
    }
}
