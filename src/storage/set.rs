use std::collections::HashSet;

use bytes::Bytes;

use super::{MemoryStorage, RedisDataType, RedisObject, StorageResult as Result};

/// Set interface
pub trait SetStorage {
    fn sadd(&mut self, key: Bytes, members: Vec<Bytes>) -> Result<i64>;
    fn srem(&mut self, key: &Bytes, members: Vec<Bytes>) -> Result<i64>;
    fn scard(&self, key: &Bytes) -> Result<i64>;
    fn smembers(&self, key: &Bytes) -> Result<Vec<Bytes>>;
    fn sismember(&self, key: &Bytes, member: &Bytes) -> Result<bool>;
}

impl SetStorage for MemoryStorage {
    fn sadd(&mut self, key: Bytes, members: Vec<Bytes>) -> Result<i64> {
        let set = self.get_set_entry(key)?;
        let num_inserted = members
            .into_iter()
            .map(|m| set.insert(m))
            .filter(|inserted| *inserted)
            .count();
        Ok(num_inserted.try_into().unwrap_or_default())
    }

    fn srem(&mut self, key: &Bytes, members: Vec<Bytes>) -> Result<i64> {
        let Some(set) = self.get_set_mut(&key)? else {
            return Ok(0);
        };

        let num_removed = members
            .iter()
            .map(|m| set.remove(m))
            .filter(|removed| *removed)
            .count();
        if set.is_empty() {
            self.data.remove(key);
        }

        Ok(num_removed.try_into().unwrap_or_default())
    }

    fn scard(&self, key: &Bytes) -> Result<i64> {
        Ok(match self.get_set(key)? {
            Some(set) => set.len().try_into().unwrap_or_default(),
            None => 0,
        })
    }

    fn smembers(&self, key: &Bytes) -> Result<Vec<Bytes>> {
        Ok(match self.get_set(key)? {
            Some(set) => set.iter().cloned().collect(),
            None => Vec::new(),
        })
    }

    fn sismember(&self, key: &Bytes, member: &Bytes) -> Result<bool> {
        Ok(match self.get_set(key)? {
            Some(set) => set.contains(member),
            None => false,
        })
    }
}

const NOT_SET: Bytes = Bytes::from_static(b"Not a set");

impl MemoryStorage {
    fn get_set(&self, key: &Bytes) -> Result<Option<&HashSet<Bytes>>> {
        let Some(data) = self.get(key) else {
            return Ok(None);
        };
        let RedisDataType::Set(set) = data else {
            return Err(NOT_SET);
        };
        Ok(Some(set))
    }

    fn get_set_mut(&mut self, key: &Bytes) -> Result<Option<&mut HashSet<Bytes>>> {
        match self.get_mut(key) {
            Some(RedisDataType::Set(set)) => Ok(Some(set)),
            Some(_) => Err(NOT_SET),
            None => Ok(None),
        }
    }

    fn get_set_entry(&mut self, key: Bytes) -> Result<&mut HashSet<Bytes>> {
        let entry = self.get_entry_with_default(key, RedisObject::new_set);
        let RedisDataType::Set(ref mut set) = entry.data else {
            return Err(NOT_SET);
        };
        Ok(set)
    }
}
