use std::collections::VecDeque;

use bytes::Bytes;

use crate::storage::MemoryStorage;

use super::{RedisDataType, RedisObject};

/// List interface
pub trait ListStorage {
    fn push(
        &mut self,
        key: Bytes,
        elems: VecDeque<Bytes>,
        dir: ListDirection,
    ) -> Result<i64, Bytes>;
    fn pop(&mut self, key: &Bytes, dir: ListDirection, count: i64) -> Option<Vec<Bytes>>;
    fn llen(&self, key: &Bytes) -> i64;
    fn lrange(&self, key: &Bytes, start: i64, stop: i64) -> Vec<Bytes>;
}

/// Direction for push/pop operations
#[derive(Debug, Clone, Copy)]
pub enum ListDirection {
    /// Front of the list
    Left,
    /// Back of the list
    Right,
}

impl ListStorage for MemoryStorage {
    fn push(
        &mut self,
        key: Bytes,
        elems: VecDeque<Bytes>,
        dir: ListDirection,
    ) -> Result<i64, Bytes> {
        let entry = self.get_entry_with_default(key.clone(), RedisObject::new_list);
        if let RedisDataType::List(ref mut vec) = entry.data {
            match dir {
                ListDirection::Right => vec.extend(elems),
                ListDirection::Left => {
                    for elem in elems {
                        vec.push_front(elem);
                    }
                }
            }
            Ok(vec.len().try_into().unwrap_or_default())
        } else {
            Err(Bytes::from_static(b"Not a list"))
        }
    }

    fn pop(&mut self, key: &Bytes, dir: ListDirection, count: i64) -> Option<Vec<Bytes>> {
        let Some(RedisDataType::List(vec)) = self.get_mut(key) else {
            return None;
        };
        let mut elems = Vec::new();
        for _ in 0..count {
            if let Some(elem) = match dir {
                ListDirection::Left => vec.pop_front(),
                ListDirection::Right => vec.pop_back(),
            } {
                elems.push(elem);
            } else {
                break;
            }
        }

        if vec.is_empty() {
            self.data.remove(key);
        }

        Some(elems)
    }

    fn llen(&self, key: &Bytes) -> i64 {
        if let Some(RedisDataType::List(list)) = self.get(key) {
            list.len().try_into().unwrap_or_default()
        } else {
            0
        }
    }

    fn lrange(&self, key: &Bytes, start: i64, stop: i64) -> Vec<Bytes> {
        let Some(RedisDataType::List(list)) = self.get(key) else {
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
}
