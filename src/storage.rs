use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use tokio::time::Instant;

pub type StreamId = (u64, u64);
pub type StreamEntry = (StreamId, Vec<(Bytes, Bytes)>);

/// Base storage interface
pub trait Storage {
    fn get(&self, key: &Bytes) -> Option<Bytes>;
    fn set(&mut self, key: Bytes, val: Bytes, ttl_millis: Option<u64>);
    fn ttl(&self, key: &Bytes) -> i64;
    fn kind(&self, key: &Bytes) -> Bytes;
    fn del(&mut self, key: &Bytes) -> bool;
    fn push(
        &mut self,
        key: Bytes,
        elems: VecDeque<Bytes>,
        dir: ListDirection,
    ) -> Result<i64, Bytes>;
    fn pop(&mut self, key: &Bytes, dir: ListDirection, count: i64) -> Option<Vec<Bytes>>;
    fn llen(&self, key: &Bytes) -> i64;
    fn lrange(&self, key: &Bytes, start: i64, stop: i64) -> Vec<Bytes>;
    fn xadd(&mut self, key: Bytes, id: Bytes, data: Vec<(Bytes, Bytes)>)
        -> Result<StreamId, Bytes>;
    fn xlen(&self, key: &Bytes) -> i64;
    fn xrange(&self, key: &Bytes, start: &Bytes, end: &Bytes) -> Result<Vec<StreamEntry>, Bytes>;
    fn xread(
        &self,
        streams: Vec<(Bytes, Bytes)>,
    ) -> Result<(Vec<(Bytes, StreamId)>, Vec<(Bytes, Vec<StreamEntry>)>), Bytes>;
    fn size(&self) -> i64;
    fn flush(&mut self);
    fn cleanup_expired(&mut self) -> usize;
}

/// Direction for push/pop operations
#[derive(Debug, Clone, Copy)]
pub enum ListDirection {
    /// Front of the list
    Left,
    /// Back of the list
    Right,
}

/// Memory storage implementation using a HashMap
#[derive(Debug, Default)]
pub struct MemoryStorage {
    data: HashMap<Bytes, RedisObject>,
}

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
    Stream(BTreeMap<StreamId, Vec<(Bytes, Bytes)>>),
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

    fn xadd(
        &mut self,
        key: Bytes,
        id: Bytes,
        data: Vec<(Bytes, Bytes)>,
    ) -> Result<StreamId, Bytes> {
        // Validate/generate ID
        const MIN_ID: StreamId = (0, 0);
        let min_id = if let Some(RedisDataType::Stream(map)) = self.get(&key) {
            map.last_key_value().map(|(id, _)| *id).unwrap_or(MIN_ID)
        } else {
            MIN_ID
        };
        let id = parse_stream_id(
            &id,
            true,
            |ms| if ms == min_id.0 { min_id.1 + 1 } else { 0 },
        )?;
        if id == (0, 0) {
            return Err(Bytes::from_static(
                b"ERR The ID specified in XADD must be greater than 0-0",
            ));
        }
        if id <= min_id {
            return Err(Bytes::from_static(
                b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
            ));
        }

        // Insert entry into stream, creating a new stream if needed
        let entry = self.get_entry_with_default(key, RedisObject::new_stream);
        let RedisDataType::Stream(ref mut map) = entry.data else {
            return Err(Bytes::from_static(b"Not a stream"));
        };
        map.insert(id, data);
        Ok(id)
    }

    fn xlen(&self, key: &Bytes) -> i64 {
        if let Some(RedisDataType::Stream(map)) = self.get(&key) {
            map.len().try_into().unwrap_or_default()
        } else {
            0
        }
    }

    fn xrange(&self, key: &Bytes, start: &Bytes, end: &Bytes) -> Result<Vec<StreamEntry>, Bytes> {
        let start = match start.as_ref() {
            b"-" => (0, 0),
            _ => parse_stream_id(start, false, |_| 0)?,
        };
        let end = match end.as_ref() {
            b"+" => (u64::MAX, u64::MAX),
            _ => parse_stream_id(end, false, |_| u64::MAX)?,
        };
        if start > end {
            Ok(Vec::new())
        } else if let Some(RedisDataType::Stream(map)) = self.get(&key) {
            Ok(map
                .range(start..=end)
                .map(|(id, data)| (*id, data.to_owned()))
                .collect())
        } else {
            Ok(Vec::new())
        }
    }

    fn xread(
        &self,
        streams: Vec<(Bytes, Bytes)>,
    ) -> Result<(Vec<(Bytes, StreamId)>, Vec<(Bytes, Vec<StreamEntry>)>), Bytes> {
        const START_ID: StreamId = (0, 0);
        let mut parsed_streams = Vec::with_capacity(streams.len());
        let mut response = Vec::with_capacity(streams.len());

        for (key, id) in streams {
            if let Some(RedisDataType::Stream(map)) = self.get(&key) {
                let (start_ms, start_seq) = match id.as_ref() {
                    b"$" => map.last_key_value().map(|(id, _)| *id).unwrap_or(START_ID),
                    _ => parse_stream_id(&id, false, |_| 0)?,
                };
                parsed_streams.push((key.clone(), (start_ms, start_seq)));

                let entries: Vec<StreamEntry> = map
                    .range((start_ms, start_seq + 1)..)
                    .map(|(id, data)| (*id, data.to_owned()))
                    .collect();
                if !entries.is_empty() {
                    response.push((key, entries));
                }
            } else {
                let parsed_id = match id.as_ref() {
                    b"$" => START_ID,
                    _ => parse_stream_id(&id, false, |_| 0)?,
                };
                parsed_streams.push((key, parsed_id));
            }
        }

        Ok((parsed_streams, response))
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
            .filter_map(|(key, obj)| (!obj.is_current()).then(|| key.clone()))
            .collect();
        for key in &expired_keys {
            self.data.remove(key);
        }

        expired_keys.len()
    }
}

const INVALID_ID: Bytes = Bytes::from_static(b"ERR invalid ID");

fn parse_stream_id<S>(raw: &Bytes, generate_ms: bool, default_seq: S) -> Result<StreamId, Bytes>
where
    S: Fn(u64) -> u64,
{
    let mut id_split = raw.splitn(2, |b| *b == b'-');
    let ms: u64 =
        match std::str::from_utf8(id_split.next().expect("first split")).map_err(|_| INVALID_ID)? {
            "*" => generate_ms.then(gen_stream_id_ms).ok_or(INVALID_ID)?,
            ms => ms.parse().map_err(|_| INVALID_ID)?,
        };
    let seq: u64 = match id_split
        .next()
        .map(std::str::from_utf8)
        .transpose()
        .map_err(|_| INVALID_ID)?
    {
        Some("*") | None => default_seq(ms),
        Some(seq) => seq.parse().map_err(|_| INVALID_ID)?,
    };
    Ok((ms, seq))
}

fn gen_stream_id_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
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
            created: Instant::now(),
            ttl_millis: None,
            data,
        }
    }

    pub fn new_list() -> Self {
        Self::new(RedisDataType::List(VecDeque::with_capacity(1)))
    }

    pub fn new_stream() -> Self {
        Self::new(RedisDataType::Stream(BTreeMap::new()))
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
