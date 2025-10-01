use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use super::{MemoryStorage, RedisDataType, RedisObject};

pub type StreamId = (u64, u64);
pub type StreamEntry = (StreamId, Vec<(Bytes, Bytes)>);
pub type StreamKeyAndEntries = (Bytes, Vec<StreamEntry>);

type KeyIdPairs = Vec<(Bytes, StreamId)>;

/// Stream interface
pub trait StreamStorage {
    /// Add an entry to a stream
    fn xadd(&mut self, key: Bytes, id: Bytes, data: Vec<(Bytes, Bytes)>)
        -> Result<StreamId, Bytes>;
    /// Total number of entries in a stream (returns 0 if no stream exists)
    fn xlen(&self, key: &Bytes) -> i64;
    /// Get a range of entries within a stream
    fn xrange(&self, key: &Bytes, start: &Bytes, end: &Bytes) -> Result<Vec<StreamEntry>, Bytes>;
    /// Read entries from one or multiple streams, starting from the given ID (exclusive) for each stream.
    /// Returns the parsed keys and IDs, along with an array of responses in the form of `(stream_key, entries)`.
    fn xread(
        &self,
        streams: Vec<(Bytes, Bytes)>,
    ) -> Result<(KeyIdPairs, Vec<StreamKeyAndEntries>), Bytes>;
}

impl StreamStorage for MemoryStorage {
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
        if let Some(RedisDataType::Stream(map)) = self.get(key) {
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
        } else if let Some(RedisDataType::Stream(map)) = self.get(key) {
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
