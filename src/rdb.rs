use bytes::Bytes;

mod crc_reader;
mod parser;

/// Represents a complete RDB file
#[derive(Debug)]
pub struct Rdb {
    version: Bytes,
    metadata: Vec<(Bytes, Bytes)>,
    databases: Vec<RdbDatabase>,
    checksum: u64,
}

/// Represents a database in the RDB
#[derive(Debug, PartialEq)]
pub struct RdbDatabase {
    idx: usize,
    db_size: usize,
    expire_size: usize,
    keys: Vec<(Bytes, Bytes, Option<u64>)>,
}
