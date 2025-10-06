use std::{fs::File, io::BufReader, path::Path, sync::Mutex};

use anyhow::Context;
use bytes::Bytes;
use tempfile::NamedTempFile;
use tokio::time::Instant;
use tracing::debug;

use crate::storage::{MemoryStorage, RedisDataType, RedisObject};

mod constants;
mod crc;
mod parser;
mod writer;

/// Represents a complete RDB file
#[derive(Debug)]
pub struct Rdb {
    version: Bytes,
    metadata: Vec<(Bytes, Bytes)>,
    databases: Vec<RdbDatabase>,
    checksum: u64,
}

/// Represents a database in the RDB file
#[derive(Debug)]
#[allow(dead_code, reason = "may use fields in future")]
pub struct RdbDatabase {
    idx: usize,
    db_size: usize,
    expire_size: usize,
    keys: Vec<(Bytes, RedisObject)>,
}

/// Load RDB file into memory. This is a synchronous blocking operation - use `spawn_blocking`
/// when calling from async code.
pub fn load_rdb_file(file_path: &Path) -> anyhow::Result<MemoryStorage> {
    // Read and parse RDB file
    let start = Instant::now();
    let file = File::open(file_path).context("File not found")?;
    let reader = BufReader::new(file);
    let rdb = parser::RdbParser::new(reader)
        .parse()
        .context("Failed to parse RDB file")?;

    let read_ms = Instant::now().duration_since(start).as_micros() as f64 / 1000.0;
    debug!(
        "Parsed RDB file from {file_path:?} in {read_ms} ms. Version: {:?}, Checksum: {}, Metadata: {:?}",
        rdb.version, rdb.checksum, rdb.metadata
    );

    // Load keys into storage
    let mut storage = MemoryStorage::default();
    for db in rdb.databases.into_iter() {
        storage.data.extend(db.keys.into_iter());
    }

    Ok(storage)
}

/// Save a snapshot of the in-memory database to disk in an RDB file.
/// This is a synchronous blocking operation - use `spawn_blocking` when calling from async code.
pub fn save_rdb_file(storage: &Mutex<MemoryStorage>, file_path: &Path) -> anyhow::Result<()> {
    let mut temp_file = NamedTempFile::new().context("create temp file")?;
    let rdb_writer = writer::RdbWriter::new(&mut temp_file);
    let start = Instant::now();
    {
        let storage_lock = storage.lock().unwrap();
        let current_keys = storage_lock
            .data
            .iter()
            .filter(|(_, obj)| obj.is_current())
            .collect();
        rdb_writer.dump(current_keys).context("write RDB file")?;
    }
    temp_file.persist(file_path).context("save RDB file")?;

    let write_ms = Instant::now().duration_since(start).as_micros() as f64 / 1000.0;
    debug!("Saved database snapshot to {file_path:?} in {write_ms} ms",);
    Ok(())
}
