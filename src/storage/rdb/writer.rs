use std::{
    io::{self, BufWriter, Write},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::bail;
use byteorder::{BigEndian, LittleEndian, WriteBytesExt};
use bytes::Bytes;
use tokio::time::Instant;

use super::{constants, crc::Crc64Writer, RedisDataType, RedisObject};

/// RDB database file writer
pub struct RdbWriter<W: Write> {
    /// Buffered file writer with checksum calculation
    file: BufWriter<Crc64Writer<W>>,
}

impl<W: Write> RdbWriter<W> {
    /// Create a new buffered RDB writer
    pub fn new(w: W) -> Self {
        Self {
            file: BufWriter::new(Crc64Writer::new(w)),
        }
    }

    /// Write all given keys and values into the writer in RDB format
    pub fn dump(mut self, keys: Vec<(&Bytes, &RedisObject)>) -> anyhow::Result<()> {
        self.write_header()?;
        self.write_metadata()?;
        self.write_database(0, keys)?;
        self.write_end()?;
        self.file.flush()?;

        Ok(())
    }

    fn write_header(&mut self) -> anyhow::Result<()> {
        self.file.write_all(b"REDIS")?; // REDIS
        self.file.write_all(b"0011")?; // file version
        Ok(())
    }

    fn write_metadata(&mut self) -> anyhow::Result<()> {
        let server_version = env!("CARGO_PKG_VERSION");
        let unix_time_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Server version
        self.file.write_u8(constants::META_FLAG)?;
        write_string(&mut self.file, b"tinikeyval-ver")?;
        write_string(&mut self.file, server_version.as_bytes())?;

        // Database save time
        self.file.write_u8(constants::META_FLAG)?;
        write_string(&mut self.file, b"ctime")?;
        write_string_int(&mut self.file, unix_time_seconds)?;

        Ok(())
    }

    fn write_database(
        &mut self,
        db_idx: usize,
        keys: Vec<(&Bytes, &RedisObject)>,
    ) -> anyhow::Result<()> {
        // Database flag and index
        self.file.write_u8(constants::DB_FLAG)?;
        write_size(&mut self.file, db_idx)?;

        // Write database sizes
        let (db_size, expire_size) =
            keys.iter()
                .fold((0, 0), |(mut db_size, mut expire_size), (_, obj)| {
                    if obj.is_persist_supported() {
                        db_size += 1;
                        if obj.expiration.is_some() {
                            expire_size += 1;
                        }
                    }
                    (db_size, expire_size)
                });
        self.file.write_u8(constants::DB_SIZE_FLAG)?;
        write_size(&mut self.file, db_size)?;
        write_size(&mut self.file, expire_size)?;

        // Write database keys
        let mut db_size_check = 0;
        let mut expire_size_check = 0;
        let unix_time_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        for (key, object) in keys {
            // Write expiration time (Unix epoch time millis)
            let expires_at = object.expiration.map(|expiration| {
                let expires_in_millis = (expiration - Instant::now()).as_millis() as u64;
                unix_time_millis + expires_in_millis
            });
            if let Some(expires_at) = expires_at {
                self.file.write_u8(constants::EXPIRY_U64_FLAG)?;
                self.file.write_u64::<LittleEndian>(expires_at)?;
                expire_size_check += 1;
            }

            // Write type flag
            let type_flag = match &object.data {
                RedisDataType::String(_) => constants::TYPE_STRING_FLAG,
                RedisDataType::List(_) => constants::TYPE_LIST_FLAG,
                RedisDataType::Set(_) => constants::TYPE_SET_FLAG,
                _ => todo!("data type not supported yet"),
            };
            self.file.write_u8(type_flag)?;

            // Write key and value
            write_string(&mut self.file, key)?;
            match &object.data {
                RedisDataType::String(value) => {
                    write_string(&mut self.file, value)?;
                }
                RedisDataType::List(list) => {
                    write_size(&mut self.file, list.len())?;
                    for member in list {
                        write_string(&mut self.file, member)?;
                    }
                }
                RedisDataType::Set(set) => {
                    write_size(&mut self.file, set.len())?;
                    for member in set {
                        write_string(&mut self.file, member)?;
                    }
                }
                _ => todo!("data type not supported"),
            };
            db_size_check += 1;
        }

        // Verify key counts
        if db_size != db_size_check || expire_size != expire_size_check {
            bail!("Database size verification failed");
        }

        Ok(())
    }

    fn write_end(&mut self) -> anyhow::Result<()> {
        self.file.write_u8(constants::END_FILE_FLAG)?;
        self.file.flush()?;

        let checksum = self.file.get_ref().checksum();
        self.file.write_u64::<LittleEndian>(checksum)?;

        Ok(())
    }
}

fn write_string(writer: &mut impl Write, val: &[u8]) -> io::Result<()> {
    write_size(writer, val.len())?;
    writer.write_all(val)?;
    Ok(())
}

fn write_size(writer: &mut impl Write, len: usize) -> io::Result<()> {
    match len {
        len if len <= 0x3F => writer.write_u8(len as u8)?,
        len if len <= 0x3FFF => write_u16_size(writer, len as u16)?,
        len if len <= u32::MAX as usize => write_u32_size(writer, len as u32)?,
        invalid_len => Err(io::Error::other(format!("Length {invalid_len} too long")))?,
    }
    Ok(())
}

/// For lengths up to 16383 (0x3FFF)
fn write_u16_size(writer: &mut impl Write, len: u16) -> io::Result<()> {
    /// sets the 2nd significant bit to indicate a u16 length
    const BITMASK: u16 = 0x4000;
    let len = len | BITMASK;
    writer.write_u16::<BigEndian>(len)?;

    Ok(())
}

/// For lengths up to 2^32 - 1 (u32::MAX)
fn write_u32_size(writer: &mut impl Write, len: u32) -> io::Result<()> {
    /// Indicates that a u32 follows
    const FIRST_BYTE: u8 = 0x80;
    writer.write_u8(FIRST_BYTE)?;
    writer.write_u32::<BigEndian>(len)?;

    Ok(())
}

/// Write an encoded integer string
fn write_string_int(writer: &mut impl Write, val: i64) -> io::Result<()> {
    match val {
        val if val >= i8::MIN as i64 && val <= i8::MAX as i64 => {
            writer.write_u8(constants::STRING_I8_FLAG)?;
            writer.write_i8(val as i8)?;
        }
        val if val >= i16::MIN as i64 && val <= i16::MAX as i64 => {
            writer.write_u8(constants::STRING_I16_FLAG)?;
            writer.write_i16::<LittleEndian>(val as i16)?;
        }
        val if val >= i32::MIN as i64 && val <= i32::MAX as i64 => {
            writer.write_u8(constants::STRING_I32_FLAG)?;
            writer.write_i32::<LittleEndian>(val as i32)?;
        }
        val => write_string(writer, val.to_string().as_bytes())?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use bytes::Buf;

    use super::super::parser::RdbParser;

    use super::*;

    #[test]
    fn size() -> io::Result<()> {
        let mut buf = Vec::new();
        write_size(&mut buf, 63)?;
        assert_eq!(buf, &[63], "len <= 63");

        let mut buf = Vec::new();
        write_size(&mut buf, 64)?;
        assert_eq!(buf, &[0b01000000, 64], "len == 64");

        let mut buf = Vec::new();
        write_size(&mut buf, 16383)?;
        assert_eq!(buf, &[0b01111111, 0xFF], "len <= 16383");

        let mut buf = Vec::new();
        write_size(&mut buf, 16384)?;
        assert_eq!(buf, &[0x80, 0, 0, 0x40, 0x00], "len > 16383");

        Ok(())
    }

    #[test]
    fn string() -> io::Result<()> {
        let mut buf = Vec::new();
        write_string(&mut buf, b"foo")?;
        write_string(&mut buf, b"bar")?;
        assert_eq!(buf, &[0x03, 0x66, 0x6F, 0x6F, 0x03, 0x62, 0x61, 0x72]);

        Ok(())
    }

    #[test]
    fn string_int() -> io::Result<()> {
        let mut buf = Vec::new();
        write_string_int(&mut buf, 12345)?;
        assert_eq!(buf, &[constants::STRING_I16_FLAG, 0x39, 0x30]);

        let mut buf = Vec::new();
        write_string_int(&mut buf, 1234567)?;
        assert_eq!(buf, &[constants::STRING_I32_FLAG, 0x87, 0xD6, 0x12, 0x00]);

        let mut buf = Vec::new();
        write_string_int(&mut buf, -12345)?;
        assert_eq!(buf, &[constants::STRING_I16_FLAG, 0xc7, 0xcf]);

        Ok(())
    }

    #[test]
    fn write_and_parse_rdb() -> anyhow::Result<()> {
        let foo_key = Bytes::from("foo");
        let foo_val = Bytes::from("bar");
        let foo_obj = RedisObject::new(RedisDataType::String(foo_val.clone()));

        let bar_key = Bytes::from("bar");
        let bar_val = Bytes::from("baz");
        let bar_exp = 5000;
        let bar_obj = RedisObject::new_with_ttl(
            RedisDataType::List(VecDeque::from([foo_val.clone(), bar_val.clone()])),
            Some(bar_exp),
        );

        let keys = vec![(&foo_key, &foo_obj), (&bar_key, &bar_obj)];
        let mut buf = Vec::new();
        let rdb_writer = RdbWriter::new(&mut buf);
        rdb_writer.dump(keys)?;

        let rdb_parser = RdbParser::new(buf.reader());
        let rdb = rdb_parser.parse()?;

        let version_meta = (
            Bytes::from("tinikeyval-ver"),
            Bytes::from(env!("CARGO_PKG_VERSION")),
        );
        assert_eq!(rdb.metadata[0], version_meta);
        assert_eq!(rdb.metadata[1].0, Bytes::from("ctime"));

        let keys = &rdb.databases[0].keys;
        assert_eq!(keys[0].0, foo_key);
        assert_eq!(keys[0].1.data, RedisDataType::String(foo_val.clone()));

        assert_eq!(keys[1].0, bar_key);
        assert_eq!(
            keys[1].1.data,
            RedisDataType::List(VecDeque::from([foo_val, bar_val]))
        );
        assert!(keys[1].1.expiration.is_some());

        Ok(())
    }
}
