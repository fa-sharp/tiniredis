use std::io::Read;

use anyhow::bail;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};

/// A parsed RDB file
#[derive(Debug)]
pub struct Rdb {
    version: Bytes,
    metadata: Vec<(Bytes, Bytes)>,
    databases: Vec<RdbDatabase>,
}

/// Represents a database in the RDB
#[derive(Debug, PartialEq)]
pub struct RdbDatabase {
    idx: usize,
    db_size: usize,
    expire_size: usize,
    keys: Vec<(Bytes, Bytes, Option<u64>)>,
}

/// RDB database file parser
pub struct RdbParser<R> {
    /// Internal buffer
    buf: BytesMut,
    /// Current flag
    flag: u8,
    /// File reader
    file: R,
}

const META_HEADER: u8 = 0xFA;
const DB_HEADER: u8 = 0xFE;
const TABLE_SIZE_HEADER: u8 = 0xFB;
const END_FILE_HEADER: u8 = 0xFF;

impl<R: Read> RdbParser<R> {
    pub fn new(file: R) -> Self {
        Self {
            buf: BytesMut::with_capacity(1024),
            flag: 0,
            file,
        }
    }

    pub fn parse(mut self) -> anyhow::Result<Rdb> {
        let version = self.parse_header()?;
        self.flag = self.file.read_u8()?;

        let mut metadata = Vec::new();
        while self.flag == META_HEADER {
            let (name, val) = self.parse_metadata()?;
            metadata.push((name, val));
        }

        let mut databases = Vec::new();
        while self.flag == DB_HEADER {
            let db = self.parse_database()?;
            databases.push(db);
        }

        Ok(Rdb {
            version,
            metadata,
            databases,
        })
    }

    fn parse_header(&mut self) -> anyhow::Result<Bytes> {
        self.buf.resize(5, 0);
        self.file.read_exact(&mut self.buf[..5])?; // REDIS
        self.file.read_exact(&mut self.buf[..4])?; // rdb version

        Ok(self.buf.split_to(4).freeze())
    }

    fn parse_metadata(&mut self) -> anyhow::Result<(Bytes, Bytes)> {
        let n = read_length_encoded_string(&mut self.file, &mut self.buf)?;
        let name = self.buf.split_to(n).freeze();
        let n = read_length_encoded_string(&mut self.file, &mut self.buf)?;
        let value = self.buf.split_to(n).freeze();

        self.flag = self.file.read_u8()?;

        Ok((name, value))
    }

    fn parse_database(&mut self) -> anyhow::Result<RdbDatabase> {
        // Read database index, database size, and expire table size
        let idx = read_size(self.file.read_u8()?, &mut self.file)?;
        if self.file.read_u8()? != TABLE_SIZE_HEADER {
            bail!("table size header for database {idx} not found");
        }
        let db_size = read_size(self.file.read_u8()?, &mut self.file)?;
        let expire_size = read_size(self.file.read_u8()?, &mut self.file)?;

        // Read keys and values
        let mut keys = Vec::with_capacity(db_size);
        while let Some((key, val, expires)) =
            next_key(&mut self.flag, &mut self.file, &mut self.buf)?
        {
            keys.push((key, val, expires));
        }

        Ok(RdbDatabase {
            idx,
            db_size,
            expire_size,
            keys,
        })
    }
}

/// Read the next key value pair in the database. Returns `None` if at the end of the database.
fn next_key(
    flag: &mut u8,
    reader: &mut impl Read,
    buf: &mut BytesMut,
) -> anyhow::Result<Option<(Bytes, Bytes, Option<u64>)>> {
    *flag = reader.read_u8()?;
    match *flag {
        // end of database
        DB_HEADER | END_FILE_HEADER => Ok(None),
        // key with u64 expiry - Unix time milliseconds
        0xFC => {
            let expires = reader.read_u64::<LittleEndian>()?;
            let (key, value) = read_key_value(reader.read_u8()?, reader, buf)?;
            Ok(Some((key, value, Some(expires))))
        }
        // key with u32 expiry - Unix time seconds
        0xFD => {
            let expires = (reader.read_u32::<LittleEndian>()? * 1000).into();
            let (key, value) = read_key_value(reader.read_u8()?, reader, buf)?;
            Ok(Some((key, value, Some(expires))))
        }
        // key with no expiry
        type_flag => {
            let (key, value) = read_key_value(type_flag, reader, buf)?;
            Ok(Some((key, value, None)))
        }
    }
}

/// Read a key value pair with the given type flag
fn read_key_value(
    flag: u8,
    reader: &mut impl Read,
    buf: &mut BytesMut,
) -> anyhow::Result<(Bytes, Bytes)> {
    // check the type flag, and then read the value
    match flag {
        // string type
        0x00 => {
            let n = read_length_encoded_string(reader, buf)?;
            let key = buf.split_to(n).freeze();
            let n = read_length_encoded_string(reader, buf)?;
            let value = buf.split_to(n).freeze();
            Ok((key, value))
        }
        flag => todo!("type not yet implemented: {flag}"),
    }
}

/// Get the first 2 significant bits of a length value
fn length_flag(first_byte: u8) -> u8 {
    (first_byte & 0b11000000) >> 6
}

/// read a size/length value
fn read_size(first_byte: u8, reader: &mut impl Read) -> anyhow::Result<usize> {
    // check the flag, and then get the length
    let length = match length_flag(first_byte) {
        // length is u8
        0b00 => first_byte as usize,
        // length is u16: last 6 bits and next byte
        0b01 => {
            let len_bytes = [first_byte & 0b00111111, reader.read_u8()?];
            u16::from_be_bytes(len_bytes) as usize
        }
        // length is u32: next 4 bytes
        0b10 => reader.read_u32::<BigEndian>()? as usize,
        0b11 => bail!("expected size, got an encoded integer string"),
        _ => unreachable!(),
    };
    Ok(length)
}

fn read_length_encoded_string(reader: &mut impl Read, buf: &mut BytesMut) -> anyhow::Result<usize> {
    let first_byte = reader.read_u8()?;

    // Check the first 2 bits
    match length_flag(first_byte) {
        // Get the length and read the whole string
        0b00 | 0b01 | 0b10 => {
            let length = read_size(first_byte, reader)?;
            buf.resize(length, 0);
            reader.read_exact(&mut buf[..length])?;

            Ok(length)
        }
        // String is an encoded integer
        0b11 => {
            let val = match first_byte {
                // value is an 8-bit integer
                0xC0 => reader.read_i8()?.to_string(),
                // value is a little-endian 16-bit integer
                0xC1 => reader.read_i16::<LittleEndian>()?.to_string(),
                // value is a little-endian 32-bit integer
                0xC2 => reader.read_i32::<LittleEndian>()?.to_string(),
                _ => unimplemented!("LZF compression not implemented"),
            };
            buf.resize(val.len(), 0);
            buf.copy_from_slice(val.as_bytes());

            Ok(val.len())
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod test {
    use bytes::Buf;

    use super::*;

    #[test]
    fn string() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();

        let bytes = &[
            0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21,
        ];
        let n = read_length_encoded_string(&mut bytes.reader(), &mut buf)?;
        assert_eq!(n, 13);
        assert_eq!(&buf[..n], b"Hello, World!".as_slice());

        let bytes = &[0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72];
        let n = read_length_encoded_string(&mut bytes.reader(), &mut buf)?;
        assert_eq!(n, 6);
        assert_eq!(&buf[..n], b"foobar".as_slice());

        let bytes = &[0x03, 0x62, 0x61, 0x7A];
        let n = read_length_encoded_string(&mut bytes.reader(), &mut buf)?;
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"baz".as_slice());

        Ok(())
    }

    #[test]
    fn string_int8() -> anyhow::Result<()> {
        let bytes = &[0xC0, 0x7B];
        let mut buf = BytesMut::new();
        let n = read_length_encoded_string(&mut bytes.reader(), &mut buf)?;
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"123".as_slice());

        Ok(())
    }

    #[test]
    fn string_int16() -> anyhow::Result<()> {
        let bytes = &[0xC1, 0x39, 0x30];
        let mut buf = BytesMut::new();
        let n = read_length_encoded_string(&mut bytes.reader(), &mut buf)?;
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"12345".as_slice());

        Ok(())
    }

    #[test]
    fn string_int32() -> anyhow::Result<()> {
        let bytes = &[0xC2, 0x87, 0xD6, 0x12, 0x00];
        let mut buf = BytesMut::new();
        let n = read_length_encoded_string(&mut bytes.reader(), &mut buf)?;
        assert_eq!(n, 7);
        assert_eq!(&buf[..n], b"1234567".as_slice());

        Ok(())
    }

    const FOO_BAR_KV: &[u8] = &[0x00, 0x03, 0x66, 0x6F, 0x6F, 0x03, 0x62, 0x61, 0x72];
    const BAZ_QUX_KV: &[u8] = &[0x00, 0x03, 0x62, 0x61, 0x7A, 0x03, 0x71, 0x75, 0x78];

    #[test]
    fn string_key_value() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        let mut reader = FOO_BAR_KV.reader();
        let (key, val) = read_key_value(reader.read_u8()?, &mut reader, &mut buf)?;
        assert_eq!(key, Bytes::from("foo"));
        assert_eq!(val, Bytes::from("bar"));

        let mut reader = BAZ_QUX_KV.reader();
        let (key, val) = read_key_value(reader.read_u8()?, &mut reader, &mut buf)?;
        assert_eq!(key, Bytes::from("baz"));
        assert_eq!(val, Bytes::from("qux"));

        Ok(())
    }

    #[test]
    fn read_key_no_expiry() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        let mut reader = FOO_BAR_KV.reader();
        let mut flag = 0;
        let (key, val, expires) = next_key(&mut flag, &mut reader, &mut buf)?.unwrap();
        assert_eq!(key, Bytes::from("foo"));
        assert_eq!(val, Bytes::from("bar"));
        assert_eq!(expires, None);

        Ok(())
    }

    #[test]
    fn read_key_ms_expiry() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        let expire_bytes = &[0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00];
        let bytes = [expire_bytes, FOO_BAR_KV].concat();
        let mut reader = bytes.reader();
        let mut flag = 0;

        let (key, val, expires) = next_key(&mut flag, &mut reader, &mut buf)?.unwrap();
        assert_eq!(key, Bytes::from("foo"));
        assert_eq!(val, Bytes::from("bar"));
        assert_eq!(expires, Some(1713824559637));

        Ok(())
    }

    #[test]
    fn read_key_secs_expiry() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();
        let expire_bytes = &[0xFC, 0x15, 0x72, 0xE7, 0x07, 0x8F, 0x01, 0x00, 0x00];
        let bytes = [expire_bytes, FOO_BAR_KV].concat();
        let mut reader = bytes.reader();
        let mut flag = 0;

        let (key, val, expires) = next_key(&mut flag, &mut reader, &mut buf)?.unwrap();
        assert_eq!(key, Bytes::from("foo"));
        assert_eq!(val, Bytes::from("bar"));
        assert_eq!(expires, Some(1713824559637));

        Ok(())
    }

    #[test]
    fn parse_rdb() -> anyhow::Result<()> {
        let raw_rdb_file: Vec<u8> = vec![
            82, 69, 68, 73, 83, 48, 48, 49, 49, 250, 10, 118, 97, 108, 107, 101, 121, 45, 118, 101,
            114, 5, 56, 46, 49, 46, 51, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115,
            192, 64, 250, 5, 99, 116, 105, 109, 101, 194, 235, 144, 225, 104, 250, 8, 117, 115,
            101, 100, 45, 109, 101, 109, 194, 64, 0, 18, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115,
            101, 192, 0, 254, 0, 251, 3, 1, 0, 3, 102, 111, 111, 3, 98, 97, 114, 0, 5, 99, 111,
            117, 110, 116, 193, 140, 60, 252, 192, 128, 30, 177, 153, 1, 0, 0, 0, 3, 98, 97, 114,
            3, 98, 97, 120, 255, 15, 57, 201, 59, 63, 77, 52, 99,
        ];
        let rdb = RdbParser::new(raw_rdb_file.reader()).parse()?;
        assert_eq!(rdb.version, b"0011".as_slice());
        assert_eq!(rdb.metadata.len(), 5);
        assert_eq!(
            rdb.databases[0],
            RdbDatabase {
                idx: 0,
                db_size: 3,
                expire_size: 1,
                keys: vec![
                    (Bytes::from("foo"), Bytes::from("bar"), None),
                    (Bytes::from("count"), Bytes::from("15500"), None),
                    (Bytes::from("bar"), Bytes::from("bax"), Some(1759613190336))
                ]
            }
        );

        Ok(())
    }
}
