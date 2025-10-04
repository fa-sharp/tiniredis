use std::{
    fs::File,
    io::{BufReader, Read},
};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};

/// A parsed RDB file
#[derive(Debug)]
struct ParsedRdb {
    version: Bytes,
    metadata: Vec<(Bytes, Bytes)>,
}

const META_HEADER: u8 = 0xFA;

fn parse_rdb_file(file_path: &str) -> anyhow::Result<ParsedRdb> {
    let mut file = BufReader::new(File::open(file_path)?);
    let mut buf = BytesMut::with_capacity(1024);

    // HEADER
    buf.resize(5, 0);
    file.read_exact(&mut buf[..5])?; // REDIS
    file.read_exact(&mut buf[..4])?; // rdb version
    let rdb_version = buf.split_to(4).freeze();

    // METADATA
    let mut metadata = Vec::new();
    while file.read_u8()? == META_HEADER {
        let n = read_length_encoded_value(&mut file, &mut buf)?;
        let name = buf.split_to(n).freeze();
        let n = read_length_encoded_value(&mut file, &mut buf)?;
        let value = buf.split_to(n).freeze();
        metadata.push((name, value));
    }

    let rdb = ParsedRdb {
        version: rdb_version,
        metadata,
    };

    Ok(rdb)
}

fn read_length_encoded_value(mut reader: impl Read, buf: &mut BytesMut) -> anyhow::Result<usize> {
    let first_byte = reader.read_u8()?;

    // check the flag in the first 2 bits, and then get the length and number of bytes read
    let (length, n) = match (first_byte & 0b11000000) >> 6 {
        0b00 => (first_byte as usize, 1), // length is the last 6 bits
        0b01 => {
            // length is last 6 bits and next byte
            let len_bytes = [first_byte & 0b00111111, reader.read_u8()?];
            (u16::from_be_bytes(len_bytes) as usize, 2)
        }
        0b10 => (reader.read_u32::<BigEndian>()? as usize, 5), // length is the next 4 bytes
        0b11 => {
            // Value is an encoded integer
            let (val, n) = match first_byte {
                0xC0 => {
                    // value is an 8-bit integer
                    (reader.read_u8()?.to_string(), 2)
                }
                0xC1 => {
                    // value is a little-endian 16-bit integer
                    (reader.read_u16::<LittleEndian>()?.to_string(), 3)
                }
                0xC2 => {
                    // value is a little-endian 32-bit integer
                    (reader.read_u32::<LittleEndian>()?.to_string(), 5)
                }
                _ => unimplemented!(),
            };
            buf.resize(val.len(), 0);
            buf.copy_from_slice(val.as_bytes());
            return Ok(val.len());
        }
        _ => unreachable!(),
    };

    buf.resize(length, 0);
    reader.read_exact(&mut buf[..length])?;

    Ok(length)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn string() -> anyhow::Result<()> {
        let mut buf = BytesMut::new();

        let bytes = &[
            0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21,
        ];
        let reader = BufReader::new(bytes.as_slice());
        let n = read_length_encoded_value(reader, &mut buf)?;
        assert_eq!(n, 13);
        assert_eq!(&buf[..n], b"Hello, World!".as_slice());

        let bytes = &[0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72];
        let reader = BufReader::new(bytes.as_slice());
        let n = read_length_encoded_value(reader, &mut buf)?;
        assert_eq!(n, 6);
        assert_eq!(&buf[..n], b"foobar".as_slice());

        let bytes = &[0x03, 0x62, 0x61, 0x7A];
        let reader = BufReader::new(bytes.as_slice());
        let n = read_length_encoded_value(reader, &mut buf)?;
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"baz".as_slice());

        Ok(())
    }

    #[test]
    fn int8() -> anyhow::Result<()> {
        let bytes = &[0xC0, 0x7B];
        let reader = BufReader::new(bytes.as_slice());
        let mut buf = BytesMut::new();
        let n = read_length_encoded_value(reader, &mut buf)?;
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"123".as_slice());

        Ok(())
    }

    #[test]
    fn int16() -> anyhow::Result<()> {
        let bytes = &[0xC1, 0x39, 0x30];
        let reader = BufReader::new(bytes.as_slice());
        let mut buf = BytesMut::new();
        let n = read_length_encoded_value(reader, &mut buf)?;
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"12345".as_slice());

        Ok(())
    }

    #[test]
    fn int32() -> anyhow::Result<()> {
        let bytes = &[0xC2, 0x87, 0xD6, 0x12, 0x00];
        let reader = BufReader::new(bytes.as_slice());
        let mut buf = BytesMut::new();
        let n = read_length_encoded_value(reader, &mut buf)?;
        assert_eq!(n, 7);
        assert_eq!(&buf[..n], b"1234567".as_slice());

        Ok(())
    }

    #[test]
    fn parse() -> anyhow::Result<()> {
        let rdb = parse_rdb_file("dump.rdb")?;
        println!("{rdb:?}");
        Ok(())
    }
}
