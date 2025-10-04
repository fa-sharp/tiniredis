use std::io::{Read, Write};

use crc64::Crc64;

/// Reader wrapper that calculates CRC64 checksum as it reads
pub struct Crc64Reader<R> {
    reader: R,
    checksum: Crc64,
}

impl<R> Crc64Reader<R> {
    pub fn new(reader: R) -> Self {
        let checksum = Crc64::new();
        Self { reader, checksum }
    }

    pub fn checksum(&self) -> u64 {
        self.checksum.get()
    }
}

impl<R: Read> Read for Crc64Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.reader.read(buf)?;
        self.checksum.write_all(&buf[..n])?;
        Ok(n)
    }
}
