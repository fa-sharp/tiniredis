use std::io::{Read, Write};

use crc_fast::{CrcAlgorithm, Digest};

/// Reader wrapper that calculates CRC64 checksum as it reads
pub struct Crc64Reader<R> {
    reader: R,
    checksum: crc_fast::Digest,
}

impl<R> Crc64Reader<R> {
    pub fn new(reader: R) -> Self {
        let checksum = Digest::new(CrcAlgorithm::Crc64Redis);
        Self { reader, checksum }
    }

    pub fn checksum(&self) -> u64 {
        self.checksum.finalize()
    }
}

impl<R: Read> Read for Crc64Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.reader.read(buf)?;
        self.checksum.update(&buf[..n]);
        Ok(n)
    }
}

/// Writer wrapper that calculates CRC64 checksum as it writes
pub struct Crc64Writer<W> {
    writer: W,
    checksum: crc_fast::Digest,
}

impl<W> Crc64Writer<W> {
    pub fn new(writer: W) -> Self {
        let checksum = Digest::new(CrcAlgorithm::Crc64Redis);
        Self { writer, checksum }
    }

    pub fn checksum(&self) -> u64 {
        self.checksum.finalize()
    }
}

impl<W: Write> Write for Crc64Writer<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.writer.write(buf)?;
        self.checksum.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
