//! RESP protocol implementation
//!
//! For details on the design and inspiration of this module,
//! see [this article](https://dpbriggs.ca/blog/Implementing-A-Copyless-Redis-Protocol-in-Rust-With-Parsing-Combinators)

mod base;
pub mod constants;
pub mod data;
mod decoder;
mod encoder;
mod errors;

pub use errors::RedisParseError;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

/// Tokio codec that can both encode and decode RESP frames.
#[derive(Debug)]
pub struct RespCodec;
impl RespCodec {
    /// Create a RESP framed I/O interface from on AsyncRead + AsyncWrite resource
    pub fn framed_io<Rw>(inner: Rw) -> Framed<Rw, RespCodec>
    where
        Rw: AsyncRead + AsyncWrite,
    {
        Framed::new(inner, RespCodec)
    }
}

/// Represents a raw RESP value
#[derive(Debug, PartialEq)]
pub enum RespValue {
    String(Bytes),
    SimpleString(Bytes),
    Error(Bytes),
    Int(i64),
    Array(Vec<RespValue>),
    NilArray,
    NilString,
}

/// References to values within the raw RESP response bytes
pub enum RespValueRef {
    String(BufWindow),
    Error(BufWindow),
    Int(i64),
    Array(Vec<RespValueRef>),
    NilArray,
    NilString,
}

impl RespValue {
    /// If value is a string, get the bytes
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            RespValue::String(bytes) | RespValue::SimpleString(bytes) => Some(bytes),
            _ => None,
        }
    }
    /// If value is a string, turn into owned bytes
    pub fn into_bytes(self) -> Option<Bytes> {
        match self {
            RespValue::String(bytes) | RespValue::SimpleString(bytes) => Some(bytes),
            _ => None,
        }
    }
}

impl RespValueRef {
    /// Get the underlying Redis value that this window is pointing at
    fn extract_redis_value(self, buf: &Bytes) -> Result<RespValue, RedisParseError> {
        Ok(match self {
            RespValueRef::String(window) => RespValue::String(window.as_bytes(buf)),
            RespValueRef::Error(window) => RespValue::Error(window.as_bytes(buf)),
            RespValueRef::Int(int) => RespValue::Int(int),
            RespValueRef::Array(elems) => RespValue::Array(
                elems
                    .into_iter()
                    .map(|value_ref| value_ref.extract_redis_value(buf))
                    .collect::<Result<_, _>>()?,
            ),
            RespValueRef::NilArray => RespValue::NilArray,
            RespValueRef::NilString => RespValue::NilString,
        })
    }
}

/// The indeces within a buffer (exclusive end) to get the output type
#[derive(Debug, PartialEq)]
pub struct BufWindow(usize, usize);

impl BufWindow {
    #[inline]
    fn as_slice<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }
    #[inline]
    fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}
