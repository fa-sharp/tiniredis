//! RESP protocol implementation
//!
//! For details on the design and inspiration of this module,
//! see [this article](https://dpbriggs.ca/blog/Implementing-A-Copyless-Redis-Protocol-in-Rust-With-Parsing-Combinators)

mod base;
pub mod constants;
mod data;
mod decoder;
mod encoder;
mod errors;

pub use errors::RedisParseError;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

/// Tokio codec that can encode and decode RESP frames
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

/// Represents a parsed RESP value
#[derive(Debug, PartialEq)]
pub enum RedisValue {
    String(Bytes),
    SimpleString(Bytes),
    Error(Bytes),
    Int(i64),
    Array(Vec<RedisValue>),
    NilArray,
    NilString,
}

/// References to values within the raw RESP response bytes
enum RedisValueRef {
    String(BufWindow),
    Error(BufWindow),
    Int(i64),
    Array(Vec<RedisValueRef>),
    NilArray,
    NilString,
}

impl RedisValue {
    /// If value is a string, get the bytes
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            RedisValue::String(bytes) | RedisValue::SimpleString(bytes) => Some(bytes),
            _ => None,
        }
    }
    /// If value is a string, turn into owned bytes
    pub fn into_bytes(self) -> Option<Bytes> {
        match self {
            RedisValue::String(bytes) | RedisValue::SimpleString(bytes) => Some(bytes),
            _ => None,
        }
    }
}

impl RedisValueRef {
    /// Get the underlying Redis value that this window is pointing at
    fn extract_redis_value(self, buf: &Bytes) -> Result<RedisValue, RedisParseError> {
        Ok(match self {
            RedisValueRef::String(window) => RedisValue::String(window.as_bytes(buf)),
            RedisValueRef::Error(window) => RedisValue::Error(window.as_bytes(buf)),
            RedisValueRef::Int(int) => RedisValue::Int(int),
            RedisValueRef::Array(elems) => RedisValue::Array(
                elems
                    .into_iter()
                    .map(|value_ref| value_ref.extract_redis_value(buf))
                    .collect::<Result<_, _>>()?,
            ),
            RedisValueRef::NilArray => RedisValue::NilArray,
            RedisValueRef::NilString => RedisValue::NilString,
        })
    }
}

/// The indeces within a buffer (exclusive end) to get the output type
#[derive(Debug, PartialEq)]
struct BufWindow(usize, usize);

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
