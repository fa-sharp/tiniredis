//! RESP protocol parser
//!
//! For more info, see [this article](https://dpbriggs.ca/blog/Implementing-A-Copyless-Redis-Protocol-in-Rust-With-Parsing-Combinators)

mod base;
mod data;
mod decoder;
mod errors;

pub use decoder::RespParser;
pub use errors::RedisParseError;

use bytes::{Bytes, BytesMut};
use bytes_utils::Str;
use memchr::memchr;

/// Represents a parsed Redis value, or a value that can be sent as a response
#[derive(Debug)]
pub enum RedisValue {
    String(Str),
    Error(Str),
    Int(i64),
    Array(Vec<RedisValue>),
    NulArray,
    NulString,
}

/// The indeces within a buffer (exclusive end) to get the output type
#[derive(Debug, PartialEq)]
struct BufWindow(usize, usize);

/// References to values within the raw RESP response bytes
enum RedisValueRef {
    String(BufWindow),
    Error(BufWindow),
    Int(i64),
    Array(Vec<RedisValueRef>),
    NulArray,
    NulString,
}

impl RedisValueRef {
    /// Get the underlying Redis value that this window is pointing at
    fn extract_redis_value(self, buf: &Bytes) -> Result<RedisValue, RedisParseError> {
        Ok(match self {
            RedisValueRef::String(window) => RedisValue::String(
                Str::from_inner(window.as_bytes(buf)).map_err(|_| RedisParseError::InvalidUtf8)?,
            ),
            RedisValueRef::Error(window) => RedisValue::Error(
                Str::from_inner(window.as_bytes(buf)).map_err(|_| RedisParseError::InvalidUtf8)?,
            ),
            RedisValueRef::Int(int) => RedisValue::Int(int),
            RedisValueRef::Array(elems) => RedisValue::Array(
                elems
                    .into_iter()
                    .map(|value_ref| value_ref.extract_redis_value(buf))
                    .collect::<Result<_, _>>()?,
            ),
            RedisValueRef::NulArray => RedisValue::NulArray,
            RedisValueRef::NulString => RedisValue::NulString,
        })
    }
}

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
