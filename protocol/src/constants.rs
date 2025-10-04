//! Various constants for RESP and Redis requests and responses

use bytes::Bytes;

use crate::RespValue;

pub const CRLF: &[u8; 2] = b"\r\n";
pub const CRLF_LEN: usize = CRLF.len();

// RESP tags
pub const SIMPLE_STRING_TAG: u8 = b'+';
pub const ERROR_TAG: u8 = b'-';
pub const BULK_STRING_TAG: u8 = b'$';
pub const INT_TAG: u8 = b':';
pub const ARRAY_TAG: u8 = b'*';

// Common responses
pub const OK: RespValue = RespValue::SimpleString(Bytes::from_static(b"OK"));
