//! RESP data parsers

use super::*;

/// A parsing result, containing the position and type of the value found, as well as the next
/// index to search from.
pub type RedisParseResult = Result<Option<(RedisValueRef, usize)>, RedisParseError>;

/// Top-level parse function. Looks at the starting tag and parses the data accordingly.
pub fn parse(buf: &BytesMut, pos: usize) -> RedisParseResult {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[pos] {
        constants::SIMPLE_STRING_TAG => simple_string(buf, pos + 1),
        constants::ERROR_TAG => error(buf, pos + 1),
        constants::BULK_STRING_TAG => bulk_string(buf, pos + 1),
        constants::INT_TAG => resp_int(buf, pos + 1),
        constants::ARRAY_TAG => array(buf, pos + 1),
        u => Err(RedisParseError::UnknownStartingByte(u)),
    }
}

pub fn simple_string(buf: &BytesMut, pos: usize) -> RedisParseResult {
    match base::word(buf, pos) {
        Some((window, next_pos)) => Ok(Some((RedisValueRef::String(window), next_pos))),
        None => Ok(None),
    }
}

pub fn bulk_string(buf: &BytesMut, pos: usize) -> RedisParseResult {
    match base::int(buf, pos)? {
        Some((bad_len, _)) if bad_len < -1 => Err(RedisParseError::BadBulkStringSize(bad_len)),
        Some((-1, next_pos)) => Ok(Some((RedisValueRef::NilString, next_pos))),
        Some((len, next_pos)) => {
            let end_pos = next_pos + len as usize;
            if buf.len() < end_pos + constants::CRLF_LEN {
                Ok(None)
            } else {
                Ok(Some((
                    RedisValueRef::String(BufWindow(next_pos, end_pos)),
                    end_pos + constants::CRLF_LEN,
                )))
            }
        }
        None => Ok(None),
    }
}

pub fn resp_int(buf: &BytesMut, pos: usize) -> RedisParseResult {
    match base::int(buf, pos)? {
        Some((int, next_pos)) => Ok(Some((RedisValueRef::Int(int), next_pos))),
        None => Ok(None),
    }
}

pub fn array(buf: &BytesMut, pos: usize) -> RedisParseResult {
    match base::int(buf, pos)? {
        Some((bad_len, _)) if bad_len < -1 => Err(RedisParseError::BadArraySize(bad_len)),
        Some((-1, next_pos)) => Ok(Some((RedisValueRef::NilArray, next_pos))),
        Some((len, next_pos)) => {
            let mut elems = Vec::with_capacity(len as usize);
            let mut current_pos = next_pos;
            for _ in 0..len {
                let Some((elem, next_pos)) = parse(buf, current_pos)? else {
                    return Ok(None);
                };
                elems.push(elem);
                current_pos = next_pos;
            }

            Ok(Some((RedisValueRef::Array(elems), current_pos)))
        }
        None => Ok(None),
    }
}

pub fn error(buf: &BytesMut, pos: usize) -> RedisParseResult {
    match base::word(buf, pos) {
        Some((window, next_pos)) => Ok(Some((RedisValueRef::Error(window), next_pos))),
        None => Ok(None),
    }
}
