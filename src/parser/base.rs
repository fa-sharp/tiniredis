//!  RESP base parsers

use super::*;

/// Try to get a RESP word from `buf` starting at index `pos`. If found, return
/// the indeces of the word, and the next index to search from.
pub fn word(buf: &BytesMut, pos: usize) -> Option<(BufWindow, usize)> {
    if buf.len() <= pos {
        return None;
    }

    // Look for position of `\n` to find a CRLF
    if let Some(crlf_pos) = memchr(b'\n', &buf[pos..]) {
        let end_pos = pos + crlf_pos - 1;
        let split = BufWindow(pos, end_pos);

        Some((split, end_pos + 2))
    } else {
        None
    }
}

pub fn int(buf: &BytesMut, pos: usize) -> Result<Option<(i64, usize)>, RedisParseError> {
    match word(buf, pos) {
        Some((window, next_pos)) => {
            match std::str::from_utf8(window.as_slice(buf))
                .map_err(|_| RedisParseError::InvalidUtf8)?
                .parse()
            {
                Ok(int) => Ok(Some((int, next_pos))),
                Err(_) => Err(RedisParseError::ParseInt),
            }
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn next_word() {
        let buf = BytesMut::from(b"*1\r".as_slice());
        assert_eq!(word(&buf, 0), None);

        let buf = BytesMut::from(b"*1\r\n".as_slice());
        assert_eq!(word(&buf, 0), Some((BufWindow(0, 2), 4)));

        let buf = BytesMut::from(b"*1\r\n$4\r\nPING\r\n".as_slice());
        assert_eq!(word(&buf, 4), Some((BufWindow(4, 6), 8)));
    }
}
