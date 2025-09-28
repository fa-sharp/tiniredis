use tokio_util::codec::Decoder;

use super::*;

/// RESP value parser that works as a tokio decoder
#[derive(Default)]
pub struct RespParser;

impl Decoder for RespParser {
    type Item = RedisValue;
    type Error = RedisParseError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.is_empty() {
            return Ok(None);
        }

        match data::parse(buf, 0)? {
            Some((window, next_pos)) => {
                // Value parsed successfully, split buffer and take bytes
                let data = buf.split_to(next_pos);
                Ok(Some(window.extract_redis_value(&data.freeze())?))
            }
            None => Ok(None),
        }
    }
}
