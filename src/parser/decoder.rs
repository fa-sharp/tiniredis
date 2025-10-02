//! RESP decoder

use tokio_util::codec::Decoder;
use tracing::trace;

use super::*;

/// RESP value parser that works as a tokio decoder
#[derive(Debug)]
pub struct RespDecoder;

impl Decoder for RespDecoder {
    type Item = RedisValue;
    type Error = RedisParseError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.is_empty() {
            return Ok(None);
        }

        trace!(
            "Receiving raw value: {}",
            String::from_utf8_lossy(buf).escape_debug()
        );

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
