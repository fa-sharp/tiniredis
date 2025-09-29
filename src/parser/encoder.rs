use bytes::BufMut;
use tokio_util::codec::Encoder;

use super::*;

/// RESP value encoder that works as a tokio encoder
pub struct RespEncoder;

impl Encoder<RedisValue> for RespEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: RedisValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RedisValue::String(str) => {
                dst.put_u8(b'$');
                dst.put_slice(str.len().to_string().as_bytes());
                dst.put_slice(constants::CRLF);
                dst.put_slice(&str);
                dst.put_slice(constants::CRLF);
            }
            RedisValue::SimpleString(str) => {
                dst.put_u8(b'+');
                dst.put_slice(&str);
                dst.put_slice(constants::CRLF);
            }
            RedisValue::Error(_) => todo!(),
            RedisValue::Int(_) => todo!(),
            RedisValue::Array(_) => todo!(),
            RedisValue::NulArray => todo!(),
            RedisValue::NulString => {
                dst.put_slice(b"$-1");
                dst.put_slice(constants::CRLF);
            }
        }

        println!(
            "Sending raw value: {}",
            String::from_utf8_lossy(dst).escape_debug()
        );

        Ok(())
    }
}
