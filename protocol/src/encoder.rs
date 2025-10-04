//! RESP encoder

use bytes::BufMut;
use tokio_util::codec::Encoder;
use tracing::trace;

use super::*;

impl Encoder<RespValue> for RespCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            RespValue::String(str) => {
                dst.put_u8(constants::BULK_STRING_TAG);
                dst.put_slice(str.len().to_string().as_bytes());
                dst.put_slice(constants::CRLF);
                dst.put_slice(&str);
                dst.put_slice(constants::CRLF);
            }
            RespValue::SimpleString(str) => {
                dst.put_u8(constants::SIMPLE_STRING_TAG);
                dst.put_slice(&str);
                dst.put_slice(constants::CRLF);
            }
            RespValue::Error(str) => {
                dst.put_u8(constants::ERROR_TAG);
                dst.put_slice(&str);
                dst.put_slice(constants::CRLF);
            }
            RespValue::Int(int) => {
                dst.put_u8(constants::INT_TAG);
                dst.put_slice(int.to_string().as_bytes());
                dst.put_slice(constants::CRLF);
            }
            RespValue::Array(values) => {
                dst.put_u8(constants::ARRAY_TAG);
                dst.put_slice(values.len().to_string().as_bytes());
                dst.put_slice(constants::CRLF);
                for value in values {
                    self.encode(value, dst)?;
                }
            }
            RespValue::NilArray => {
                dst.put_slice(b"*-1");
                dst.put_slice(constants::CRLF);
            }
            RespValue::NilString => {
                dst.put_slice(b"$-1");
                dst.put_slice(constants::CRLF);
            }
        }

        trace!(
            "Building raw value: {}",
            String::from_utf8_lossy(dst).escape_debug()
        );

        Ok(())
    }
}
