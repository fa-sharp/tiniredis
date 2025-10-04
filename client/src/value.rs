use bytes::Bytes;
use tinikeyval_protocol::RespValue;

use crate::error::ClientError;

/// A parsed value converted from RESP
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(Bytes),
    Array(Vec<Value>),
    Int(i64),
    Nil,
}

impl TryFrom<RespValue> for Value {
    type Error = ClientError;

    fn try_from(value: RespValue) -> Result<Self, Self::Error> {
        Ok(match value {
            RespValue::String(bytes) | RespValue::SimpleString(bytes) => Value::String(bytes),
            RespValue::Error(bytes) => Err(ClientError::ResponseError(bytes))?,
            RespValue::Int(i) => Value::Int(i),
            RespValue::Array(values) => {
                let converted = values.into_iter().map(Value::try_from);
                Value::Array(converted.collect::<Result<_, _>>()?)
            }
            RespValue::NilArray | RespValue::NilString => Value::Nil,
        })
    }
}
