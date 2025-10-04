use bytes::Bytes;
use tinikeyval_protocol::RedisValue;

use crate::error::Error;

/// A parsed Redis value
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(Bytes),
    Array(Vec<Value>),
    Int(i64),
    Nil,
}

impl TryFrom<RedisValue> for Value {
    type Error = Error;

    fn try_from(value: RedisValue) -> Result<Self, Self::Error> {
        Ok(match value {
            RedisValue::String(bytes) | RedisValue::SimpleString(bytes) => Value::String(bytes),
            RedisValue::Error(bytes) => Err(Error::ResponseError(bytes))?,
            RedisValue::Int(i) => Value::Int(i),
            RedisValue::Array(values) => {
                let converted = values.into_iter().map(Value::try_from);
                Value::Array(converted.collect::<Result<_, _>>()?)
            }
            RedisValue::NilArray | RedisValue::NilString => Value::Nil,
        })
    }
}
