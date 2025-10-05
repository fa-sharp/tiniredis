pub const META_FLAG: u8 = 0xFA;
pub const DB_FLAG: u8 = 0xFE;
pub const DB_SIZE_FLAG: u8 = 0xFB;
pub const END_FILE_FLAG: u8 = 0xFF;

/// Expiration in Unix time milliseconds (little endian)
pub const EXPIRY_U64_FLAG: u8 = 0xFC;
/// Expiration in Unix time seconds (little endian)
pub const EXPIRY_U32_FLAG: u8 = 0xFD;

pub const STRING_I8_FLAG: u8 = 0xC0;
pub const STRING_I16_FLAG: u8 = 0xC1;
pub const STRING_I32_FLAG: u8 = 0xC2;

pub const TYPE_STRING_FLAG: u8 = 0x00;
pub const TYPE_LIST_FLAG: u8 = 0x01;
pub const TYPE_SET_FLAG: u8 = 0x02;
