use bytes::{Buf, BufMut, Bytes, BytesMut};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

use super::VarIntUnsigned;

#[derive(EnumIter, PartialEq, Clone)]
#[repr(i16)]
pub enum Api {
    Invalid,
    ApiVersions = 18,
    DescribeTopicPartitions = 75,
}

#[derive(Debug, Clone)]
#[repr(i16)]
pub enum ErrorCode {
    NoError = 0,
    UnknownTopicOrPartition = 3,
    Unsupported = 35,
}

impl From<i16> for Api {
    fn from(value: i16) -> Self {
        match value {
            18 => Self::ApiVersions,
            75 => Self::DescribeTopicPartitions,
            _ => Self::Invalid,
        }
    }
}

impl Api {
    pub fn get_vec() -> Vec<Api> {
        Api::iter().filter(|x| *x != Api::Invalid).collect()
    }

    pub fn versions(&self) -> (i16, i16) {
        match self {
            Self::ApiVersions => (0, 4),
            Self::DescribeTopicPartitions => (0, 0),
            Self::Invalid => (0, 0),
        }
    }
}

#[derive(Debug)]
pub struct ClientId {
    id: String,
}
impl Deserialize for ClientId {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let length = bytes.get_i16();
        let id = String::from_utf8_lossy(&bytes.copy_to_bytes(length as usize)).to_string();
        Self { id }
    }
}

#[derive(Debug, Clone)]
pub struct TextData {
    pub data: String,
}

impl Deserialize for TextData {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let length = VarIntUnsigned::deserialize(bytes).0 - 1;
        let data = String::from_utf8_lossy(&bytes.copy_to_bytes(length as usize)).to_string();
        Self { data }
    }
}

impl Serialize for TextData {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i8(self.data.len() as i8 + 1);
        bytes.put_slice(self.data.as_bytes());
    }
}

#[derive(Debug, Clone)]
pub struct CompactArray<T> {
    pub array: Vec<T>,
}

impl<T> CompactArray<T> {
    pub fn new(array: Vec<T>) -> Self {
        Self { array }
    }
}

impl<T: Deserialize> Deserialize for CompactArray<T> {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let length = VarIntUnsigned::deserialize(bytes);
        Self {
            array: (1..length.0).map(|_| T::deserialize(bytes)).collect(),
        }
    }
}

impl<T: Serialize> Serialize for CompactArray<T> {
    fn serialize(&self, bytes: &mut BytesMut) {
        VarIntUnsigned(self.array.len() as u64 + 1).serialize(bytes);
        for item in &self.array {
            item.serialize(bytes);
        }
    }
}

impl Serialize for i32 {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i32(*self);
    }
}

pub trait Serialize {
    fn serialize(&self, bytes: &mut BytesMut);
}
pub trait Deserialize {
    fn deserialize(bytes: &mut Bytes) -> Self;
}
