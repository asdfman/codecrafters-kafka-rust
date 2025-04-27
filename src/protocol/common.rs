use bytes::{Buf, BufMut, Bytes, BytesMut};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

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

#[derive(Debug)]
pub struct TextData {
    pub data: String,
}

impl Deserialize for TextData {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let length = bytes.get_i8() - 1;
        println!("length: {}", length);
        let data = String::from_utf8_lossy(&bytes.copy_to_bytes(length as usize)).to_string();
        println!("data: {:?}", data);
        Self { data }
    }
}

impl Serialize for TextData {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i8(self.data.len() as i8 + 1);
        bytes.put_slice(self.data.as_bytes());
    }
}

#[derive(Debug)]
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
        let length = bytes.get_i8();
        println!("length: {}", length);
        Self {
            array: (1..length).map(|_| T::deserialize(bytes)).collect(),
        }
    }
}

impl<T: Serialize> Serialize for CompactArray<T> {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i8(self.array.len() as i8 + 1);
        for item in &self.array {
            item.serialize(bytes);
        }
    }
}

pub trait Serialize {
    fn serialize(&self, bytes: &mut BytesMut);
}
pub trait Deserialize {
    fn deserialize(bytes: &mut Bytes) -> Self;
}
