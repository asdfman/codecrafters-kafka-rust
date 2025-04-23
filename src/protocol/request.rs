use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct Request {
    pub message_size: i32,
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
}

impl Request {
    pub fn get_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_i16(self.request_api_key);
        bytes.put_i16(self.request_api_version);
        bytes.put_i32(self.correlation_id);
        bytes.freeze()
    }
}

impl From<Bytes> for Request {
    fn from(mut bytes: Bytes) -> Self {
        Self {
            message_size: bytes.get_i32(),
            request_api_key: bytes.get_i16(),
            request_api_version: bytes.get_i16(),
            correlation_id: bytes.get_i32(),
        }
    }
}
