use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct Response {
    message_size: i32,
    correlation_id: i32,
}

impl Response {
    pub fn get_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_i32(self.message_size);
        bytes.put_i32(self.correlation_id);
        bytes.freeze()
    }

    pub fn new(correlation_id: i32) -> Self {
        Self {
            message_size: 0,
            correlation_id,
        }
    }
}
