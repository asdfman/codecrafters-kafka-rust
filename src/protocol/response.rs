use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct Response {
    message_size: i32,
    header: ResponseHeader,
}

#[derive(Debug)]
struct ResponseHeader {
    correlation_id: i32,
}

impl Default for Response {
    fn default() -> Self {
        Self {
            message_size: 0,
            header: ResponseHeader { correlation_id: 7 },
        }
    }
}

impl Response {
    pub fn get_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::new();
        bytes.put_i32(self.message_size);
        bytes.put_i32(self.header.correlation_id);
        bytes.freeze()
    }
}
