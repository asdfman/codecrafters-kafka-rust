use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct Response {
    message_size: i32,
    correlation_id: i32,
    error_code: i16,
}

impl Response {
    pub fn new(correlation_id: i32, error_code: i16) -> Self {
        Self {
            message_size: 0,
            correlation_id,
            error_code,
        }
    }
}

impl From<Response> for Bytes {
    fn from(response: Response) -> Self {
        let mut bytes = BytesMut::new();
        bytes.put_i32(response.message_size);
        bytes.put_i32(response.correlation_id);
        bytes.put_i16(response.error_code);
        bytes.freeze()
    }
}
