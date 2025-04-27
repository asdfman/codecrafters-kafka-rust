use std::fmt::Debug;

use super::{ErrorCode, Serialize};
use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct Response<T: Serialize> {
    correlation_id: i32,
    body: T,
    skip_tag_buffer: bool,
}

impl<T: Serialize> Response<T> {
    pub fn new(correlation_id: i32, body: T) -> Self {
        Self {
            correlation_id,
            body,
            skip_tag_buffer: false,
        }
    }
    pub fn new_v0(correlation_id: i32, body: T) -> Self {
        Self {
            correlation_id,
            body,
            skip_tag_buffer: true,
        }
    }
}

impl<T: Serialize + Debug> From<Response<T>> for Bytes {
    fn from(response: Response<T>) -> Self {
        println!("response: {:?}", response);
        let mut bytes = BytesMut::new();
        bytes.put_bytes(0, 4); // reserve 4 bytes for size
        bytes.put_i32(response.correlation_id);

        // Header V0 has no tag buffer
        if !response.skip_tag_buffer {
            bytes.put_i8(0); // tag buffer
        }

        response.body.serialize(&mut bytes);

        let size = bytes.len() as i32 - 4; // size of message after first 4 bytes
        bytes[0..4].copy_from_slice(&size.to_be_bytes());
        bytes.freeze()
    }
}

#[derive(Debug)]
pub struct EmptyResponseBody {
    pub error_code: ErrorCode,
}
impl Serialize for EmptyResponseBody {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i16(self.error_code.clone() as i16);
    }
}
