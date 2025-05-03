use crate::protocol::{response, CompactArray};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{Deserialize, ErrorCode, RequestHeader, Serialize};

#[derive(Debug)]
pub struct FetchRequest {
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    session_id: i32,
    session_epoch: i32,
    topics: CompactArray<FetchTopic>,
}
impl Deserialize for FetchRequest {
    fn deserialize(bytes: &mut bytes::Bytes) -> Self {
        Self {
            max_wait_ms: bytes.get_i32(),
            min_bytes: bytes.get_i32(),
            max_bytes: bytes.get_i32(),
            isolation_level: bytes.get_i8(),
            session_id: bytes.get_i32(),
            session_epoch: bytes.get_i32(),
            topics: CompactArray::<FetchTopic>::deserialize(bytes),
        }
    }
}

#[derive(Debug)]
pub struct FetchTopic {
    topic_id: i128,
}
impl Deserialize for FetchTopic {
    fn deserialize(bytes: &mut bytes::Bytes) -> Self {
        Self {
            topic_id: bytes.get_i128(),
        }
    }
}

#[derive(Debug)]
pub struct FetchResponseBody {
    error_code: ErrorCode,
    session_id: i32,
    responses: CompactArray<FetchTopicResponse>,
}
impl Serialize for FetchResponseBody {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i32(0); // throttle time
        bytes.put_i16(self.error_code.clone() as i16);
        bytes.put_i32(self.session_id);
        self.responses.serialize(bytes);
        bytes.put_i8(0); // tag buffer
                         //bytes.put_u8(255); // next cursor
    }
}

#[derive(Debug)]
pub struct FetchTopicResponse {
    topic_id: i128,
}
impl Serialize for FetchTopicResponse {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i128(self.topic_id);
    }
}

pub fn fetch_handler(bytes: &mut bytes::Bytes, header: RequestHeader) -> Bytes {
    let fetch_request = FetchRequest::deserialize(bytes);
    dbg!(&fetch_request);
    let response_body = FetchResponseBody {
        error_code: ErrorCode::NoError,
        session_id: fetch_request.session_id,
        responses: CompactArray::new(vec![]),
    };
    let response = response::Response::new(header.correlation_id, response_body);
    dbg!(&response);
    response.into()
}
