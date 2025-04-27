use crate::protocol::Deserialize;
use bytes::{Buf, Bytes};

use super::ClientId;

pub struct Request<T: Deserialize> {
    pub header: RequestHeader,
    pub body: T,
}

pub struct RequestHeader {
    pub message_size: i32,
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: ClientId,
}

pub struct EmptyRequestBody;
impl Deserialize for EmptyRequestBody {
    fn deserialize(_bytes: &mut Bytes) -> Self {
        Self
    }
}

impl From<&mut Bytes> for RequestHeader {
    fn from(bytes: &mut Bytes) -> Self {
        let header = Self {
            message_size: bytes.get_i32(),
            request_api_key: bytes.get_i16(),
            request_api_version: bytes.get_i16(),
            correlation_id: bytes.get_i32(),
            client_id: ClientId::deserialize(bytes),
        };
        bytes.advance(1); // tag buffer
        header
    }
}
