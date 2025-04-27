use crate::protocol::{Api, CompactArray, ErrorCode, Response, Serialize};
use bytes::{BufMut, Bytes, BytesMut};

use super::RequestHeader;

#[derive(Debug)]
pub struct ApiVersionsResponse {
    pub error_code: ErrorCode,
    pub api_versions: CompactArray<ApiVersionsEntry>,
}

#[derive(Debug)]
pub struct ApiVersionsEntry {
    pub api_key: i16,
    pub min_supported_ver: i16,
    pub max_supported_ver: i16,
}

impl Serialize for ApiVersionsEntry {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i16(self.api_key);
        bytes.put_i16(self.min_supported_ver);
        bytes.put_i16(self.max_supported_ver);
        bytes.put_i8(0); // tag buffer
    }
}

impl Serialize for ApiVersionsResponse {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i16(self.error_code.clone() as i16);
        self.api_versions.serialize(bytes);
        bytes.put_i32(0); // throttle time 0
        bytes.put_i8(0); // tag buffer
    }
}

pub fn api_versions_handler(header: RequestHeader) -> Bytes {
    let vec = Api::get_vec()
        .iter()
        .map(|x| {
            let (min_ver, max_ver) = x.versions();
            ApiVersionsEntry {
                api_key: x.clone() as i16,
                min_supported_ver: min_ver,
                max_supported_ver: max_ver,
            }
        })
        .collect();
    let body = ApiVersionsResponse {
        error_code: ErrorCode::NoError,
        api_versions: CompactArray::new(vec),
    };
    Response::new_v0(header.correlation_id, body).into()
}
