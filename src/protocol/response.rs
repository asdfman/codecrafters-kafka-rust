use bytes::{BufMut, Bytes, BytesMut};

#[derive(Debug)]
pub struct Response<T> {
    correlation_id: i32,
    error_code: ErrorCode,
    body: Option<T>,
}

#[derive(Debug)]
#[repr(i16)]
pub enum ErrorCode {
    NoError = 0,
    Unsupported = 35,
}

impl<T: ResponseBody> Response<T> {
    pub fn new(correlation_id: i32, error_code: ErrorCode, body: Option<T>) -> Self {
        Self {
            correlation_id,
            error_code,
            body,
        }
    }
}

trait ResponseBody {
    fn get_bytes(&self, bytes: &mut BytesMut);
}

impl<T: ResponseBody> From<Response<T>> for Bytes {
    fn from(response: Response<T>) -> Self {
        let mut bytes = BytesMut::new();
        bytes.put_bytes(0, 4); // reserve 4 bytes for size
                               //
        bytes.put_i32(response.correlation_id);
        bytes.put_i16(response.error_code as i16);

        if response.body.is_some() {
            response.body.unwrap().get_bytes(&mut bytes);
        }

        let size = bytes.len() as i32 - 4; // size of message after first 4 bytes
        bytes[0..4].copy_from_slice(&size.to_be_bytes());
        bytes.freeze()
    }
}

#[derive(Debug)]
pub struct ApiVersionsResponse {
    pub api_versions: Vec<ApiVersionsEntry>,
}
#[derive(Debug)]
pub struct ApiVersionsEntry {
    pub api_key: i16,
    pub min_supported_ver: i16,
    pub max_supported_ver: i16,
}

impl ResponseBody for ApiVersionsResponse {
    fn get_bytes(&self, bytes: &mut BytesMut) {
        bytes.put_i8(self.api_versions.len() as i8 + 1); // array length
        for api in &self.api_versions {
            bytes.put_i16(api.api_key);
            bytes.put_i16(api.min_supported_ver);
            bytes.put_i16(api.max_supported_ver);
            bytes.put_i8(0); // tag buffer
        }
        bytes.put_i32(0); // throttle time 0
        bytes.put_i8(0); // tag buffer
    }
}
