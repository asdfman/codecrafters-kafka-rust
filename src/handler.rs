use bytes::Bytes;

use crate::protocol::*;

const API_VERSIONS_MAX_VER: i16 = 4;
const API_VERSIONS_MIN_VER: i16 = 0;

pub fn handle_request(bytes: Bytes) -> anyhow::Result<Bytes> {
    let req: Request = bytes.into();
    println!("{:?}", req);

    let response = match ApiKeys::from(req.request_api_key) {
        ApiKeys::ApiVersions => api_versions_handler(req),
        _ => Response::new(req.correlation_id, ErrorCode::Unsupported, None),
    };
    println!("{:?}", response);
    Ok(response.into())
}

fn api_versions_handler(req: Request) -> Response<ApiVersionsResponse> {
    if req.request_api_version > 4 {
        return Response::new(req.correlation_id, ErrorCode::Unsupported, None);
    }
    let body = Some(ApiVersionsResponse {
        api_versions: vec![ApiVersionsEntry {
            api_key: 18,
            min_supported_ver: API_VERSIONS_MIN_VER,
            max_supported_ver: API_VERSIONS_MAX_VER,
        }],
    });
    Response::new(req.correlation_id, ErrorCode::NoError, body)
}
