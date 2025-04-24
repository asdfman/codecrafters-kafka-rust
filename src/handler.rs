use bytes::Bytes;

use crate::protocol::*;

pub fn handle_request(bytes: Bytes) -> anyhow::Result<Bytes> {
    let req: Request = bytes.into();
    println!("{:?}", req);
    let mut api = ApiKeys::from(req.request_api_key);
    let (min_ver, max_ver) = api.versions();
    if req.request_api_version < min_ver || req.request_api_version > max_ver {
        api = ApiKeys::Invalid;
    }
    let response: Bytes = match api {
        ApiKeys::ApiVersions => api_versions_handler(req),
        ApiKeys::DescribeTopicPartitions => describe_topic_handler(req),
        _ => unsupported_response(req.correlation_id),
    };
    Ok(response)
}

fn api_versions_handler(req: Request) -> Bytes {
    let mut body = ApiVersionsResponse {
        api_versions: vec![],
    };
    for api in ApiKeys::get_vec() {
        let (min_ver, max_ver) = api.versions();
        body.api_versions.push(ApiVersionsEntry {
            api_key: api as i16,
            min_supported_ver: min_ver,
            max_supported_ver: max_ver,
        });
    }
    Response::new(req.correlation_id, ErrorCode::NoError, Some(body)).into()
}

fn describe_topic_handler(req: Request) -> Bytes {
    todo!();
}

fn unsupported_response(correlation_id: i32) -> Bytes {
    Response::<()>::new(correlation_id, ErrorCode::Unsupported, None).into()
}
