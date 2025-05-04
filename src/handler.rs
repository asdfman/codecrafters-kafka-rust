use bytes::Bytes;

use crate::protocol::*;

pub fn process_request(mut bytes: Bytes) -> anyhow::Result<Bytes> {
    let header: RequestHeader = RequestHeader::from(&mut bytes);
    let mut api = Api::from(header.request_api_key);
    let (min_ver, max_ver) = api.versions();
    if header.request_api_version < min_ver || header.request_api_version > max_ver {
        api = Api::Invalid;
    }
    if api == Api::DescribeTopicPartitions {
        Ok(describe_topic_partitions_handler(&mut bytes, header))
    } else if api == Api::ApiVersions {
        Ok(api_versions_handler(header))
    } else if api == Api::Fetch {
        fetch_handler(&mut bytes, header)
    } else {
        Ok(invalid_request_handler(header))
    }
}

fn invalid_request_handler(header: RequestHeader) -> Bytes {
    Response::new_v0(
        header.correlation_id,
        EmptyResponseBody {
            error_code: ErrorCode::Unsupported,
        },
    )
    .into()
}
