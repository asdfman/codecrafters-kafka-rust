use crate::protocol::{CompactArray, Response};
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
    partitions: CompactArray<FetchTopicPartition>,
}
impl Serialize for FetchTopicResponse {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i128(self.topic_id);
        self.partitions.serialize(bytes);
        bytes.put_i8(0); // tag buffer
    }
}

#[derive(Debug)]
pub struct FetchTopicPartition {
    partition_index: i32,
    error_code: ErrorCode,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted_transactions: CompactArray<i32>,
    preferred_read_replica: i32,
    record_batches: CompactArray<i32>,
}
impl Serialize for FetchTopicPartition {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i32(self.partition_index);
        bytes.put_i16(self.error_code.clone() as i16);
        bytes.put_i64(self.high_watermark);
        bytes.put_i64(self.last_stable_offset);
        bytes.put_i64(self.log_start_offset);
        self.aborted_transactions.serialize(bytes);
        bytes.put_i32(self.preferred_read_replica);
        self.record_batches.serialize(bytes);
        bytes.put_i8(0); // tag buffer
    }
}

pub fn fetch_handler(bytes: &mut bytes::Bytes, header: RequestHeader) -> Bytes {
    let req = FetchRequest::deserialize(bytes);
    dbg!(&req);
    let mut responses = vec![];
    for topic in req.topics.array.iter() {
        responses.push(handle_topic(topic));
    }
    let response_body = FetchResponseBody {
        error_code: ErrorCode::NoError,
        session_id: req.session_id,
        responses: CompactArray::new(responses),
    };
    let response = Response::new(header.correlation_id, response_body);
    dbg!(&response);
    response.into()
}

fn handle_topic(topic: &FetchTopic) -> FetchTopicResponse {
    let mut partitions = vec![];
    partitions.push(FetchTopicPartition {
        partition_index: 0,
        error_code: ErrorCode::UnknownTopic,
        high_watermark: 0,
        last_stable_offset: 0,
        log_start_offset: 0,
        aborted_transactions: CompactArray::new(vec![]),
        preferred_read_replica: 0,
        record_batches: CompactArray::new(vec![]),
    });
    FetchTopicResponse {
        topic_id: topic.topic_id,
        partitions: CompactArray::new(partitions),
    }
}
