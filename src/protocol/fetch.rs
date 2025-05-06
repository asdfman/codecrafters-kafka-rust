use std::collections::HashSet;

use crate::{
    metadata::{
        read_cluster_metadata, read_partition_metadata, MetadataFile, RecordBatch, TopicRecord,
    },
    protocol::{CompactArray, Response, TextData},
};
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{Deserialize, ErrorCode, RequestHeader, Serialize, VarIntUnsigned};

#[derive(Debug)]
pub struct FetchRequest {
    _max_wait_ms: i32,
    _min_bytes: i32,
    _max_bytes: i32,
    _isolation_level: i8,
    session_id: i32,
    _session_epoch: i32,
    topics: CompactArray<FetchTopic>,
}
impl Deserialize for FetchRequest {
    fn deserialize(bytes: &mut bytes::Bytes) -> Self {
        Self {
            _max_wait_ms: bytes.get_i32(),
            _min_bytes: bytes.get_i32(),
            _max_bytes: bytes.get_i32(),
            _isolation_level: bytes.get_i8(),
            session_id: bytes.get_i32(),
            _session_epoch: bytes.get_i32(),
            topics: CompactArray::<FetchTopic>::deserialize(bytes),
        }
    }
}

#[derive(Debug)]
pub struct FetchTopic {
    topic_id: i128,
    partitions: CompactArray<FetchTopicRequestPartition>,
}
impl Deserialize for FetchTopic {
    fn deserialize(bytes: &mut bytes::Bytes) -> Self {
        Self {
            topic_id: bytes.get_i128(),
            partitions: CompactArray::<FetchTopicRequestPartition>::deserialize(bytes),
        }
    }
}

#[derive(Debug)]
pub struct FetchTopicRequestPartition {
    partition_index: i32,
    _current_leader_epoch: i32,
    _fetch_offset: i64,
    _log_start_offset: i64,
    _partition_max_bytes: i32,
}
impl Deserialize for FetchTopicRequestPartition {
    fn deserialize(bytes: &mut bytes::Bytes) -> Self {
        Self {
            partition_index: bytes.get_i32(),
            _current_leader_epoch: bytes.get_i32(),
            _fetch_offset: bytes.get_i64(),
            _log_start_offset: bytes.get_i64(),
            _partition_max_bytes: bytes.get_i32(),
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
    record_batches: Vec<RecordBatch>,
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

        let mut record_bytes = BytesMut::new();
        for record in &self.record_batches {
            record.serialize(&mut record_bytes);
        }
        let length = VarIntUnsigned(record_bytes.len() as u64 + 1);
        length.serialize(bytes);
        bytes.put_slice(&record_bytes);
        bytes.put_i8(0); // tag buffer
    }
}

pub fn fetch_handler(bytes: &mut bytes::Bytes, header: RequestHeader) -> Result<Bytes> {
    let metadata = read_cluster_metadata().unwrap();
    let req = FetchRequest::deserialize(bytes);
    let mut responses = vec![];
    for topic in req.topics.array.iter() {
        if let Some(topic_found) = metadata.get_topics().find(|x| x.uuid == topic.topic_id) {
            let partition_ids = topic
                .partitions
                .array
                .iter()
                .map(|x| x.partition_index)
                .collect::<HashSet<_>>();
            responses.push(topic_handler(topic_found, &metadata, partition_ids));
        } else {
            responses.push(topic_not_found_response(topic));
        }
    }
    let response_body = FetchResponseBody {
        error_code: ErrorCode::NoError,
        session_id: req.session_id,
        responses: CompactArray::new(responses),
    };
    let response = Response::new(header.correlation_id, response_body);
    Ok(response.into())
}

fn topic_handler(
    topic_record: &TopicRecord,
    metadata: &MetadataFile,
    partition_ids: HashSet<i32>,
) -> FetchTopicResponse {
    let mut partitions = vec![];
    let partition_iter = metadata.get_topic_partitions(&topic_record.uuid);
    for partition in partition_iter {
        if !partition_ids.contains(&partition.partition_id) {
            continue;
        }
        let mut records = vec![];
        if let Ok(partition_metadata) =
            read_partition_metadata(topic_record.topic_name.data.clone(), partition.partition_id)
        {
            for record in partition_metadata.record_batches {
                records.push(record);
            }
        }
        partitions.push(FetchTopicPartition {
            partition_index: partition.partition_id,
            error_code: ErrorCode::NoError,
            high_watermark: 0,
            last_stable_offset: 0,
            log_start_offset: 0,
            aborted_transactions: CompactArray::new(vec![]),
            preferred_read_replica: 0,
            record_batches: records,
        });
    }
    FetchTopicResponse {
        topic_id: topic_record.uuid,
        partitions: CompactArray::new(partitions),
    }
}

fn topic_not_found_response(topic: &FetchTopic) -> FetchTopicResponse {
    let partitions = vec![FetchTopicPartition {
        partition_index: 0,
        error_code: ErrorCode::UnknownTopic,
        high_watermark: 0,
        last_stable_offset: 0,
        log_start_offset: 0,
        aborted_transactions: CompactArray::new(vec![]),
        preferred_read_replica: 0,
        record_batches: vec![],
    }];
    FetchTopicResponse {
        topic_id: topic.topic_id,
        partitions: CompactArray::new(partitions),
    }
}
