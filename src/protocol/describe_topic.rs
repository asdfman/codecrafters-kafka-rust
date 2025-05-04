use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    metadata::{read_cluster_metadata, MetadataFile},
    protocol::{ErrorCode, Response},
};

use super::{CompactArray, Deserialize, RequestHeader, Serialize, TextData};

#[derive(Debug)]
pub struct DescribeTopicPartitionsRequest {
    topics_array: CompactArray<TopicRequestItem>,
    response_partition_limit: i32,
    cursor: i8,
}

#[derive(Debug)]
pub struct TopicRequestItem {
    pub topic_name: TextData,
}

impl Deserialize for TopicRequestItem {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let topic = Self {
            topic_name: TextData::deserialize(bytes),
        };
        bytes.advance(1); // tag buffer
        topic
    }
}

impl Deserialize for DescribeTopicPartitionsRequest {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let topics_array = CompactArray::<TopicRequestItem>::deserialize(bytes);
        let response_partition_limit = bytes.get_i32();
        let cursor = bytes.get_i8();
        Self {
            topics_array,
            response_partition_limit,
            cursor,
        }
    }
}

#[derive(Debug)]
pub struct DescribeTopicPartitionsResponse {
    topics: CompactArray<Topic>,
}
impl Serialize for DescribeTopicPartitionsResponse {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i32(0); // throttle time
        self.topics.serialize(bytes);
        bytes.put_u8(255); // next cursor
        bytes.put_i8(0); // tag buffer
    }
}

#[derive(Debug)]
pub struct Topic {
    error_code: i16,
    topic_name: TextData,
    topic_id: i128,
    is_internal: bool,
    partitions: CompactArray<Partition>,
    operations: i32,
}
impl Serialize for Topic {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i16(self.error_code);
        self.topic_name.serialize(bytes);
        bytes.put_i128(self.topic_id);
        bytes.put_i8(if self.is_internal { 1 } else { 0 });
        self.partitions.serialize(bytes);
        bytes.put_i32(self.operations);
        bytes.put_i8(0); // tag buffer
    }
}

#[derive(Debug)]
pub struct Partition {
    pub error_code: ErrorCode,
    pub partition_index: i32,
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub replica_nodes: CompactArray<i32>,
    pub isr_nodes: CompactArray<i32>,
    pub eligible_nodes: CompactArray<i32>,
    pub last_known_elr: CompactArray<i32>,
    pub offline_replicas: CompactArray<i32>,
}
impl Serialize for Partition {
    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_i16(self.error_code.clone() as i16);
        bytes.put_i32(self.partition_index);
        bytes.put_i32(self.leader_id);
        bytes.put_i32(self.leader_epoch);
        self.replica_nodes.serialize(bytes);
        self.isr_nodes.serialize(bytes);
        self.eligible_nodes.serialize(bytes);
        self.last_known_elr.serialize(bytes);
        self.offline_replicas.serialize(bytes);
        bytes.put_i8(0); // tag buffer
    }
}

pub fn describe_topic_partitions_handler(bytes: &mut Bytes, header: RequestHeader) -> Bytes {
    let metadata = read_cluster_metadata().unwrap();
    let req = DescribeTopicPartitionsRequest::deserialize(bytes);
    //dbg!(&req);
    let mut topics = vec![];
    for topic in req.topics_array.array.iter() {
        topics.push(handle_topic(&topic.topic_name.data, &metadata));
    }
    let response = Response::new(
        header.correlation_id,
        DescribeTopicPartitionsResponse {
            topics: CompactArray::new(topics),
        },
    );
    response.into()
}

fn handle_topic(topic_name: &str, metadata: &MetadataFile) -> Topic {
    let mut topics = metadata.get_topics();
    if let Some(topic) = topics.find(|x| x.topic_name.data == topic_name) {
        let partitions: Vec<Partition> = metadata
            .get_topic_partitions(&topic.uuid)
            .enumerate()
            .map(|(idx, x)| x.into_partition_response(idx as i32))
            .collect();
        Topic {
            error_code: ErrorCode::NoError as i16,
            topic_name: topic.topic_name.clone(),
            topic_id: topic.uuid,
            is_internal: false,
            partitions: CompactArray::new(partitions),
            operations: 3576,
        }
    } else {
        Topic {
            error_code: ErrorCode::UnknownTopicOrPartition as i16,
            topic_name: TextData {
                data: topic_name.to_owned(),
            },
            topic_id: 0,
            is_internal: false,
            partitions: CompactArray { array: vec![] },
            operations: 0,
        }
    }
}
