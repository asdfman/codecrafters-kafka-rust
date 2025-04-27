use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::protocol::{ClientId, ErrorCode, Response};

use super::{CompactArray, Deserialize, Request, RequestHeader, Serialize, TextData};

#[derive(Debug)]
pub struct DescribeTopicPartitionsRequest {
    topics_array: CompactArray<TextData>,
    response_partition_limit: i32,
    cursor: i8,
}

impl Deserialize for DescribeTopicPartitionsRequest {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let topics_array = CompactArray::<TextData>::deserialize(bytes);
        println!("topics_array: {:?}", topics_array);
        let response_partition_limit = bytes.get_i32();
        println!("response_partition_limit: {:?}", response_partition_limit);
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
        bytes.put_i32(0);
        self.topics.serialize(bytes);
        bytes.put_u8(255); // next cursor
        bytes.put_i8(0); // tag buffer
    }
}

#[derive(Debug)]
struct Topic {
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
struct Partition;
impl Serialize for Partition {
    fn serialize(&self, _bytes: &mut BytesMut) {
        // Implement serialization logic here
    }
}
impl Deserialize for Partition {
    fn deserialize(_bytes: &mut Bytes) -> Self {
        // Implement deserialization logic here
        Self
    }
}

pub fn describe_topic_partitions_handler(bytes: &mut Bytes, header: RequestHeader) -> Bytes {
    let req = DescribeTopicPartitionsRequest::deserialize(bytes);
    println!("DescribeTopicPartitionsRequest: {:?}", req);
    let topics = CompactArray::new(vec![Topic {
        error_code: ErrorCode::UnknownTopicOrPartition as i16,
        topic_name: TextData {
            data: req.topics_array.array[0].data.clone(),
        },
        topic_id: 0,
        is_internal: false,
        partitions: CompactArray { array: vec![] },
        operations: 0,
    }]);
    let response = Response::new(
        header.correlation_id,
        DescribeTopicPartitionsResponse { topics },
    );
    response.into()
}
