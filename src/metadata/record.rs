use crate::protocol::{CompactArray, Deserialize, Partition, TextData, VarIntUnsigned};
use bytes::{Buf, Bytes};

#[derive(Debug)]
pub enum RecordType {
    Topic(TopicRecord),
    Partition(PartitionRecord),
    FeatureLevel(FeatureLevelRecord),
}

impl Deserialize for RecordType {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let record_type = bytes.as_ref()[1] as i8;
        match record_type {
            2 => RecordType::Topic(TopicRecord::deserialize(bytes)),
            3 => RecordType::Partition(PartitionRecord::deserialize(bytes)),
            12 => RecordType::FeatureLevel(FeatureLevelRecord::deserialize(bytes)),
            _ => panic!("Unknown record type"),
        }
    }
}

impl RecordType {
    pub fn try_get_topic(&self) -> Option<&TopicRecord> {
        if let Self::Topic(topic) = self {
            Some(topic)
        } else {
            None
        }
    }

    pub fn try_get_topic_partition(&self, topic_uuid: &i128) -> Option<&PartitionRecord> {
        if let Self::Partition(partition) = self {
            if partition.topic_uuid == *topic_uuid {
                return Some(partition);
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct TopicRecord {
    frame_version: i8,
    record_type: i8,
    version: i8,
    pub topic_name: TextData,
    pub uuid: i128,
    tagged_fields_count: u8,
}

impl Deserialize for TopicRecord {
    fn deserialize(bytes: &mut Bytes) -> Self {
        Self {
            frame_version: bytes.get_i8(),
            record_type: bytes.get_i8(),
            version: bytes.get_i8(),
            topic_name: TextData::deserialize(bytes),
            uuid: bytes.get_i128(),
            tagged_fields_count: bytes.get_u8(),
        }
    }
}

#[derive(Debug)]
pub struct PartitionRecord {
    frame_version: i8,
    record_type: i8,
    version: i8,
    partition_id: i32,
    topic_uuid: i128,
    replica_array: CompactArray<i32>,
    in_sync_replica_array: CompactArray<i32>,
    removing_replica_array: CompactArray<i32>,
    adding_replica_array: CompactArray<i32>,
    leader_id: i32,
    leader_epoch: i32,
    partition_epoch: i32,
    directories_array: CompactArray<i128>,
    tagged_fields_count: u8,
}

impl PartitionRecord {
    pub fn into_partition_response(&self, index: i32) -> Partition {
        Partition {
            error_code: crate::protocol::ErrorCode::NoError,
            partition_index: index,
            leader_id: self.leader_id,
            leader_epoch: self.leader_epoch,
            replica_nodes: self.replica_array.clone(),
            isr_nodes: self.in_sync_replica_array.clone(),
            eligible_nodes: CompactArray::<i32>::new(vec![]),
            last_known_elr: CompactArray::<i32>::new(vec![]),
            offline_replicas: CompactArray::<i32>::new(vec![]),
        }
    }
}

impl Deserialize for PartitionRecord {
    fn deserialize(bytes: &mut Bytes) -> Self {
        Self {
            frame_version: bytes.get_i8(),
            record_type: bytes.get_i8(),
            version: bytes.get_i8(),
            partition_id: bytes.get_i32(),
            topic_uuid: bytes.get_i128(),
            replica_array: CompactArray::<i32>::deserialize(bytes),
            in_sync_replica_array: CompactArray::<i32>::deserialize(bytes),
            removing_replica_array: CompactArray::<i32>::deserialize(bytes),
            adding_replica_array: CompactArray::<i32>::deserialize(bytes),
            leader_id: bytes.get_i32(),
            leader_epoch: bytes.get_i32(),
            partition_epoch: bytes.get_i32(),
            directories_array: CompactArray::<i128>::deserialize(bytes),
            tagged_fields_count: bytes.get_u8(),
        }
    }
}

#[derive(Debug)]
pub struct FeatureLevelRecord {
    frame_version: i8,
    record_type: i8,
    version: i8,
    name_length: VarIntUnsigned,
    name: String,
    feature_level: i16,
    tagged_fields_count: u8,
}

impl Deserialize for FeatureLevelRecord {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let frame_version: i8 = bytes.get_i8();
        let record_type: i8 = bytes.get_i8();
        let version: i8 = bytes.get_i8();
        let name_length: VarIntUnsigned = VarIntUnsigned::deserialize(bytes);
        let name: String =
            String::from_utf8_lossy(&bytes.copy_to_bytes(name_length.0 as usize - 1)).to_string();
        let feature_level: i16 = bytes.get_i16();
        let tagged_fields_count: u8 = bytes.get_u8();
        Self {
            frame_version,
            record_type,
            version,
            name_length,
            name,
            feature_level,
            tagged_fields_count,
        }
    }
}

impl Deserialize for i32 {
    fn deserialize(bytes: &mut Bytes) -> Self {
        bytes.get_i32()
    }
}

impl Deserialize for i128 {
    fn deserialize(bytes: &mut Bytes) -> Self {
        bytes.get_i128()
    }
}
