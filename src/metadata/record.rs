use crate::protocol::{CompactArray, Deserialize, Partition, Serialize, TextData, VarIntUnsigned};
use bytes::{Buf, BufMut, Bytes};

#[derive(Debug)]
pub enum RecordType {
    Topic(TopicRecord),
    Partition(PartitionRecord),
    FeatureLevel(FeatureLevelRecord),
    RawBytes(RawBytesRecord),
}

impl RecordType {
    pub fn new(bytes: &mut Bytes, length: &i64) -> Self {
        let record_type = bytes.as_ref()[1] as i8;
        match record_type {
            2 => RecordType::Topic(TopicRecord::deserialize(bytes)),
            3 => RecordType::Partition(PartitionRecord::deserialize(bytes)),
            12 => RecordType::FeatureLevel(FeatureLevelRecord::deserialize(bytes)),
            _ => RecordType::RawBytes(RawBytesRecord::new(bytes, length)),
        }
    }
}

impl Serialize for RecordType {
    fn serialize(&self, bytes: &mut bytes::BytesMut) {
        match self {
            // RecordType::Topic(topic) => topic.serialize(bytes),
            // RecordType::Partition(partition) => partition.serialize(bytes),
            RecordType::FeatureLevel(feature_level) => feature_level.serialize(bytes),
            RecordType::RawBytes(raw_bytes) => raw_bytes.serialize(bytes),
            _ => (),
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
pub struct RawBytesRecord {
    pub data: Bytes,
}
impl RawBytesRecord {
    fn new(bytes: &mut Bytes, length: &i64) -> Self {
        let data = bytes.copy_to_bytes(*length as usize);
        Self { data }
    }
}
impl Serialize for RawBytesRecord {
    fn serialize(&self, bytes: &mut bytes::BytesMut) {
        bytes.put_slice(&self.data);
    }
}

#[derive(Debug)]
pub struct TopicRecord {
    _frame_version: i8,
    _record_type: i8,
    _version: i8,
    pub topic_name: TextData,
    pub uuid: i128,
    _tagged_fields_count: u8,
}

impl Deserialize for TopicRecord {
    fn deserialize(bytes: &mut Bytes) -> Self {
        Self {
            _frame_version: bytes.get_i8(),
            _record_type: bytes.get_i8(),
            _version: bytes.get_i8(),
            topic_name: TextData::deserialize(bytes),
            uuid: bytes.get_i128(),
            _tagged_fields_count: bytes.get_u8(),
        }
    }
}

#[derive(Debug)]
pub struct PartitionRecord {
    _frame_version: i8,
    _record_type: i8,
    _version: i8,
    pub partition_id: i32,
    topic_uuid: i128,
    replica_array: CompactArray<i32>,
    in_sync_replica_array: CompactArray<i32>,
    _removing_replica_array: CompactArray<i32>,
    _adding_replica_array: CompactArray<i32>,
    leader_id: i32,
    leader_epoch: i32,
    _partition_epoch: i32,
    _directories_array: CompactArray<i128>,
    _tagged_fields_count: u8,
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
            _frame_version: bytes.get_i8(),
            _record_type: bytes.get_i8(),
            _version: bytes.get_i8(),
            partition_id: bytes.get_i32(),
            topic_uuid: bytes.get_i128(),
            replica_array: CompactArray::<i32>::deserialize(bytes),
            in_sync_replica_array: CompactArray::<i32>::deserialize(bytes),
            _removing_replica_array: CompactArray::<i32>::deserialize(bytes),
            _adding_replica_array: CompactArray::<i32>::deserialize(bytes),
            leader_id: bytes.get_i32(),
            leader_epoch: bytes.get_i32(),
            _partition_epoch: bytes.get_i32(),
            _directories_array: CompactArray::<i128>::deserialize(bytes),
            _tagged_fields_count: bytes.get_u8(),
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
        let name_length = VarIntUnsigned::deserialize(bytes);
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

impl Serialize for FeatureLevelRecord {
    fn serialize(&self, bytes: &mut bytes::BytesMut) {
        bytes.put_i8(self.frame_version);
        bytes.put_i8(self.record_type);
        bytes.put_i8(self.version);
        self.name_length.serialize(bytes);
        bytes.put_slice(self.name.as_bytes());
        bytes.put_i16(self.feature_level);
        bytes.put_u8(self.tagged_fields_count);
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
