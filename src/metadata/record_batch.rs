use crate::protocol::Deserialize;
use crate::{metadata::record::RecordType, protocol::VarIntSigned};
use bytes::{Buf, Bytes};

#[derive(Debug)]
pub struct RecordBatch {
    base_offset: i64,
    batch_length: i32,
    partition_leader_epoch: i32,
    magic_byte: i8,
    crc: i32,
    attributes: i16,
    last_offset_delta: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    pub records: Vec<Record>,
}
impl Deserialize for RecordBatch {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let base_offset = bytes.get_i64();
        let batch_length = bytes.get_i32();
        let partition_leader_epoch = bytes.get_i32();
        let magic_byte = bytes.get_i8();
        let crc = bytes.get_i32();
        let attributes = bytes.get_i16();
        let last_offset_delta = bytes.get_i32();
        let base_timestamp = bytes.get_i64();
        let max_timestamp = bytes.get_i64();
        let producer_id = bytes.get_i64();
        let producer_epoch = bytes.get_i16();
        let base_sequence = bytes.get_i32();
        let records_length = bytes.get_i32();
        let mut records = vec![];
        for _ in 0..records_length {
            let record = Record::deserialize(bytes);
            records.push(record);
        }
        Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic_byte,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        }
    }
}

#[derive(Debug)]
pub struct Record {
    length: VarIntSigned,
    attributes: i8,
    timestamp_delta: VarIntSigned,
    offset_delta: VarIntSigned,
    key_length: VarIntSigned,
    key: Vec<u8>,
    value_length: VarIntSigned,
    pub value: RecordType,
    headers_length: i8,
}
impl Deserialize for Record {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let length = VarIntSigned::deserialize(bytes);
        let attributes = bytes.get_i8();
        let timestamp_delta = VarIntSigned::deserialize(bytes);
        let offset_delta = VarIntSigned::deserialize(bytes);
        let key_length = VarIntSigned::deserialize(bytes);
        let mut key = vec![];
        if key_length.0 > 0 {
            for _ in 0..key_length.0 {
                key.push(bytes.get_u8());
            }
        }
        let value_length = VarIntSigned::deserialize(bytes);
        let value = RecordType::deserialize(bytes);
        let headers_length = bytes.get_i8();
        Self {
            length,
            attributes,
            timestamp_delta,
            offset_delta,
            key_length,
            key,
            value_length,
            value,
            headers_length,
        }
    }
}
