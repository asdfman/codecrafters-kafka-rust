use crate::protocol::{Deserialize, Serialize};
use crate::{metadata::record::RecordType, protocol::VarIntSigned};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32c::crc32c;

#[derive(Debug)]
pub struct RecordBatch {
    base_offset: i64,
    _batch_length: i32,
    partition_leader_epoch: i32,
    magic_byte: i8,
    _crc: i32,
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
            _batch_length: batch_length,
            partition_leader_epoch,
            magic_byte,
            _crc: crc,
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

impl Serialize for RecordBatch {
    fn serialize(&self, bytes: &mut bytes::BytesMut) {
        let start_offset = bytes.len();
        bytes.put_i64(self.base_offset);
        let batch_length_start = bytes.len();
        bytes.put_i32(0); // reserve 4 bytes for batch length
        bytes.put_i32(self.partition_leader_epoch);
        bytes.put_i8(self.magic_byte);
        let crc_start = bytes.len();
        bytes.put_i32(0); // reserve 4 bytes for crc
        bytes.put_i16(self.attributes);
        bytes.put_i32(self.last_offset_delta);
        bytes.put_i64(self.base_timestamp);
        bytes.put_i64(self.max_timestamp);
        bytes.put_i64(self.producer_id);
        bytes.put_i16(self.producer_epoch);
        bytes.put_i32(self.base_sequence);
        bytes.put_i32(self.records.len() as i32);
        for record in &self.records {
            record.serialize(bytes);
        }
        let crc = crc32c(&bytes[crc_start + 4..]);
        bytes[crc_start..crc_start + 4].copy_from_slice(&crc.to_be_bytes());
        let batch_length = (bytes.len() - start_offset - 12) as i32;
        bytes[batch_length_start..batch_length_start + 4]
            .copy_from_slice(&batch_length.to_be_bytes());
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
        let value = RecordType::new(bytes, &value_length.0);
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

impl Serialize for Record {
    fn serialize(&self, bytes: &mut BytesMut) {
        self.length.serialize(bytes);
        bytes.put_i8(self.attributes);
        self.timestamp_delta.serialize(bytes);
        self.offset_delta.serialize(bytes);
        self.key_length.serialize(bytes);
        bytes.put_slice(&self.key);
        self.value_length.serialize(bytes);
        self.value.serialize(bytes);
        bytes.put_i8(self.headers_length);
    }
}
