use crate::{
    metadata::{PartitionRecord, RecordBatch, TopicRecord},
    protocol::{Deserialize, Serialize},
};
use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use std::fs::File;
use std::io::Read;

#[derive(Debug)]
pub struct MetadataFile {
    pub record_batches: Vec<RecordBatch>,
}

impl Deserialize for MetadataFile {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let mut record_batches = Vec::new();
        while bytes.has_remaining() {
            let record_batch = RecordBatch::deserialize(bytes);
            record_batches.push(record_batch);
        }
        Self { record_batches }
    }
}

impl Serialize for MetadataFile {
    fn serialize(&self, bytes: &mut BytesMut) {
        for record_batch in &self.record_batches {
            record_batch.serialize(bytes);
        }
    }
}

fn read_metadata_bytes(path: String) -> Result<Bytes> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    Ok(Bytes::from(buffer))
}

pub fn read_partition_metadata(topic_name: String, partition_id: i32) -> Result<MetadataFile> {
    let path = format!(
        "/tmp/kraft-combined-logs/{}-{}/00000000000000000000.log",
        topic_name, partition_id
    );
    let mut bytes = read_metadata_bytes(path)?;
    let metadata = MetadataFile::deserialize(&mut bytes);
    Ok(metadata)
}

pub fn read_cluster_metadata() -> Result<MetadataFile> {
    let path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log".to_string();
    let mut bytes = read_metadata_bytes(path)?;
    Ok(MetadataFile::deserialize(&mut bytes))
}

impl MetadataFile {
    pub fn get_topics(&self) -> impl Iterator<Item = &TopicRecord> {
        self.record_batches
            .iter()
            .flat_map(|x| &x.records)
            .filter_map(|x| x.value.try_get_topic())
    }

    pub fn get_topic_partitions<'a>(
        &'a self,
        topic_uuid: &'a i128,
    ) -> impl Iterator<Item = &'a PartitionRecord> + 'a {
        self.record_batches
            .iter()
            .flat_map(|x| &x.records)
            .filter_map(|x| x.value.try_get_topic_partition(topic_uuid))
    }
}
