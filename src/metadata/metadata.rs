use crate::{
    metadata::{PartitionRecord, Record, RecordBatch, RecordType, TopicRecord},
    protocol::{Deserialize, Topic},
};
use anyhow::Result;
use bytes::{Buf, Bytes};
use std::fs::File;
use std::io::Read;

#[derive(Debug)]
pub struct MetadataFile {
    record_batches: Vec<RecordBatch>,
}

impl Deserialize for MetadataFile {
    fn deserialize(bytes: &mut Bytes) -> Self {
        let mut record_batches = Vec::new();
        while bytes.has_remaining() {
            let record_batch = RecordBatch::deserialize(bytes);
            dbg!(&record_batch);
            record_batches.push(record_batch);
        }
        Self { record_batches }
    }
}

fn read_metadata_bytes() -> Result<Bytes> {
    let mut file =
        File::open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    Ok(Bytes::from(buffer))
}

pub fn read_metadata() -> Result<MetadataFile> {
    let mut bytes = read_metadata_bytes()?;
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
