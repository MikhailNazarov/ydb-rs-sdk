use crate::{errors, utils::system_time_to_timestamp, YdbError, YdbResult};
use derive_builder::Builder;
use std::time;
use ydb_grpc::google_proto_workaround::protobuf::Timestamp;

#[derive(Builder)]
#[builder(build_fn(error = "errors::YdbError", validate = "Self::validate"))]
#[allow(dead_code)]
pub struct TopicWriterMessage {
    #[builder(default = "None")]
    pub(crate) seq_no: Option<i64>,
    #[builder(default = "time::SystemTime::now()")]
    pub(crate) created_at: time::SystemTime,

    pub(crate) data: Vec<u8>,
    pub(crate) metadata: Vec<TopicWriteMetadataItem>,
}

pub(crate) struct TopicWriterSessionMessage {
    pub(crate) seq_no: i64,
    pub(crate) created_at: Timestamp,
    pub(crate) data: Vec<u8>,
    pub(crate) metadata: Vec<TopicWriteMetadataItem>,
}

impl TryFrom<TopicWriterMessage> for TopicWriterSessionMessage {
    type Error = YdbError;

    fn try_from(value: TopicWriterMessage) -> Result<Self, Self::Error> {
        Ok(TopicWriterSessionMessage {
            seq_no: value
                .seq_no
                .ok_or_else(|| YdbError::custom("empty message seq_no"))?,
            created_at: system_time_to_timestamp(value.created_at)?,
            data: value.data,
            metadata: value.metadata,
        })
    }
}

#[derive(Clone)]
pub struct TopicWriteMetadataItem {
    pub key: String,
    pub value: Vec<u8>,
}

impl TopicWriterMessageBuilder {
    fn validate(&self) -> YdbResult<()> {
        Ok(())
    }
}
