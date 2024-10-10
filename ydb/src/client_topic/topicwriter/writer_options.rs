use crate::client_topic::list_types::Codec;
use crate::errors;
use derive_builder::Builder;
use prost::bytes::Bytes;
use std::collections::HashMap;
use std::time::Duration;

type EncoderFunc = fn(Bytes) -> Bytes;

#[allow(dead_code)]
#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterOptions {
    /// Path of topic to write.
    pub topic_path: String,

    /// ProducerId (aka SourceId) to use.
    #[builder(setter(strip_option), default)]
    pub(crate) producer_id: Option<String>,

    // MessageGroupId to use.
    // #[builder(setter(strip_option), default)]
    // pub(crate) message_group_id: Option<String>,
    /// Explicitly enables or disables deduplication for this write session.
    /// If ProducerId option is defined deduplication will always be enabled.
    /// If ProducerId option is empty, but deduplication is enable, a random ProducerId is generated.
    #[builder(default = "true")]
    pub(crate) deduplication_enabled: bool,

    /// Write to an exact partition. Generally server assigns partition automatically by message_group_id.
    /// Using this option is **not recommended** unless you know for sure why you need it.
    #[builder(setter(strip_option), default)]
    pub(crate) partition_id: Option<u32>,

    /// User metadata that may be attached to write session
    #[builder(setter(strip_option), default)]
    pub(crate) session_metadata: Option<HashMap<String, String>>,

    #[builder(default = "true")]
    pub(crate) auto_seq_no: bool,
    #[builder(default = "true")]
    pub(crate) auto_created_at: bool,
    #[builder(default = "10")]
    pub(crate) write_request_messages_chunk_size: usize,
    #[builder(default = "Duration::from_secs(1)")]
    pub(crate) write_request_send_messages_period: Duration,
    /// Codec to use for data compression prior to write.
    /// In case of no specified codec, codec is auto-selected
    #[builder(setter(strip_option), default)]
    pub(crate) codec: Option<Codec>,
    #[builder(setter(strip_option), default)]
    pub(crate) custom_encoders: Option<HashMap<Codec, EncoderFunc>>,

    /// level to use for data compression prior to write.
    #[builder(default = "4")]
    pub(crate) compression_level: i32,

    /// Options specific to connections
    #[builder(default = "TopicWriterConnectionOptionsBuilder::default().build()?")]
    pub(crate) connection_options: TopicWriterConnectionOptions,
}

#[allow(dead_code)]
#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterConnectionOptions {
    #[builder(setter(strip_option), default)]
    pub(crate) connection_timeout: Option<core::time::Duration>,
    #[builder(setter(strip_option), default)]
    pub(crate) max_message_size_bytes: Option<i32>,
    #[builder(setter(strip_option), default)]
    pub(crate) max_buffer_messages_count: Option<i32>,
    #[builder(setter(strip_option), default)]
    pub(crate) update_token_interval: Option<core::time::Duration>,

    /// Writer will accumulate messages until reaching up to `batch_flush_size_bytes` bytes
    /// but for no longer than `batch_flush_interval`.
    /// Upon reaching `batch_flush_interval` or `batch_flush_size_bytes` limit, all messages will be written with one batch.
    /// Greatly increases performance for small messages.
    /// Setting either value to zero means immediate write with no batching. (**Unrecommended**, especially for clients
    /// sending small messages at high rate).
    #[builder(setter(strip_option), default)]
    pub(crate) batch_flush_interval: Option<core::time::Duration>,

    /// Writer will accumulate messages until reaching up to `batch_flush_size_bytes` bytes
    /// but for no longer than `batch_flush_interval`.
    /// Upon reaching `batch_flush_interval` or `batch_flush_size_bytes` limit, all messages will be written with one batch.
    /// Greatly increases performance for small messages.
    /// Setting either value to zero means immediate write with no batching. (**Unrecommended**, especially for clients
    /// sending small messages at high rate).
    #[builder(setter(strip_option), default)]
    pub(crate) batch_flush_size_bytes: Option<i32>,

    #[builder(default = "TopicWriterRetrySettingsBuilder::default().build()?")]
    retry_settings: TopicWriterRetrySettings,
}

#[allow(dead_code)]
#[derive(Builder, Clone)]
#[builder(build_fn(error = "errors::YdbError"))]
pub struct TopicWriterRetrySettings {
    /// Delay for statuses that require waiting before retry (such as OVERLOADED).
    #[builder(setter(strip_option), default)]
    delay: Option<core::time::Duration>,

    /// Maximum number of retries.
    #[builder(setter(strip_option), default)]
    max_retries: Option<usize>,

    /// Maximum time to wait for overall retries.
    #[builder(setter(strip_option), default)]
    max_time: Option<core::time::Duration>,
}
