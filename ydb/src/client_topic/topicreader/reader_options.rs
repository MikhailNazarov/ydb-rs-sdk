use std::time::{Duration, SystemTime};

// Message that describes topic to read.
pub struct TopicReadOptions {
    // Topics that will be read by this session.
    pub topic_path: String,

    // Partitions that will be read by this session.
    // If list is empty - then session will read all partitions.
    pub(crate) partition_ids: Vec<i64>,

    // Skip all messages that has write timestamp smaller than now - max_lag.
    // Zero means infinite lag.
    pub(crate) max_lag: Option<Duration>,

    // Read data only after this timestamp from this topic.
    // Read only messages with 'written_at' value greater or equal than this timestamp.
    pub(crate) read_from: Option<SystemTime>,
}

pub struct TopicReaderOptions {
    pub(crate) topics: Vec<TopicReadOptions>,

    // Path of consumer that is used for reading by this session.
    pub(crate) consumer: String,

    // Optional name. Will be shown in debug stat.
    pub(crate) reader_name: Option<String>,

    // Direct reading from a partition node.
    pub(crate) direct_read: bool,
}
