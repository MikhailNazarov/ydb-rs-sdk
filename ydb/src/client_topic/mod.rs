use std::time::SystemTime;

use ydb_grpc::google_proto_workaround::protobuf::Timestamp;

use crate::YdbResult;

pub(crate) mod client;
pub(crate) mod list_types;
pub(crate) mod topicreader;
pub(crate) mod topicwriter;

//todo: move to utils
pub(crate) fn system_time_to_timestamp(system_time: SystemTime) -> YdbResult<Timestamp> {
    let duration_since_epoch = system_time.duration_since(SystemTime::UNIX_EPOCH)?;
    let seconds = duration_since_epoch.as_secs() as i64;
    let nanos = duration_since_epoch.subsec_nanos() as i32;

    Ok(Timestamp { seconds, nanos })
}
