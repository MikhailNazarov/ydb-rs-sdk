use ydb_grpc::ydb_proto::topic::stream_read_message::InitResponse;

use crate::grpc_wrapper::raw_errors::{RawError, RawResult};

use super::RawServerMessage;

#[derive(serde::Serialize)]
pub(crate) struct RawInitResponse {
    pub session_id: String,
}

impl TryFrom<InitResponse> for RawInitResponse {
    type Error = RawError;

    fn try_from(value: InitResponse) -> RawResult<Self> {
        Ok(Self {
            session_id: value.session_id,
        })
    }
}

impl TryFrom<RawServerMessage> for RawInitResponse {
    type Error = RawError;

    fn try_from(value: RawServerMessage) -> RawResult<Self> {
        if let RawServerMessage::Init(response) = value {
            Ok(response)
        } else {
            let message_string = match serde_json::to_string(&value) {
                Ok(str) => str,
                Err(err) => format!("Failed to serialize message: {}", err),
            };
            Err(RawError::Custom(format!(
                "Expected to get InitResponse, got: {}",
                message_string,
            )))
        }
    }
}
