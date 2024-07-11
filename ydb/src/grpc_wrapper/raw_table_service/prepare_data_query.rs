use crate::grpc_wrapper::{raw_errors::RawError, raw_ydb_operation::RawOperationParams};

use super::value::{r#type::RawType, RawParameter};

pub(crate) struct RawPrepareDataQueryRequest {
    pub session_id: String,
    pub yql_text: String,
    pub operation_params: RawOperationParams,
}

impl From<RawPrepareDataQueryRequest> for ydb_grpc::ydb_proto::table::PrepareDataQueryRequest {
    fn from(v: RawPrepareDataQueryRequest) -> Self {
        Self {
            session_id: v.session_id,
            yql_text: v.yql_text,
            operation_params: Some(v.operation_params.into()),
        }
    }
}

pub(crate) struct RawPrepareDataQueryResult {
    pub query_id: String,
    pub parameters_types: Vec<RawParameter>,
}

impl TryFrom<ydb_grpc::ydb_proto::table::PrepareQueryResult> for RawPrepareDataQueryResult {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::table::PrepareQueryResult,
    ) -> Result<Self, Self::Error> {
        let params: Result<Vec<RawParameter>, RawError> = value
            .parameters_types
            .into_iter()
            .map(|(name, r#type)| {
                let raw_type = RawType::try_from(r#type);
                match raw_type {
                    Ok(r) => Ok(RawParameter { name, r#type: r }),
                    Err(e) => Err(e),
                }
            })
            .collect();
        Ok(Self {
            query_id: value.query_id,
            parameters_types: params?,
        })
    }
}
