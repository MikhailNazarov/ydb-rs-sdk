use crate::grpc_wrapper::{raw_errors::RawError, raw_ydb_operation::RawOperationParams};

pub(crate) struct RawExplainDataQueryRequest {
    pub session_id: String,
    pub yql_text: String,
    pub operation_params: RawOperationParams,
}

impl From<RawExplainDataQueryRequest> for ydb_grpc::ydb_proto::table::ExplainDataQueryRequest {
    fn from(v: RawExplainDataQueryRequest) -> Self {
        Self {
            session_id: v.session_id,
            yql_text: v.yql_text,
            operation_params: Some(v.operation_params.into()),
        }
    }
}

pub(crate) struct RawExplainDataQueryResult {
    query_ast: String,
    query_plan: String,
}

impl TryFrom<ydb_grpc::ydb_proto::table::ExplainQueryResult> for RawExplainDataQueryResult {
    type Error = RawError;

    fn try_from(
        value: ydb_grpc::ydb_proto::table::ExplainQueryResult,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            query_ast: value.query_ast,
            query_plan: value.query_plan,
        })
    }
}
