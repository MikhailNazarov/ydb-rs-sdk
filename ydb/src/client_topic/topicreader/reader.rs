use ydb_grpc::{google_proto_workaround::{self, protobuf::Timestamp}, ydb_proto::topic::stream_read_message::{init_request::TopicReadSettings, InitRequest}};

use crate::{client_topic::system_time_to_timestamp, grpc_connection_manager::GrpcConnectionManager, grpc_wrapper::{self, raw_topic_service::{client::RawTopicClient, stream_read::{init::RawInitResponse, RawServerMessage}}}, YdbError, YdbResult};

use super::reader_options::TopicReaderOptions;

pub struct TopicWriter {
    pub(crate) consumer: String,
    pub(crate) session_id: String,
}


impl TopicWriter{
    pub(crate) async fn new(
        reader_options: TopicReaderOptions,
        connection_manager: GrpcConnectionManager,
    )-> YdbResult<Self>{

        let mut topic_service = connection_manager
            .get_auth_service(grpc_wrapper::raw_topic_service::client::RawTopicClient::new)
            .await?;

        let session_id = Self::init(&mut topic_service, &reader_options).await?;        

        let reader = Self {            
            consumer: reader_options.consumer,
            session_id,
        };

        Ok(reader)
    }


    async fn init(topic_service: &mut RawTopicClient, reader_options: &TopicReaderOptions)->YdbResult<String>{

        let settings: YdbResult<Vec<_>> = reader_options.topics.iter().cloned().map(|t|
            Ok::<_, YdbError>(TopicReadSettings{
                path: t.topic_path,
                partition_ids: t.partition_ids,
                max_lag:  t.max_lag.map(|x|x.into()),
                read_from: t.read_from.map(|x|system_time_to_timestamp(x)).transpose()?,
            })).collect();

        let init_req_body = InitRequest{
            topics_read_settings:  settings?,
                
            consumer: reader_options.consumer.clone(),
        };

        let mut stream = topic_service.stream_read(init_req_body).await?;
        let init_response = RawInitResponse::try_from(stream.receive::<RawServerMessage>().await?)?;
        
        let (messages_sender, messages_receiver): (
            mpsc::Sender<TopicReaderMessage>,
            mpsc::Receiver<TopicReaderMessage>,
        ) = mpsc::channel(32_usize);

        Ok(init_response.session_id)
    }
}