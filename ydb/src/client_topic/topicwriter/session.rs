use std::{sync::Arc, time::{Duration, Instant}};

use tokio::{sync::{mpsc, Mutex}, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::{trace, warn};
use ydb_grpc::ydb_proto::topic::stream_write_message::{self, from_client::ClientMessage, init_request, write_request::{message_data::{self, Partitioning}, MessageData}, InitRequest, WriteRequest};

use crate::{client_topic::system_time_to_timestamp, grpc_connection_manager::GrpcConnectionManager, grpc_wrapper::{grpc_stream_wrapper::AsyncGrpcStreamWrapper, raw_topic_service::{client::RawTopicClient, stream_write::{init::RawInitResponse, RawServerMessage}}}, TopicWriterMessage, TopicWriterOptions, YdbError, YdbResult};

use super::{message_write_status::WriteAck, writer::TopicWriterMode, writer_reception_queue::TopicWriterReceptionQueue};

enum SessionState {
    Closed,
    Opened(ActiveSession),
}

type TopicWriteStream =
    AsyncGrpcStreamWrapper<stream_write_message::FromClient, stream_write_message::FromServer>;

struct ActiveSession {
    session_id: String,
    stream: TopicWriteStream,
    client: RawTopicClient,

  
    cancellation_token: CancellationToken,

    receiver_loop: JoinHandle<()>,
    sender_loop: JoinHandle<()>,
}

pub(crate) struct TopicWriterSession {
    connection_manager: GrpcConnectionManager,
    opts: TopicWriterOptions,
    state: SessionState,
    producer_id: String,   
    
}

impl TopicWriterSession {
    pub(crate) fn new(connection_manager: GrpcConnectionManager, opts: TopicWriterOptions) -> Self {
        let producer_id = Self::build_producer_id(&opts);

        
        
        TopicWriterSession {
            connection_manager,
            opts,
            state: SessionState::Closed,
            producer_id,

           
        }
    }

    pub(crate) async fn connect(&mut self) -> YdbResult<()> {
        let client = self
            .connection_manager
            .get_auth_service(RawTopicClient::new)
            .await?;

        let mut res = self.init().await?;

        let (messages_sender, mut messages_receiver): (
            mpsc::Sender<TopicWriterMessage>,
            mpsc::Receiver<TopicWriterMessage>,
        ) = mpsc::channel(32_usize);
        let cancellation_token = CancellationToken::new();

        let receiver_loop = self.spawn_receiver(
            messages_receiver,
            &mut res.stream,
            cancellation_token.clone(),
        );

        self.state = SessionState::Opened(ActiveSession {
            session_id: res.inner.session_id.clone(),
            stream: res.stream,
            client,
            cancellation_token,
            receiver_loop,
            sender_loop,
        });

        Ok(())
    }
    
    fn build_producer_id(opts: &TopicWriterOptions) -> String {
        if let Some(id) = &opts.producer_id {
            id.clone()
        } else {
            uuid::Uuid::new_v4().to_string()
        }
    }

    fn spawn_receiver(
        &self, 
        messages_receiver: mpsc::Receiver<TopicWriterMessage>, 
        mut stream: &mut TopicWriteStream,       
        ct: CancellationToken
    )->JoinHandle<()>{

        let params = self.receiver_task_params(&mut stream);
        
        let messages_receiver = messages_receiver;

        tokio::spawn(async move {
            let mut message_receiver = messages_receiver; // force move inside
            let params = params; // force move inside

            loop {
                match Self::write_loop_iteration(
                    &mut message_receiver,
                    &params,
                )
                .await
                {
                    Ok(()) => {}
                    Err(writer_iteration_error) => {
                        ct.cancel();
                        let mut writer_state = writer_state_ref_writer_loop.lock().unwrap(); // TODO handle error
                        *writer_state = TopicWriterMode::FinishedWithError(writer_iteration_error);
                        return;
                    }
                }
                if ct.is_cancelled() {
                    break;
                }
            }

        })
    }

    fn spawn_sender(
        stream: TopicWriteStream,
        ct: CancellationToken
    )->JoinHandle<()>{
        tokio::spawn(async move {
            let mut stream = stream; // force move inside
            let mut reception_queue = message_loop_reception_queue; // force move inside
            
            let writer_state_ref_message_receive_loop = topic_writer_state.clone();
            let message_loop_reception_queue = confirmation_reception_queue.clone();


            loop {
                tokio::select! {
                    _ = ct.cancelled() => { return ; }
                    message_receive_it_res = Self::receive_messages_loop_iteration(
                                                          &mut stream,
                                                          &reception_queue) => {
                        match message_receive_it_res {
                            Ok(_) => {}
                            Err(receive_message_iteration_error) => {
                                ct.cancel();
                                warn!("error receive message for topic writer receiver stream loop: {}", &receive_message_iteration_error);
                                let mut writer_state =
                                    writer_state_ref_message_receive_loop.lock().unwrap(); // TODO handle error
                                *writer_state =
                                    TopicWriterMode::FinishedWithError(receive_message_iteration_error);
                                return ;
                            }
                        }
                    }
                }
            }
        })
    }

    fn receiver_task_params(&self, stream: &mut TopicWriteStream)->WriterPeriodicTaskParams{
        WriterPeriodicTaskParams {
            write_request_messages_chunk_size: self.opts.write_request_messages_chunk_size,
            write_request_send_messages_period: self.opts.write_request_send_messages_period,
            producer_id: self.producer_id.clone(),
            request_stream: stream.clone_sender(),
        }
    }

    /// Process Init request
    async fn init(&mut self)->YdbResult<InitResponse>{

        match &mut self.state{
            SessionState::Closed =>{},
            SessionState::Opened(_)=> return Err(YdbError::custom("init: topic writer session is already opened")),
        };        
        let mut client = self.connection_manager
            .get_auth_service(RawTopicClient::new)
            .await?;

        let init_request_body = InitRequest {
            path: self.opts.topic_path.clone(),
            producer_id: self.producer_id.clone(),
            write_session_meta: self.opts.session_metadata.clone().unwrap_or_default(),
            get_last_seq_no: self.opts.auto_seq_no,
            partitioning: Some(init_request::Partitioning::MessageGroupId(self.producer_id.clone())),
        };


        let mut stream = client.stream_write(init_request_body).await?;
        let inner = RawInitResponse::try_from(stream.receive::<RawServerMessage>().await?)?;

        Ok(InitResponse{
            inner,
            client,
            stream
        })
    }

    async fn receive_messages_loop_iteration(
        receiver: &mut TopicWriteStream,
        confirmation_reception_queue: &Arc<Mutex<TopicWriterReceptionQueue>>,
    ) -> YdbResult<()> {
        match receiver.receive::<RawServerMessage>().await {
            Ok(message) => match message {
                RawServerMessage::Init(_init_response_body) => {
                    return Err(YdbError::Custom(
                        "Unexpected message type in stream reader: init_response".to_string(),
                    ));
                }
                RawServerMessage::Write(write_response_body) => {
                    for raw_ack in write_response_body.acks {
                        let write_ack = WriteAck::from(raw_ack);
                        let mut reception_queue = confirmation_reception_queue.lock().await;
                        let reception_ticket = reception_queue.try_get_ticket();
                        match reception_ticket {
                            None => {
                                return Err(YdbError::Custom(
                                    "Expected reception ticket to be actually present".to_string(),
                                ));
                            }
                            Some(ticket) => {
                                if write_ack.seq_no != ticket.get_seq_no() {
                                    return Err(YdbError::custom(format!(
                                        "Reception ticket and write ack seq_no mismatch. Seqno from ack: {}, expected: {}",
                                        write_ack.seq_no, ticket.get_seq_no()
                                    )));
                                }
                                ticket.send_confirmation_if_needed(write_ack.status);
                            }
                        }
                    }
                }
                RawServerMessage::UpdateToken(_update_token_response_body) => {}
            },
            Err(some_err) => {
                return Err(YdbError::from(some_err));
            }
        }
        Ok(())
    }

    async fn write_loop_iteration(
        messages_receiver: &mut mpsc::Receiver<TopicWriterMessage>,
        task_params: &WriterPeriodicTaskParams,
    ) -> YdbResult<()> {
        let start = Instant::now();
        let mut messages = vec![];

        // wait messages loop
        'messages_loop: loop {
            let elapsed = start.elapsed();
            if messages.len() >= task_params.write_request_messages_chunk_size
                || !messages.is_empty() && elapsed >= task_params.write_request_send_messages_period
            {
                break;
            }

            match timeout(
                task_params.write_request_send_messages_period - elapsed,
                messages_receiver.recv(),
            )
            .await
            {
                Ok(Some(message)) => {
                    let data_size = message.data.len() as i64;
                    messages.push(MessageData {
                        seq_no: message
                            .seq_no
                            .ok_or_else(|| YdbError::custom("empty message seq_no"))?,
                        created_at: Some(system_time_to_timestamp(message.created_at)?),
                        data: message.data,
                        uncompressed_size: data_size,
                        partitioning: Some(message_data::Partitioning::MessageGroupId(
                            task_params.producer_id.clone(),
                        )),
                        metadata_items: message.metadata.into_iter().map(|m|MetadataItem{
                            key: m.key,
                            value: m.value,
                        }).collect(),
                    });
                }
                Ok(None) => {
                    trace!("Channel has been closed. Stop topic send messages loop.");
                    return Ok(());
                }
                Err(_elapsed) => {
                    break 'messages_loop;
                }
            }
        }

        if !messages.is_empty() {
            trace!("Sending topic message to grpc stream...");
            task_params
                .request_stream
                .send(stream_write_message::FromClient {
                    client_message: Some(ClientMessage::WriteRequest(WriteRequest {
                        messages,
                        codec: 1,
                        tx: None,
                    })),
                })
                .unwrap(); // TODO: HANDLE ERROR
        }
        Ok(())
    }

}

struct InitResponse{
    inner: RawInitResponse,
    client: RawTopicClient,
    stream: TopicWriteStream,
}

struct WriterPeriodicTaskParams {
    write_request_messages_chunk_size: usize,
    write_request_send_messages_period: Duration,
    producer_id: String,
    request_stream: mpsc::UnboundedSender<stream_write_message::FromClient>,
}