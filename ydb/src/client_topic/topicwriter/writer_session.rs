use std::{mem::swap, sync::{atomic::AtomicI64, Arc}, time::{Duration, Instant, UNIX_EPOCH}};

use tokio::{sync::{mpsc, Mutex}, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::{trace, warn};
use ydb_grpc::ydb_proto::topic::stream_write_message::{self, from_client::ClientMessage, init_request::Partitioning, write_request::{message_data, MessageData}, InitRequest, WriteRequest};

use crate::{grpc_connection_manager::GrpcConnectionManager, grpc_wrapper::raw_topic_service::{client::RawTopicClient, stream_write::{init::RawInitResponse, RawServerMessage}}, TopicWriterMessage, TopicWriterOptions, YdbError, YdbResult};

use super::{message_write_status::{MessageWriteStatus, WriteAck}, writer::{TopicWriterState, WriterStream}, writer_reception_queue::TopicWriterReceptionQueue};


struct WriterPeriodicTaskParams {
    write_request_messages_chunk_size: usize,
    write_request_send_messages_period: Duration,
    producer_id: String,
    request_stream: mpsc::UnboundedSender<stream_write_message::FromClient>,
}

pub(crate) struct WriterSession {
    message_sender: mpsc::Sender<TopicWriterMessage>,
    last_seq_no: AtomicI64,
    cancellation_token: CancellationToken,
    state: Arc<Mutex<TopicWriterState>>,
    write_loop: Option<JoinHandle<()>>,
    receive_loop: Option<JoinHandle<()>>,

    path: String,
    producer_id: Option<String>,
    partition_id: i64,
    session_id: String,

    write_request_messages_chunk_size: usize,
    write_request_send_messages_period: Duration,

    reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,

    auto_set_seq_no: bool,
}

impl WriterSession{
    

    pub(super) async fn run_session(
        producer_id: String,
        opts: &TopicWriterOptions,
        connection_manager: GrpcConnectionManager,
    )->YdbResult<WriterSession>{
        let mut raw_client = connection_manager
            .get_auth_service(RawTopicClient::new)
            .await?;

        let init_request_body = InitRequest {
            path: opts.topic_path.clone(),
            producer_id: producer_id.clone(),
            write_session_meta: opts.session_metadata.clone().unwrap_or_default(),
            get_last_seq_no: opts.auto_seq_no,
            partitioning: Some(Partitioning::MessageGroupId(producer_id.clone())),
        };
        let ct = CancellationToken::new();
        let (message_sender, message_receiver) = mpsc::channel(32_usize);
        let mut stream= raw_client.stream_write(init_request_body).await?;
        let res = RawInitResponse::try_from(stream.receive::<RawServerMessage>().await?)?;
        
        let confirmation_reception_queue = Arc::new(Mutex::new(TopicWriterReceptionQueue::new()));

        let task_params = WriterPeriodicTaskParams {
            write_request_messages_chunk_size: opts.write_request_messages_chunk_size,
            write_request_send_messages_period: opts.write_request_send_messages_period,
            producer_id: producer_id.clone(),
            request_stream: stream.clone_sender(),
        };

        let write_loop = Self::run_write_loop(message_receiver, ct.clone(), task_params, state.clone());
        let receive_loop = Self::run_receive_loop(stream, ct.clone(), confirmation_reception_queue, state.clone());
        
        Ok(WriterSession{
            message_sender,
            partition_id: res.partition_id,
            session_id: res.session_id,
            last_seq_no: res.last_seq_no.into(),
            cancellation_token: ct,
            write_loop: Some(write_loop),
            receive_loop: Some(receive_loop),
            path: opts.topic_path.clone(),
            producer_id: Some(producer_id.clone()),
            write_request_messages_chunk_size: opts.write_request_messages_chunk_size,
            write_request_send_messages_period: opts.write_request_send_messages_period,
            auto_set_seq_no: opts.auto_seq_no,
        })
    }

    fn run_receive_loop(
        stream: WriterStream,
        ct: CancellationToken,
        reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,
        state: Arc<Mutex<TopicWriterState>>,
    ) -> JoinHandle<()>{
        tokio::spawn(async move {
            let mut stream = stream; // force move inside
            let mut reception_queue = reception_queue; // force move inside

            loop {
                tokio::select! {
                    _ = ct.cancelled() => { return ; }
                    message_receive_it_res = Self::receive_messages_loop_iteration(
                                                          &mut stream,
                                                          &mut reception_queue) => {
                        match message_receive_it_res {
                            Ok(_) => {}
                            Err(receive_message_iteration_error) => {
                                ct.cancel();
                                warn!("error receive message for topic writer receiver stream loop: {}", &receive_message_iteration_error);
                                let mut writer_state =
                                    state.lock().await; 
                                *writer_state =
                                    TopicWriterState::FinishedWithError(receive_message_iteration_error);
                                return ;
                            }
                        }
                    }
                }
            }
        })
    }

    fn run_write_loop(
        receiver: mpsc::Receiver<TopicWriterMessage>,
        ct: CancellationToken, 
        task_params: WriterPeriodicTaskParams,
        state: Arc<Mutex<TopicWriterState>>
    )->JoinHandle<()>{
        tokio::spawn(async move {
            let mut receiver = receiver; // force move inside
            let task_params = task_params; // force move inside

            loop {
                match Self::write_loop_iteration(
                    &mut receiver,
                    &task_params,
                )
                .await
                {
                    Ok(()) => {}
                    Err(writer_iteration_error) => {
                        ct.cancel();
                        let mut writer_state = state.lock().await; 
                        *writer_state = TopicWriterState::FinishedWithError(writer_iteration_error);
                        return;
                    }
                }
                if ct.is_cancelled() {
                    break;
                }
            }
        })
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
                    let created = message.created_at.duration_since(UNIX_EPOCH)?;
                    messages.push(MessageData {
                        seq_no: message
                            .seq_no
                            .ok_or_else(|| YdbError::custom("empty message seq_no"))?,
                        created_at: Some(ydb_grpc::google_proto_workaround::protobuf::Timestamp {
                            seconds: created.as_secs() as i64,
                            nanos: created.subsec_nanos() as i32,
                        }),
                        metadata_items: vec![],
                        data: message.data,
                        uncompressed_size: data_size,
                        partitioning: Some(message_data::Partitioning::MessageGroupId(
                            task_params.producer_id.clone(),
                        )),
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

    async fn wait_handle(handle: &mut Option<JoinHandle<()>>) -> YdbResult<()>{
       
        let mut t = None;
        swap(handle, &mut t);

        match t{
            Some(handle) => {
                handle.await?;
            },
            None => (),
        }
        
        Ok(())
    }

    pub(crate) async fn stop(&mut self) -> YdbResult<()> {
        self.flush().await?;
        self.cancellation_token.cancel();

        let write_loop_res = Self::wait_handle(&mut self.write_loop).await;
        let receive_loop_res = Self::wait_handle(&mut self.receive_loop).await;

        write_loop_res?;
        receive_loop_res?;
        Ok(())
    }

    pub(super) async fn write_message(
        &self,
        mut message: TopicWriterMessage,
        wait_ack: Option<tokio::sync::oneshot::Sender<MessageWriteStatus>>,
    ) -> YdbResult<()> {
        self.is_cancelled().await?;

        if self.auto_set_seq_no {
            if message.seq_no.is_some() {
                return Err(YdbError::custom(
                    "force set message seqno possible only if auto_set_seq_no disabled",
                ));
            }
            message.seq_no = Some(self.last_seq_num_handled + 1);
        };

        let message_seqno = if let Some(mess_seqno) = message.seq_no {
            self.last_seq_num_handled = mess_seqno;
            mess_seqno
        } else {
            return Err(YdbError::custom("need to set message seq_no"));
        };

        self.writer_message_sender
            .send(message)
            .await
            .map_err(|err| {
                YdbError::custom(format!("can't send the message to channel: {}", err))
            })?;

        let reception_type = wait_ack.map_or(
            TopicWriterReceptionType::NoConfirmationExpected,
            TopicWriterReceptionType::AwaitingConfirmation,
        );

        {
            // bracket needs for release mutex as soon as possible - before await
            let mut reception_queue = self.confirmation_reception_queue.lock().unwrap();
            reception_queue.add_ticket(TopicWriterReceptionTicket::new(
                message_seqno,
                reception_type,
            ));
        }

        Ok(())
    }

    pub async fn flush(&self) -> YdbResult<()> {
        self.is_cancelled().await?;

        let flush_op_completed = {
            let mut reception_queue = self.reception_queue.lock().await;
            reception_queue.init_flush_op()?
        };

        Ok(flush_op_completed.await?)
    }

    async fn is_cancelled(&self) -> YdbResult<()> {
        let state = self.state.lock().await;
        match &*state {
            TopicWriterState::Running => Ok(()),
            TopicWriterState::FinishedWithError(err) => Err(err.clone()),
        }
    }

    async fn receive_messages_loop_iteration(
        server_messages_receiver: &mut WriterStream,
        confirmation_reception_queue: &Arc<Mutex<TopicWriterReceptionQueue>>,
    ) -> YdbResult<()> {
        match server_messages_receiver.receive::<RawServerMessage>().await {
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

}