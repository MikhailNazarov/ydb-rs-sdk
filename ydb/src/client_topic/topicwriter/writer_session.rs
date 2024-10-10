use std::{ sync::Arc, time::{Duration, Instant}};

use tokio::{select, sync::{mpsc::{self, UnboundedSender}, Mutex, RwLock}, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::{trace, warn};
use ydb_grpc::ydb_proto::topic::{stream_write_message::{self, from_client::ClientMessage, init_request, write_request::{message_data::{self, Partitioning}, MessageData}, FromClient, InitRequest, WriteRequest}, MetadataItem};

use crate::{ grpc_connection_manager::GrpcConnectionManager, grpc_wrapper::{grpc_stream_wrapper::AsyncGrpcStreamWrapper, raw_topic_service::{client::RawTopicClient, stream_write::{init::RawInitResponse, RawServerMessage}}}, utils::system_time_to_timestamp, TopicWriterMessage, TopicWriterOptions, TopicWriterOptionsBuilder, YdbError, YdbResult};

use super::{message::TopicWriterSessionMessage, message_write_status::WriteAck, writer_reception_queue::TopicWriterReceptionQueue};

/// State of topic writer session
enum SessionState {
    /// Session is closed
    /// `Error` is set if session is closed with error.
    /// `None` if session is closed without error or just created
    Closed(Option<YdbError>),
    /// Session is opened
    /// `ActiveSession` contains active session data
    Opened(ActiveSession),
}

/// Stream for topic writer
type TopicWriteStream =
    AsyncGrpcStreamWrapper<stream_write_message::FromClient, stream_write_message::FromServer>;

/// Active session data for topic writer
struct ActiveSession {
    /// Session id
    session_id: String,    

    /// Topic client
    client: RawTopicClient,  

    /// Cancellation token for session
    cancellation_token: CancellationToken,

    /// Message sender
    message_sender: mpsc::Sender<TopicWriterSessionMessage>,    
}

/// Session for topic writer
pub(crate) struct TopicWriterSession {
    /// YDB connection manager
    connection_manager: GrpcConnectionManager,
    
    /// Writer options
    opts: TopicWriterOptions,

    /// Session state
    state: Arc<RwLock<SessionState>>,

    /// Producer id
    producer_id: String,   
}

impl TopicWriterSession {

    /// Creates new topic writer session with given connection manager and default options
    pub(crate) fn new(connection_manager: GrpcConnectionManager) -> Self {
        Self::with_opts(connection_manager, TopicWriterOptionsBuilder::default().build().unwrap())
    }

    /// Creates new topic writer session with given connection manager and options
    pub(crate) fn with_opts(connection_manager: GrpcConnectionManager, opts: TopicWriterOptions) -> Self { 
        
        let producer_id = Self::get_producer_id(&opts);
        
        TopicWriterSession {
            connection_manager,
            opts,
            state: Arc::new(RwLock::new(SessionState::Closed(None))),
            producer_id,
        }
    }

    /// Runs topic writer session loop
    pub(crate) async fn run(&mut self) -> YdbResult<()> {
        { // block for shorter guard lifetime
            if let SessionState::Opened(_) = &*self.state.read().await {
                trace!("Topic writer session is already opened");
                return Ok(());
            }
        }

        trace!("Starting topic writer session");

        let client = self
            .connection_manager
            .get_auth_service(RawTopicClient::new)
            .await?;

        let mut res = self.init_request().await?;

        let (message_sender, message_receiver) = mpsc::channel(32_usize);
        let (error_sender, error_receiver) = mpsc::channel(1_usize);
        
        let cancellation_token = CancellationToken::new();

        let sender = res.stream.clone_sender();
        let receiver_loop = self.spawn_receiver(
            message_receiver,
           sender,
           error_sender.clone(),
            cancellation_token.clone(),
        );

        let sender_loop = self.spawn_sender(
            res.stream,
            error_sender.clone(),
            cancellation_token.clone()
        );

       
        self.state = Arc::new(RwLock::new( SessionState::Opened(ActiveSession {
            session_id: res.inner.session_id.clone(),            
            client,
            cancellation_token: cancellation_token.clone(),            
            message_sender,            
        })));

        Ok(select! {
            _ = cancellation_token.cancelled() => {},
            _ = receiver_loop => {},
            _ = sender_loop => {},
        })
    }
    
    /// Get producer id from options according to `deduplication_enabled` option.
    /// 
    /// If `deduplication_enabled` is false, empty string is returned
    /// otherwise `producer_id` option or generated value is returned
    fn get_producer_id(opts: &TopicWriterOptions) -> String {
        if !opts.deduplication_enabled{
            "".to_owned()
        } else if let Some(id) = &opts.producer_id {
            id.clone()
        } else{
            uuid::Uuid::new_v4().to_string()
        }
    }

    /// Spawns receiver loop
    fn spawn_receiver(
        &self, 
        messages_receiver: mpsc::Receiver<TopicWriterSessionMessage>, 
        sender: UnboundedSender<FromClient>,
        error_sender: mpsc::Sender<YdbError>,
        ct: CancellationToken
    )->JoinHandle<()>{

        let params = self.receiver_task_params(sender);
        
        let messages_receiver = messages_receiver;

        tokio::spawn(async move {
            // force move inside
            let mut message_receiver = messages_receiver;             

            loop {
                match Self::write_loop_iteration(
                    &mut message_receiver,
                    &params,
                )
                .await
                {
                    Ok(()) => {}
                    Err(error) => {
                        ct.cancel();
                        error_sender.send(error).await.unwrap();                        
                        return;
                    }
                }
                if ct.is_cancelled() {
                    break;
                }
            }

        })
    }

    async fn handle_error(state: Arc<RwLock<SessionState>>, ct: CancellationToken, error: YdbError){
        ct.cancel();
        let mut guard = state.write().await;        
        *guard = SessionState::Closed(Some(error));
    }

    /// Spawns sender loop
    fn spawn_sender(
        &self,
        stream: TopicWriteStream,
        error_sender: mpsc::Sender<YdbError>,
        ct: CancellationToken
    )->JoinHandle<()>{
        tokio::spawn(async move {
            let mut stream = stream; // force move inside
            // let mut reception_queue = message_loop_reception_queue; // force move inside
            
            // let writer_state_ref_message_receive_loop = topic_writer_state.clone();
            // let message_loop_reception_queue = confirmation_reception_queue.clone();


            loop {
                tokio::select! {
                    _ = ct.cancelled() => { return ; }
                    message_receive_it_res = Self::receive_messages_loop_iteration(
                                                          &mut stream,
                                                          &reception_queue) => {
                        match message_receive_it_res {
                            Ok(_) => {}
                            Err(e) => {
                                ct.cancel();
                                warn!("error receive message for topic writer receiver stream loop: {}", &e);
                                error_sender.send(e).await.unwrap();
                                return ;
                            }
                        }
                    }
                }
            }
        })
    }

    fn receiver_task_params(&self, sender: UnboundedSender<FromClient>)->WriterPeriodicTaskParams{
        WriterPeriodicTaskParams {
            write_request_messages_chunk_size: self.opts.write_request_messages_chunk_size,
            write_request_send_messages_period: self.opts.write_request_send_messages_period,
            producer_id: self.producer_id.clone(),
            sender,
        }
    }

    /// Process Init request
    async fn init_request(&mut self)->YdbResult<InitResponse>{

        match &*self.state.read().await{
            SessionState::Closed(_) =>{},
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
        messages_receiver: &mut mpsc::Receiver<TopicWriterSessionMessage>,
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
                        seq_no: message.seq_no,
                        created_at: Some(message.created_at),
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
            task_params.sender
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

    pub(crate) async fn write(&self, message: TopicWriterSessionMessage)-> YdbResult<()>{
        match &*self.state.read().await{
            SessionState::Closed(_) => Err(YdbError::Custom("Session is closed".to_owned())),
            SessionState::Opened(session)=> {
                session.message_sender.send(message).await
                    .map_err(|err|YdbError::custom(format!("can't send the message to channel: {}", err)))
            },
        }
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
    sender: mpsc::UnboundedSender<stream_write_message::FromClient>,
}