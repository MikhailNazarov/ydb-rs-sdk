use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::grpc_connection_manager::GrpcConnectionManager;

use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::{YdbError, YdbResult};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex};

use tracing::log::trace;
use tracing::warn;
use ydb_grpc::ydb_proto::topic::stream_write_message;

use super::writer_session::WriterSession;

pub(crate) enum TopicWriterState {
    Idle,
    Working(WriterSession),
    FinishedWithError(YdbError),
}

/// TopicWriter at initial state of implementation
/// it really doesn't ready for use. For example
/// It isn't handle lost connection to the server and have some unimplemented method.
#[allow(dead_code)]
pub struct TopicWriter {
    // pub(crate) path: String,
    // pub(crate) producer_id: Option<String>,
    // pub(crate) partition_id: i64,
    // pub(crate) session_id: String,
    // pub(crate) last_seq_num_handled: i64,
    // pub(crate) write_request_messages_chunk_size: usize,
    // pub(crate) write_request_send_messages_period: Duration,

    // pub(crate) auto_set_seq_no: bool,
    // pub(crate) codecs_from_server: RawSupportedCodecs,

    // writer_message_sender: mpsc::Sender<TopicWriterMessage>,
    // writer_loop: JoinHandle<()>,
    // receive_messages_loop: JoinHandle<()>,

    // cancellation_token: CancellationToken,
    // writer_state: Arc<Mutex<TopicWriterState>>,

    // confirmation_reception_queue: Arc<Mutex<TopicWriterReceptionQueue>>,

    // pub(crate) connection_manager: GrpcConnectionManager,

    producer_id: String,
    opts: TopicWriterOptions,
    connection_manager: GrpcConnectionManager,
    state: Arc<Mutex<TopicWriterState>>,
    
}

#[allow(dead_code)]
pub struct AckFuture {
    receiver: tokio::sync::oneshot::Receiver<MessageWriteStatus>,
}

impl Future for AckFuture {
    type Output = YdbResult<MessageWriteStatus>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(_cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(Ok(result)),
            Poll::Ready(Err(_)) => Poll::Ready(Err(YdbError::custom("message writer was closed"))),
            Poll::Pending => Poll::Pending,
        }
    }
}

struct WriterPeriodicTaskParams {
    write_request_messages_chunk_size: usize,
    write_request_send_messages_period: Duration,
    producer_id: Option<String>,
    request_stream: mpsc::UnboundedSender<stream_write_message::FromClient>,
}

pub(crate) type WriterStream = AsyncGrpcStreamWrapper<stream_write_message::FromClient, stream_write_message::FromServer>;

impl TopicWriter {

    

    pub(crate) fn new(
        opts: TopicWriterOptions,
        connection_manager: GrpcConnectionManager,
    ) -> Self {

        let producer_id = opts.producer_id.clone().unwrap_or_else(||{
            uuid::Uuid::new_v4().to_string()
        });

        // let session = WriterSession::run_session(
        //     producer_id,
        //     &opts,
        //     connection_manager.clone()
        // ).await?;
        Self { 
            producer_id,
            opts,
            connection_manager,
            state: Arc::new(Mutex::new(TopicWriterState::Idle)), 
            //Arc::new(Mutex::new(TopicWriterState::Working(session))) 
        }
    }

    pub(crate) async fn run(&self)->YdbResult<()>{

        trace!("Starting...");
        let mut state = self.state.lock().await;

        if let TopicWriterState::Working(_) = &*state{
            warn!("writer is already working");
            return Ok(());
        }

        let session = WriterSession::run_session(
            self.producer_id.clone(),
            &self.opts,
            self.connection_manager.clone()
        ).await?;

        *state = TopicWriterState::Working(session);
        Ok(())
    }

    pub async fn stop(self) -> YdbResult<()> {
        trace!("Stopping...");
        let mut state = self.state.lock().await;

        if let TopicWriterState::Working(session) = &mut *state{
            *state = match session.stop().await{
                Ok(_) => TopicWriterState::Idle,
                Err(e) => TopicWriterState::FinishedWithError(e),
            };
            return Ok(());
        }

        warn!("writer is not working");
        Ok(())
    }

    pub async fn write(&self, message: TopicWriterMessage) -> YdbResult<()> {
        if let TopicWriterState::Working(session) = &*self.state.lock().await {
            session.write_message(message, None).await?;
            return Ok(());
        }
        Err(YdbError::custom("writer is not working"))
    }

    pub async fn write_with_ack(
        &self,
        message: TopicWriterMessage,
    ) -> YdbResult<MessageWriteStatus> {

        if let TopicWriterState::Working(session) = &*self.state.lock().await {
            let (tx, rx)= tokio::sync::oneshot::channel();
            session.write_message(message, Some(tx)).await?;
            return Ok(rx.await?);
        }
        Err(YdbError::custom("writer is not working"))
    }

    pub async fn write_with_ack_future(
        &self,
        message: TopicWriterMessage,
    ) -> YdbResult<AckFuture> {

        if let TopicWriterState::Working(session) = &*self.state.lock().await {
            let (tx, rx)= tokio::sync::oneshot::channel();
            session.write_message(message, Some(tx)).await?;
            return Ok(AckFuture { receiver: rx });
        }
        Err(YdbError::custom("writer is not working"))
    }
    
}
