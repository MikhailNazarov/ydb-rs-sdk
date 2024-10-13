use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::client_topic::topicwriter::writer_reception_queue::{
    TopicWriterReceptionTicket, TopicWriterReceptionType,
};
use crate::grpc_connection_manager::GrpcConnectionManager;

use crate::grpc_wrapper::grpc_stream_wrapper::AsyncGrpcStreamWrapper;
use crate::{YdbError, YdbResult};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};

use tracing::log::trace;
use ydb_grpc::ydb_proto::topic::stream_write_message;

use super::writer_session::WriterSession;

pub(crate) enum TopicWriterState {
    Created,
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
            state: Arc::new(Mutex::new(TopicWriterState::Created)), 
            //Arc::new(Mutex::new(TopicWriterState::Working(session))) 
        }
    }

    pub(crate) async fn run(
        &self
    )->YdbResult<()>{
        let session = WriterSession::run_session(
            self.producer_id.clone(),
            &self.opts,
            self.connection_manager.clone()
        ).await?;
        *self.state.lock().await = TopicWriterState::Working(session);
        Ok(())
    }

    pub async fn stop(self) -> YdbResult<()> {
        trace!("Stopping...");

        self.flush().await?;
        self.cancellation_token.cancel();

        self.writer_loop.await.map_err(|err| {
            YdbError::custom(format!(
                "error while wait finish writer_loop on stop: {}",
                err
            ))
        })?; // TODO: handle ERROR
        trace!("Writer loop stopped");

        self.receive_messages_loop.await.map_err(|err| {
            YdbError::custom(format!(
                "error while wait finish receive_messages_loop on stop: {}",
                err
            ))
        })?; // TODO: handle ERROR
        trace!("Message receive stopped");
        Ok(())
    }

    pub async fn write(&self, message: TopicWriterMessage) -> YdbResult<()> {
        self.write_message(message, None).await?;
        Ok(())
    }

    pub async fn write_with_ack(
        &self,
        message: TopicWriterMessage,
    ) -> YdbResult<MessageWriteStatus> {
        let (tx, rx): (
            tokio::sync::oneshot::Sender<MessageWriteStatus>,
            tokio::sync::oneshot::Receiver<MessageWriteStatus>,
        ) = tokio::sync::oneshot::channel();

        self.write_message(message, Some(tx)).await?;
        Ok(rx.await?)
    }

    pub async fn write_with_ack_future(
        &self,
        _message: TopicWriterMessage,
    ) -> YdbResult<AckFuture> {
        let (tx, rx): (
            tokio::sync::oneshot::Sender<MessageWriteStatus>,
            tokio::sync::oneshot::Receiver<MessageWriteStatus>,
        ) = tokio::sync::oneshot::channel();

        self.write_message(_message, Some(tx)).await?;
        Ok(AckFuture { receiver: rx })
    }

    async fn write_message(
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
            let mut reception_queue = self.confirmation_reception_queue.lock().unwrap();
            reception_queue.init_flush_op()?
        };

        Ok(flush_op_completed.await?)
    }

    async fn is_cancelled(&self) -> YdbResult<()> {
        let state = self.writer_state.lock().unwrap();
        match state.deref() {
            TopicWriterState::Working => Ok(()),
            TopicWriterState::FinishedWithError(err) => Err(err.clone()),
        }
    }
}
