use crate::client_topic::topicwriter::message::TopicWriterMessage;
use crate::client_topic::topicwriter::message_write_status::MessageWriteStatus;
use crate::client_topic::topicwriter::writer_options::TopicWriterOptions;
use crate::grpc_connection_manager::GrpcConnectionManager;

use crate::{YdbError, YdbResult};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::{mpsc, oneshot};

use tokio::task::JoinHandle;
use tracing::log::trace;
use tracing::error;

use super::writer_session::WriterSession;


#[derive(Clone)]
#[allow(dead_code)]
pub struct TopicWriter {
    commands: mpsc::Sender<Command>,
    run_loop: Arc<JoinHandle<()>>,
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

enum Command{
    Write(TopicWriterMessage, Option<oneshot::Sender<MessageWriteStatus>>),
    Stop
}

impl TopicWriter {

    pub(crate) fn new(
        opts: TopicWriterOptions,
        connection_manager: GrpcConnectionManager,
    ) -> Self {

        let producer_id = opts.producer_id.clone().unwrap_or_else(||{
            uuid::Uuid::new_v4().to_string()
        });
        let (commands_sender,commands_receiver) = mpsc::channel(32);

        let run_loop = tokio::spawn(async move {
            Self::run_loop(producer_id, opts, connection_manager,  commands_receiver).await;
        });

        Self {
            commands: commands_sender,
            run_loop: Arc::new(run_loop),
        }
    }

    async fn run_loop(producer_id: String, opts: TopicWriterOptions, connection_manager: GrpcConnectionManager, mut rx: mpsc::Receiver<Command>)->YdbResult<()>{

        loop{
            let session = WriterSession::run_session(
                producer_id.clone(),
                &opts,
                connection_manager.clone()
            ).await?;
            
            match Self::command_loop(session, &mut rx).await{
                Ok(_) => {
                    trace!("session closed");
                    break;
                },
                Err(e) => {
                    error!("session closed with error: {}", e);
                    continue;
                }
            }
        }

        Ok(())
    }

    async fn command_loop(mut session: WriterSession, commands_receiver: &mut mpsc::Receiver<Command>)->YdbResult<()>{
        while let Some(command) = commands_receiver.recv().await{
            match command{
                Command::Write(message, ack) => {
                    session.write_message(message, ack).await?;
                },
                Command::Stop => {
                    session.stop().await?;
                    break;
                }
            }
        }
        Ok(())
    }

    pub async fn stop(self) -> YdbResult<()> {

        self.commands.send(Command::Stop).await
            .map_err(|_| YdbError::custom("can't send stop command"))?;
        Ok(())
    }

    pub async fn write(&self, message: TopicWriterMessage) -> YdbResult<()> {
        self.commands.send(Command::Write(message, None)).await
            .map_err( |_| YdbError::custom("can't send write command"))?;
        Ok(())
    }

    pub async fn write_with_ack(
        &self,
        message: TopicWriterMessage,
    ) -> YdbResult<MessageWriteStatus> {
        let (tx, rx)= tokio::sync::oneshot::channel();

        self.commands.send(Command::Write(message, Some(tx))).await
            .map_err( |_| YdbError::custom("can't send write command"))?;

        Ok(rx.await?)
    }

    pub async fn write_with_ack_future(
        &self,
        message: TopicWriterMessage,
    ) -> YdbResult<AckFuture> {

        let (tx, rx)= tokio::sync::oneshot::channel();

        self.commands.send(Command::Write(message, Some(tx))).await
            .map_err( |_| YdbError::custom("can't send write command"))?;

        Ok(AckFuture { receiver: rx })
    }
    
}
