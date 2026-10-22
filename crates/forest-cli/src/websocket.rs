//! OneBot's duplex socket carries both API receipts and host events. Echo is
//! transport correlation only; the runtime still dispatches typed interfaces.
use forest_onebot::Transport;
use forest_runtime::{LogWriter, onebot::OneBotIngress};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

#[derive(Clone)]
pub struct Actions(watch::Receiver<Option<mpsc::Sender<Action>>>);
pub struct Connections {
    current: watch::Sender<Option<mpsc::Sender<Action>>>,
    capacity: usize,
}
/// Exclusive borrow prevents overlapping connections from clearing each other's sender.
pub struct Session<'a> {
    owner: &'a mut Connections,
    incoming: mpsc::Receiver<Action>,
}
impl Connections {
    pub fn connected(&mut self) -> Session<'_> {
        let (sender, incoming) = mpsc::channel(self.capacity);
        self.current.send_replace(Some(sender));
        Session {
            owner: self,
            incoming,
        }
    }
}
impl Drop for Session<'_> {
    fn drop(&mut self) {
        self.owner.current.send_replace(None);
    }
}
struct Action {
    name: String,
    params: Vec<u8>,
    reply: oneshot::Sender<Vec<u8>>,
}
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("OneBot is disconnected")]
    Disconnected,
    #[error("OneBot connection closed before receipt; the action was not replayed")]
    ReceiptLost,
    #[error("OneBot outgoing queue full")]
    QueueFull,
}
pub fn channel(capacity: usize) -> (Actions, Connections) {
    let (tx, rx) = watch::channel(None);
    (
        Actions(rx),
        Connections {
            current: tx,
            capacity,
        },
    )
}
impl Transport for Actions {
    type Error = Error;
    async fn request(&self, action: &str, params: Vec<u8>) -> Result<Vec<u8>, Error> {
        let (reply, result) = oneshot::channel();
        let sender = self.0.borrow().clone().ok_or(Error::Disconnected)?;
        sender
            .try_send(Action {
                name: action.into(),
                params,
                reply,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                mpsc::error::TrySendError::Closed(_) => Error::Disconnected,
            })?;
        result.await.map_err(|_| Error::ReceiptLost)
    }
}

pub async fn serve<S>(
    mut socket: WebSocketStream<S>,
    ingress: &OneBotIngress,
    mut session: Session<'_>,
    logs: &LogWriter,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut next = 0u64;
    let mut pending = HashMap::new();
    loop {
        tokio::select! {
            action=session.incoming.recv()=>{
                let Some(action)=action else{return Ok(());};
                next=next.checked_add(1).expect("OneBot echo exhausted");
                let echo=format!("forest:{next}");
                let params:serde_json::Value=serde_json::from_slice(&action.params)?;
                let request=serde_json::json!({"action":action.name,"params":params,"echo":echo});
                pending.insert(echo,action.reply);
                socket.send(Message::Text(serde_json::to_string(&request)?.into())).await?;
            }
            message=socket.next()=>{
                let Some(message)=message else{return Ok(());};
                let bytes=match message?{
                    Message::Text(text)=>text.as_bytes().to_vec(),
                    Message::Binary(bytes)=>bytes.to_vec(),
                    Message::Ping(bytes)=>{socket.send(Message::Pong(bytes)).await?;continue;},
                    Message::Close(_)=>return Ok(()),
                    Message::Pong(_)|Message::Frame(_)=>continue,
                };
                #[derive(Deserialize)]struct Envelope{echo:Option<String>}
                let header:Envelope=serde_json::from_slice(&bytes)?;
                if let Some(echo)=header.echo{
                    if let Some(reply)=pending.remove(&echo){let _=reply.send(bytes);}
                }else{super::dispatch_event(ingress,logs,&bytes);}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forest_runtime::{
        RuntimeOptions, Scheduler, StopMode,
        onebot::{self, GroupScope},
    };

    #[tokio::test]
    async fn connection_loss_settles_receipts_and_never_replays_actions() {
        let temp = tempfile::tempdir().unwrap();
        let scheduler = Scheduler::in_memory(RuntimeOptions {
            data_directory: temp.path().into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let (actions, mut connections) = channel(8);
        let logs = scheduler.logs();
        let ingress = onebot::install(
            &scheduler,
            forest_onebot::Client::new(actions.clone()),
            GroupScope::All,
            vec![],
        )
        .unwrap();
        assert!(matches!(
            actions.request("test", b"{}".to_vec()).await,
            Err(Error::Disconnected)
        ));
        for (number, lose_receipt) in [(1, true), (2, false)] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let incoming = async {
                let (socket, _) = listener.accept().await.unwrap();
                tokio_tungstenite::accept_async(socket).await.unwrap()
            };
            let (client, mut peer) = tokio::join!(
                tokio_tungstenite::connect_async(format!(
                    "ws://{}",
                    listener.local_addr().unwrap()
                )),
                incoming,
            );
            let exchange = async {
                let wire = async {
                    let message = peer.next().await.unwrap().unwrap();
                    let value: serde_json::Value =
                        serde_json::from_slice(&message.into_data()).unwrap();
                    assert_eq!(value["params"]["number"], number);
                    if !lose_receipt {
                        peer.send(Message::Text(
                            serde_json::json!({"echo":value["echo"],"data":{"number":number}})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                    }
                    peer.close(None).await.unwrap();
                };
                let (result, ()) = tokio::join!(
                    actions.request(
                        "test",
                        serde_json::to_vec(&serde_json::json!({"number":number})).unwrap()
                    ),
                    wire
                );
                if lose_receipt {
                    assert!(matches!(result, Err(Error::ReceiptLost)));
                } else {
                    assert!(result.is_ok());
                }
            };
            let (result, ()) = tokio::join!(
                serve(client.unwrap().0, &ingress, connections.connected(), &logs),
                exchange
            );
            result.unwrap();
            assert!(matches!(
                actions.request("test", b"{}".to_vec()).await,
                Err(Error::Disconnected)
            ));
        }
        scheduler.shutdown(StopMode::Drain).await.unwrap();
    }
}
