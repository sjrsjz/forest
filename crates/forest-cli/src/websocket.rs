//! OneBot's duplex socket carries both API receipts and host events. Echo is
//! transport correlation only; the runtime still dispatches typed interfaces.
use forest_onebot::Transport;
use forest_runtime::onebot::OneBotIngress;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

#[derive(Clone)]
pub struct Actions(mpsc::Sender<Action>);
pub struct Incoming(mpsc::Receiver<Action>);
struct Action {
    name: String,
    params: Vec<u8>,
    reply: oneshot::Sender<Vec<u8>>,
}
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("OneBot connection closed")]
    Closed,
    #[error("OneBot outgoing queue full")]
    QueueFull,
}
pub fn channel(capacity: usize) -> (Actions, Incoming) {
    let (tx, rx) = mpsc::channel(capacity);
    (Actions(tx), Incoming(rx))
}
impl Transport for Actions {
    type Error = Error;
    async fn request(&self, action: &str, params: Vec<u8>) -> Result<Vec<u8>, Error> {
        let (reply, result) = oneshot::channel();
        self.0
            .try_send(Action {
                name: action.into(),
                params,
                reply,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })?;
        result.await.map_err(|_| Error::Closed)
    }
}

pub async fn serve<S>(
    mut socket: WebSocketStream<S>,
    ingress: &OneBotIngress,
    mut actions: Incoming,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut next = 0u64;
    let mut pending = HashMap::new();
    loop {
        tokio::select! {
            action=actions.0.recv()=>{
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
                }else{super::dispatch_event(ingress,&bytes);}
            }
        }
    }
}
