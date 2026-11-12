use super::*;
use forest_onebot::{
    Status,
    action::{self, Action},
};
use forest_protocol::service::{WebSocketMessage, WebSocketSend};
use futures::future::{Either, select};
use serde::{Deserialize, Serialize};

struct Receipt<'a> {
    key: String,
    pending: &'a RefCell<HashMap<String, oneshot::Sender<Result<Vec<u8>, OneBotError>>>>,
}
impl Drop for Receipt<'_> {
    fn drop(&mut self) {
        self.pending.borrow_mut().remove(&self.key);
    }
}

impl Adapter {
    async fn execute<A: Action>(&self, action: &A) -> Result<A::Response, OneBotError> {
        let socket = self.socket.get().ok_or(OneBotError::Disconnected)?;
        let next = self
            .next
            .get()
            .checked_add(1)
            .expect("OneBot request id exhausted");
        self.next.set(next);
        let key = next.to_string();
        let (reply, receipt) = oneshot::channel();
        self.pending.borrow_mut().insert(key.clone(), reply);
        let _receipt = Receipt {
            key: key.clone(),
            pending: &self.pending,
        };
        let wire = serde_json::to_string(
            &serde_json::json!({"action": A::NAME, "params": action, "echo": key}),
        )
        .map_err(|e| OneBotError::Encode(e.to_string()))?;
        // One deadline covers both sending and waiting for the matching receipt.
        let exchange = Box::pin(async {
            host::ws_send(WebSocketSend {
                handle: socket,
                message: WebSocketMessage::Text(wire),
                timeout_ms: None,
            })
            .await
            .map_err(|e| OneBotError::Transport(e.to_string()))?
            .map_err(|e| OneBotError::Transport(e.to_string()))?;
            receipt.await.map_err(|_| OneBotError::ReceiptLost)?
        });
        let bytes = match select(exchange, host::sleep(self.settings.action_timeout_ms)).await {
            Either::Left((result, _)) => result?,
            Either::Right((_, _)) => return Err(OneBotError::Timeout),
        };
        #[derive(Deserialize)]
        struct Response {
            status: Status,
            retcode: i32,
            #[serde(default)]
            data: serde_json::Value,
            message: Option<String>,
            wording: Option<String>,
        }
        let response: Response =
            serde_json::from_slice(&bytes).map_err(|e| OneBotError::Decode(e.to_string()))?;
        match (response.status, response.retcode) {
            (Status::Ok, 0) => serde_json::from_value(response.data)
                .map_err(|e| OneBotError::Decode(e.to_string())),
            (Status::Async, 1) => Err(OneBotError::Submitted),
            (Status::Failed, code) if code != 0 && code != 1 => Err(OneBotError::Api {
                retcode: code,
                message: response.message,
                wording: response.wording,
            }),
            (status, retcode) => Err(OneBotError::Protocol(format!(
                "inconsistent status {status:?}, retcode {retcode}"
            ))),
        }
    }

    pub(super) async fn send_group(
        self: Rc<Self>,
        _: CallContext,
        request: SendGroupMessage,
    ) -> Result<SentMessage, OneBotError> {
        if !self.group_allowed(request.group_id) {
            return Err(OneBotError::GroupDenied);
        }
        let sent = self
            .execute(&action::SendGroupMsg {
                group_id: request.group_id,
                message: request.message,
                auto_escape: request.auto_escape,
            })
            .await?;
        Ok(SentMessage {
            message_id: sent.message_id,
        })
    }

    pub(super) async fn send_private(
        self: Rc<Self>,
        _: CallContext,
        request: SendPrivateMessage,
    ) -> Result<SentMessage, OneBotError> {
        if !self.settings.allowed_groups.is_empty() {
            return Err(OneBotError::GroupDenied);
        }
        let sent = self
            .execute(&action::SendPrivateMsg {
                user_id: request.user_id,
                message: request.message,
                auto_escape: request.auto_escape,
            })
            .await?;
        Ok(SentMessage {
            message_id: sent.message_id,
        })
    }
    pub(super) async fn query(
        self: Rc<Self>,
        _: CallContext,
        request: Query,
    ) -> Result<String, OneBotError> {
        let group = match &request {
            Query::GroupInfo { group_id, .. }
            | Query::GroupMemberList { group_id }
            | Query::GroupMemberInfo { group_id, .. } => Some(*group_id),
            _ => None,
        };
        if group.is_some_and(|group| !self.group_allowed(group)) {
            return Err(OneBotError::GroupDenied);
        }
        match request {
            Query::LoginInfo => json(self.execute(&action::GetLoginInfo {}).await?),
            Query::FriendList => json(self.execute(&action::GetFriendList {}).await?),
            Query::GroupList => {
                let mut data = self.execute(&action::GetGroupList {}).await?;
                data.retain(|group| self.group_allowed(group.group_id));
                json(data)
            }
            Query::GroupInfo { group_id, no_cache } => json(
                self.execute(&action::GetGroupInfo {
                    group_id,
                    no_cache: Some(no_cache),
                })
                .await?,
            ),
            Query::GroupMemberList { group_id } => json(
                self.execute(&action::GetGroupMemberList { group_id })
                    .await?,
            ),
            Query::GroupMemberInfo {
                group_id,
                user_id,
                no_cache,
            } => json(
                self.execute(&action::GetGroupMemberInfo {
                    group_id,
                    user_id,
                    no_cache: Some(no_cache),
                })
                .await?,
            ),
            Query::StrangerInfo { user_id, no_cache } => json(
                self.execute(&action::GetStrangerInfo {
                    user_id,
                    no_cache: Some(no_cache),
                })
                .await?,
            ),
        }
    }
}

fn json(value: impl Serialize) -> Result<String, OneBotError> {
    serde_json::to_string(&value).map_err(|e| OneBotError::Encode(e.to_string()))
}
