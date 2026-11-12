//! OneBot ingress and native actions. Agent tools remain independent Wasm modules.
use crate::{EventDefinition, NativeInterface, Scheduler};
use anyhow::Result;
use forest_onebot::{
    CallError, Client, Outcome, Transport,
    action::{self, Action},
    forest::{
        self as contract, GroupMessage, OneBotError, Query, SendGroupMessage, SendPrivateMessage,
        SentMessage,
    },
};
use forest_protocol::{manifest::callers, *};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};

#[derive(Clone)]
pub struct OneBotIngress {
    scheduler: Scheduler,
    groups: Arc<GroupScope>,
}

/// Optional host test boundary, independent of a module's enabled groups.
#[derive(Clone, Debug, Default)]
pub enum GroupScope {
    #[default]
    All,
    Only(HashSet<i64>),
}
impl GroupScope {
    pub fn only(groups: impl IntoIterator<Item = i64>) -> Self {
        Self::Only(groups.into_iter().collect())
    }
    fn contains(&self, group: i64) -> bool {
        match self {
            Self::All => true,
            Self::Only(groups) => groups.contains(&group),
        }
    }
}
pub struct IngressReport {
    pub accepted: usize,
    pub rejected: Vec<(Token, DispatchError)>,
}

pub fn install<T: Transport + Send + 'static>(
    scheduler: &Scheduler,
    client: Client<T>,
    groups: GroupScope,
    permitted_callers: Vec<SubjectPattern>,
) -> Result<OneBotIngress> {
    let client = Arc::new(client);
    let groups = Arc::new(groups);
    let allowed = groups.clone();
    let group_client = client.clone();
    scheduler.register_native(NativeInterface::new(
        contract::OBJECT.parse()?,
        contract::SEND_GROUP_MESSAGE.parse()?,
        callers(permitted_callers.clone()),
        move |_, request: SendGroupMessage| {
            let client = group_client.clone();
            let allowed = allowed.clone();
            async move {
                if !allowed.contains(request.group_id) {
                    Err(OneBotError::GroupDenied)
                } else {
                    let action = action::SendGroupMsg {
                        group_id: request.group_id,
                        message: request.message,
                        auto_escape: request.auto_escape,
                    };
                    execute(&client, &action).await.map(|sent| SentMessage {
                        message_id: sent.message_id,
                    })
                }
            }
        },
    ))?;
    let private_client = client.clone();
    let private_groups = groups.clone();
    scheduler.register_native(NativeInterface::new(
        contract::OBJECT.parse()?,
        contract::SEND_PRIVATE_MESSAGE.parse()?,
        callers(permitted_callers.clone()),
        move |_, request: SendPrivateMessage| {
            let client = private_client.clone();
            let groups = private_groups.clone();
            async move {
                // An explicitly group-scoped test host must not send private messages.
                if !matches!(*groups, GroupScope::All) {
                    return Err(OneBotError::GroupDenied);
                }
                execute(
                    &client,
                    &action::SendPrivateMsg {
                        user_id: request.user_id,
                        message: request.message,
                        auto_escape: request.auto_escape,
                    },
                )
                .await
                .map(|sent| SentMessage {
                    message_id: sent.message_id,
                })
            }
        },
    ))?;
    let query_groups = groups.clone();
    scheduler.register_native(NativeInterface::new(
        contract::OBJECT.parse()?,
        contract::QUERY.parse()?,
        callers(permitted_callers),
        move |_, request: Query| {
            let client = client.clone();
            let groups = query_groups.clone();
            async move { query(&client, &groups, request).await }
        },
    ))?;
    scheduler.register_event(EventDefinition {
        source: contract::OBJECT.parse()?,
        event: contract::GROUP_MESSAGE.parse()?,
        input: GroupMessage::as_schema(),
    })?;
    Ok(OneBotIngress {
        scheduler: scheduler.clone(),
        groups,
    })
}

async fn execute<T: Transport, A: Action>(
    client: &Client<T>,
    action: &A,
) -> Result<A::Response, OneBotError> {
    match client.call(action).await {
        Ok(Outcome::Completed(data)) => Ok(data),
        Ok(Outcome::Submitted) => Err(OneBotError::Submitted),
        Err(error) => Err(match error {
            CallError::Transport(error) => OneBotError::Transport(error.to_string()),
            CallError::Encode(error) => OneBotError::Encode(error.to_string()),
            CallError::Decode(error) => OneBotError::Decode(error.to_string()),
            CallError::Protocol(error) => OneBotError::Protocol(error.to_string()),
            CallError::Api(error) => OneBotError::Api {
                retcode: error.retcode,
                message: error.message,
                wording: error.wording,
            },
        }),
    }
}

fn json(data: impl Serialize) -> Result<String, OneBotError> {
    serde_json::to_string(&data).map_err(|e| OneBotError::Encode(e.to_string()))
}

async fn query<T: Transport>(
    client: &Client<T>,
    groups: &GroupScope,
    request: Query,
) -> Result<String, OneBotError> {
    let group = match &request {
        Query::GroupInfo { group_id, .. }
        | Query::GroupMemberList { group_id }
        | Query::GroupMemberInfo { group_id, .. } => Some(*group_id),
        _ => None,
    };
    if group.is_some_and(|group| !groups.contains(group)) {
        return Err(OneBotError::GroupDenied);
    }
    match request {
        Query::LoginInfo => json(execute(client, &action::GetLoginInfo {}).await?),
        Query::FriendList => json(execute(client, &action::GetFriendList {}).await?),
        Query::GroupList => {
            let mut data = execute(client, &action::GetGroupList {}).await?;
            data.retain(|group| groups.contains(group.group_id));
            json(data)
        }
        Query::GroupInfo { group_id, no_cache } => json(
            execute(
                client,
                &action::GetGroupInfo {
                    group_id,
                    no_cache: Some(no_cache),
                },
            )
            .await?,
        ),
        Query::GroupMemberList { group_id } => {
            json(execute(client, &action::GetGroupMemberList { group_id }).await?)
        }
        Query::GroupMemberInfo {
            group_id,
            user_id,
            no_cache,
        } => json(
            execute(
                client,
                &action::GetGroupMemberInfo {
                    group_id,
                    user_id,
                    no_cache: Some(no_cache),
                },
            )
            .await?,
        ),
        Query::StrangerInfo { user_id, no_cache } => json(
            execute(
                client,
                &action::GetStrangerInfo {
                    user_id,
                    no_cache: Some(no_cache),
                },
            )
            .await?,
        ),
    }
}

impl OneBotIngress {
    pub fn receive(&self, bytes: &[u8]) -> Result<IngressReport> {
        #[derive(Deserialize)]
        struct Header {
            post_type: Option<String>,
            message_type: Option<String>,
            group_id: Option<i64>,
        }
        let header: Header = serde_json::from_slice(bytes)?;
        if header.post_type.as_deref() != Some("message")
            || header.message_type.as_deref() != Some("group")
        {
            return Ok(IngressReport {
                accepted: 0,
                rejected: vec![],
            });
        }
        if !header
            .group_id
            .is_some_and(|group| self.groups.contains(group))
        {
            return Ok(IngressReport {
                accepted: 0,
                rejected: vec![],
            });
        }
        let event: GroupMessage = serde_json::from_slice(bytes)?;
        let origin: Token = format!("/user/onebot/{}", event.user_id).parse()?;
        let results = self.scheduler.notify(
            &contract::OBJECT.parse()?,
            &contract::GROUP_MESSAGE.parse()?,
            origin,
            &event,
        );
        let accepted = results.iter().filter(|(_, result)| result.is_ok()).count();
        let rejected = results
            .into_iter()
            .filter_map(|(object, result)| result.err().map(|error| (object, error)))
            .collect();
        Ok(IngressReport { accepted, rejected })
    }
}
