//! Host ingress and one explicit native action. The protocol crate stays pure.
use crate::{EventDefinition, NativeInterface, Scheduler};
use anyhow::Result;
use forest_onebot::{
    CallError, Client, Outcome, Transport,
    action::SendGroupMsg,
    forest::{self as contract, GroupMessage, OneBotError, SendGroupMessage, SentMessage},
};
use forest_protocol::{manifest::callers, *};
use serde::Deserialize;
use std::{collections::HashSet, sync::Arc};

#[derive(Clone)]
pub struct OneBotIngress {
    scheduler: Scheduler,
    groups: Arc<HashSet<i64>>,
}
pub struct IngressReport {
    pub accepted: usize,
    pub rejected: Vec<(Token, DispatchError)>,
}

pub fn install<T: Transport + Send + 'static>(
    scheduler: &Scheduler,
    client: Client<T>,
    groups: Vec<i64>,
    permitted_callers: Vec<SubjectPattern>,
) -> Result<OneBotIngress> {
    let client = Arc::new(client);
    let groups: Arc<HashSet<i64>> = Arc::new(groups.into_iter().collect());
    let allowed = groups.clone();
    scheduler.register_native(NativeInterface::new(
        contract::OBJECT.parse()?,
        contract::SEND_GROUP_MESSAGE.parse()?,
        true,
        callers(permitted_callers),
        move |_, request: SendGroupMessage| {
            let client = client.clone();
            let allowed = allowed.clone();
            async move {
                if !allowed.contains(&request.group_id) {
                    Err(OneBotError::GroupDenied)
                } else {
                    let action = SendGroupMsg {
                        group_id: request.group_id,
                        message: request.message,
                        auto_escape: request.auto_escape,
                    };
                    match client.call(&action).await {
                        Ok(Outcome::Completed(sent)) => Ok(SentMessage {
                            message_id: sent.message_id,
                        }),
                        Ok(Outcome::Submitted) => Err(OneBotError::Submitted),
                        Err(error) => Err(match error {
                            CallError::Transport(error) => {
                                OneBotError::Transport(error.to_string())
                            }
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
            }
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
            .is_some_and(|group| self.groups.contains(&group))
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
