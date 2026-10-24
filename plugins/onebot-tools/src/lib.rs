//! OneBot tools are a guest package; credentials and transport stay with the host.
pub mod contract;
pub mod managed;
mod message;
pub mod output;

use contract::{Error, Invocation};
use forest_onebot::forest::{self as onebot, OneBotError, Query, SendPrivateMessage, SentMessage};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use serde::{Deserialize, de::DeserializeOwned};
use std::rc::Rc;

pub const OBJECT: &str = "/module/onebot-tools";

pub struct OneBotTools;

impl Module for OneBotTools {
    async fn create(_: CreateContext) -> Result<Self, String> {
        Ok(Self)
    }

    fn handlers() -> Handlers<Self> {
        let access = callers(vec![
            SubjectPattern::Path("/module/agent-tools".parse().unwrap()),
            SubjectPattern::Path("/user/admin".parse().unwrap()),
        ]);
        Handlers::new()
            .with(
                "/get_login_info".parse().unwrap(),
                true,
                access.clone(),
                Self::login,
            )
            .with(
                "/get_friend_list".parse().unwrap(),
                true,
                access.clone(),
                Self::friends,
            )
            .with(
                "/get_group_list".parse().unwrap(),
                true,
                access.clone(),
                Self::groups,
            )
            .with(
                "/get_group_info".parse().unwrap(),
                true,
                access.clone(),
                Self::group,
            )
            .with(
                "/get_group_member_list".parse().unwrap(),
                true,
                access.clone(),
                Self::members,
            )
            .with(
                "/get_group_member_info".parse().unwrap(),
                true,
                access.clone(),
                Self::member,
            )
            .with(
                "/get_stranger_info".parse().unwrap(),
                true,
                access.clone(),
                Self::user,
            )
            .with(
                "/send_group_message".parse().unwrap(),
                true,
                access.clone(),
                Self::send_group,
            )
            .with(
                "/send_private_message".parse().unwrap(),
                true,
                access,
                Self::send_private,
            )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Group {
    group_id: Option<i64>,
    #[serde(default)]
    no_cache: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct User {
    user_id: i64,
    #[serde(default)]
    no_cache: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    group_id: Option<i64>,
    user_id: i64,
    #[serde(default)]
    no_cache: bool,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Page {
    #[serde(default)]
    offset: usize,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Members {
    group_id: Option<i64>,
    #[serde(default)]
    offset: usize,
    limit: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupMessage {
    group_id: Option<i64>,
    message: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivateMessage {
    user_id: i64,
    message: String,
}

fn arguments<T: DeserializeOwned>(input: &Invocation) -> Result<T, Error> {
    serde_json::from_str(&input.arguments_json).map_err(|e| Error::Arguments(e.to_string()))
}

async fn query(request: Query) -> Result<String, Error> {
    let data: Result<String, OneBotError> = host::call(
        onebot::OBJECT.parse().unwrap(),
        onebot::QUERY.parse().unwrap(),
        &request,
    )
    .await
    .map_err(|e| Error::Execution(e.to_string()))?;
    data.map_err(|e| Error::Execution(format!("{e:?}")))
}

fn page_options(page: &Page) -> Result<usize, Error> {
    let limit = page.limit.unwrap_or(50);
    if !(1..=100).contains(&limit) {
        return Err(Error::Arguments("limit must be between 1 and 100".into()));
    }
    Ok(limit)
}

async fn list(request: Query, page: Page) -> Result<String, Error> {
    let limit = page_options(&page)?;
    let data = query(request).await?;
    let items: Vec<serde_json::Value> =
        serde_json::from_str(&data).map_err(|e| Error::Execution(e.to_string()))?;
    let total = items.len();
    let selected = items
        .into_iter()
        .skip(page.offset)
        .take(limit)
        .collect::<Vec<_>>();
    let end = page.offset.saturating_add(selected.len());
    Ok(serde_json::json!({
        "items":selected, "total":total, "offset":page.offset,
        "next_offset":(end < total).then_some(end),
    })
    .to_string())
}

impl OneBotTools {
    async fn login(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        let _: Empty = arguments(&input)?;
        query(Query::LoginInfo).await
    }
    async fn friends(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        list(Query::FriendList, arguments(&input)?).await
    }
    async fn groups(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        list(Query::GroupList, arguments(&input)?).await
    }
    async fn group(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        let args: Group = arguments(&input)?;
        query(Query::GroupInfo {
            group_id: args.group_id.unwrap_or(input.group_id),
            no_cache: args.no_cache,
        })
        .await
    }
    async fn members(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        let args: Members = arguments(&input)?;
        list(
            Query::GroupMemberList {
                group_id: args.group_id.unwrap_or(input.group_id),
            },
            Page {
                offset: args.offset,
                limit: args.limit,
            },
        )
        .await
    }
    async fn member(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        let args: Member = arguments(&input)?;
        query(Query::GroupMemberInfo {
            group_id: args.group_id.unwrap_or(input.group_id),
            user_id: args.user_id,
            no_cache: args.no_cache,
        })
        .await
    }
    async fn user(self: Rc<Self>, _: CallContext, input: Invocation) -> Result<String, Error> {
        let args: User = arguments(&input)?;
        query(Query::StrangerInfo {
            user_id: args.user_id,
            no_cache: args.no_cache,
        })
        .await
    }
    async fn send_group(
        self: Rc<Self>,
        _: CallContext,
        input: Invocation,
    ) -> Result<String, Error> {
        let args: GroupMessage = arguments(&input)?;
        let group_id = args.group_id.unwrap_or(input.group_id);
        let sent: Result<SentMessage, output::Error> = host::call(
            output::OBJECT.parse().unwrap(),
            output::SEND.parse().unwrap(),
            &output::GroupOutput {
                group_id,
                bot_id: input.bot_id,
                bot_name: None,
                message: forest_onebot::message::Message::Cq(args.message.clone()),
            },
        )
        .await
        .map_err(|e| Error::Execution(e.to_string()))?;
        let sent = sent.map_err(|e| Error::Execution(format!("{e:?}")))?;
        Ok(serde_json::json!({"group_id":group_id,"message_id":sent.message_id,"message":args.message}).to_string())
    }
    async fn send_private(
        self: Rc<Self>,
        _: CallContext,
        input: Invocation,
    ) -> Result<String, Error> {
        let args: PrivateMessage = arguments(&input)?;
        let message = message::resolve(&args.message).await?;
        let sent: Result<SentMessage, OneBotError> = host::call(
            onebot::OBJECT.parse().unwrap(),
            onebot::SEND_PRIVATE_MESSAGE.parse().unwrap(),
            &SendPrivateMessage {
                user_id: args.user_id,
                message,
                auto_escape: Some(false),
            },
        )
        .await
        .map_err(|e| Error::Execution(e.to_string()))?;
        let sent = sent.map_err(|e| Error::Execution(format!("{e:?}")))?;
        Ok(serde_json::json!({"user_id":args.user_id,"message_id":sent.message_id,"message":args.message}).to_string())
    }
}

forest_sdk::export_module!(super::OneBotTools);

fn dependency<I: AsSchema, O: AsSchema>(object: &str, interface: &str) -> InterfaceUse {
    InterfaceUse {
        object: object.parse().unwrap(),
        interface: interface.parse().unwrap(),
        input: I::as_schema(),
        output: O::as_schema(),
    }
}

pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("OneBot tools".into()),
            description: Some("Account, group and member queries and explicit CQ messaging".into()),
        },
        module: "module.wasm".into(),
        configs: vec![],
        provides: OneBotTools::handlers().declarations(),
        uses: vec![
            dependency::<Query, Result<String, OneBotError>>(onebot::OBJECT, onebot::QUERY),
            dependency::<output::GroupOutput, Result<SentMessage, output::Error>>(
                output::OBJECT,
                output::SEND,
            ),
            dependency::<SendPrivateMessage, Result<SentMessage, OneBotError>>(
                onebot::OBJECT,
                onebot::SEND_PRIVATE_MESSAGE,
            ),
            dependency::<String, Result<managed::Blob, managed::Error>>(managed::OBJECT, "/get"),
        ],
        subscriptions: vec![],
    }
}
