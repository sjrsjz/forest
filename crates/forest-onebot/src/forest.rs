//! Forest contracts preserve v11 field names and message representations.
use crate::message::Message;
use forest_serde::{DeriveSchema, DeriveValue};
use serde::{Deserialize, Serialize};

pub const OBJECT: &str = "/core/onebot";
pub const GROUP_MESSAGE: &str = "/group_message";
pub const SEND_GROUP_MESSAGE: &str = "/send_group_message";
pub const SEND_PRIVATE_MESSAGE: &str = "/send_private_message";
pub const QUERY: &str = "/query";

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct GroupMessage {
    pub time: i64,
    pub self_id: i64,
    pub post_type: String,
    pub message_type: String,
    pub sub_type: String,
    pub message_id: i32,
    pub group_id: i64,
    pub user_id: i64,
    pub anonymous: Option<Anonymous>,
    pub message: Message,
    pub raw_message: String,
    pub sender: GroupSender,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct Anonymous {
    pub id: i64,
    pub name: String,
    pub flag: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct GroupSender {
    pub user_id: Option<i64>,
    pub nickname: Option<String>,
    pub card: Option<String>,
    pub sex: Option<String>,
    pub age: Option<i32>,
    pub area: Option<String>,
    pub level: Option<String>,
    pub role: Option<String>,
    pub title: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct SendGroupMessage {
    pub group_id: i64,
    pub message: Message,
    pub auto_escape: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct SendPrivateMessage {
    pub user_id: i64,
    pub message: Message,
    pub auto_escape: Option<bool>,
}

/// Read-only v11 actions exposed to modules; responses are the action's JSON data.
#[derive(Clone, Debug, DeriveSchema, DeriveValue)]
pub enum Query {
    LoginInfo,
    FriendList,
    GroupList,
    GroupInfo {
        group_id: i64,
        no_cache: bool,
    },
    GroupMemberList {
        group_id: i64,
    },
    GroupMemberInfo {
        group_id: i64,
        user_id: i64,
        no_cache: bool,
    },
    StrangerInfo {
        user_id: i64,
        no_cache: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub struct SentMessage {
    pub message_id: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize, DeriveSchema, DeriveValue)]
pub enum OneBotError {
    Transport(String),
    Encode(String),
    Decode(String),
    Protocol(String),
    Api {
        retcode: i32,
        message: Option<String>,
        wording: Option<String>,
    },
    Submitted,
    GroupDenied,
}
