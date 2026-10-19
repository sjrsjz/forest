//! Typed data returned by the standard public actions.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::message::Message;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sex {
    Male,
    Female,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Owner,
    Admin,
    Member,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SentMessage {
    pub message_id: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginInfo {
    pub user_id: i64,
    pub nickname: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrangerInfo {
    pub user_id: i64,
    pub nickname: String,
    pub sex: Sex,
    pub age: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FriendInfo {
    pub user_id: i64,
    pub nickname: String,
    pub remark: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupInfo {
    pub group_id: i64,
    pub group_name: String,
    pub member_count: i32,
    pub max_member_count: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberInfo {
    pub group_id: i64,
    pub user_id: i64,
    pub nickname: String,
    pub card: String,
    pub sex: Sex,
    pub age: i32,
    pub area: Option<String>,
    pub join_time: i64,
    pub last_sent_time: i64,
    pub level: String,
    pub role: Role,
    pub unfriendly: bool,
    pub title: Option<String>,
    pub title_expire_time: Option<i64>,
    pub card_changeable: Option<bool>,
}

/// Sender fields other than user_id are best-effort in message reports.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateSender {
    pub user_id: i64,
    pub nickname: Option<String>,
    pub sex: Option<Sex>,
    pub age: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupSender {
    #[serde(flatten)]
    pub user: PrivateSender,
    pub card: Option<String>,
    pub area: Option<String>,
    pub level: Option<String>,
    pub role: Option<Role>,
    pub title: Option<String>,
}

/// The discriminator belongs to get_msg's data object, not its sender object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "message_type", rename_all = "snake_case")]
pub enum MessageSource {
    Private { sender: PrivateSender },
    Group { sender: GroupSender },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageInfo {
    pub time: i64,
    pub message_id: i32,
    pub real_id: i32,
    #[serde(flatten)]
    pub source: MessageSource,
    pub message: Message,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardMessage {
    pub message: Message,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Anonymous {
    pub id: i64,
    pub name: String,
    pub flag: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupHonor {
    pub group_id: i64,
    pub current_talkative: Option<CurrentTalkative>,
    pub talkative_list: Option<Vec<HonorMember>>,
    pub performer_list: Option<Vec<HonorMember>>,
    pub legend_list: Option<Vec<HonorMember>>,
    pub strong_newbie_list: Option<Vec<HonorMember>>,
    pub emotion_list: Option<Vec<HonorMember>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentTalkative {
    pub user_id: i64,
    pub nickname: String,
    pub avatar: String,
    pub day_count: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HonorMember {
    pub user_id: i64,
    pub nickname: String,
    pub avatar: String,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cookies {
    pub cookies: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsrfToken {
    pub token: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Credentials {
    pub cookies: String,
    pub csrf_token: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileInfo {
    pub file: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capability {
    pub yes: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ImplementationStatus {
    pub online: Option<bool>,
    pub good: bool,
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionInfo {
    pub app_name: String,
    pub app_version: String,
    pub protocol_version: String,
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}
