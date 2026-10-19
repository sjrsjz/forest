//! The 38 public v11 actions. A request fixes its response type at compile time.

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{message::Message, response::*};

/// Implement explicitly for an implementation-specific action; no string dispatch.
pub trait Action: Serialize {
    type Response: DeserializeOwned;
    const NAME: &'static str;
}

// Declares required fields before `;`, and optional fields after it. This only
// generates concrete structs and trait impls; it is not a runtime registry.
macro_rules! action {
    ($name:ident, $wire:literal => $response:ty {
        $($(#[$attr:meta])* $field:ident: $ty:ty),* ;
        $($optional:ident: $optional_ty:ty),* $(,)?
    }) => {
        #[doc = concat!("Parameters for `", $wire, "`.")]
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        pub struct $name {
            $($(#[$attr])* pub $field: $ty,)*
            $(#[serde(skip_serializing_if = "Option::is_none")]
              pub $optional: Option<$optional_ty>,)*
        }
        impl Action for $name {
            type Response = $response;
            const NAME: &'static str = $wire;
        }
    };
}

action!(SendPrivateMsg, "send_private_msg" => SentMessage { user_id: i64, message: Message; auto_escape: bool });
action!(SendGroupMsg, "send_group_msg" => SentMessage { group_id: i64, message: Message; auto_escape: bool });

/// Selects a valid destination rather than requiring both user_id and group_id.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "message_type", rename_all = "snake_case")]
pub enum SendMsg {
    Private {
        user_id: i64,
        message: Message,
        #[serde(skip_serializing_if = "Option::is_none")]
        auto_escape: Option<bool>,
    },
    Group {
        group_id: i64,
        message: Message,
        #[serde(skip_serializing_if = "Option::is_none")]
        auto_escape: Option<bool>,
    },
}

impl Action for SendMsg {
    type Response = SentMessage;
    const NAME: &'static str = "send_msg";
}

action!(DeleteMsg, "delete_msg" => () { message_id: i32; });
action!(GetMsg, "get_msg" => MessageInfo { message_id: i32; });
action!(GetForwardMsg, "get_forward_msg" => ForwardMessage { id: String; });
action!(SendLike, "send_like" => () { user_id: i64; times: i32 });
action!(SetGroupKick, "set_group_kick" => () { group_id: i64, user_id: i64; reject_add_request: bool });
action!(SetGroupBan, "set_group_ban" => () { group_id: i64, user_id: i64; duration: i64 });

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnonymousTarget {
    User { anonymous: Anonymous },
    Flag { anonymous_flag: String },
}

action!(SetGroupAnonymousBan, "set_group_anonymous_ban" => () {
    group_id: i64, #[serde(flatten)] target: AnonymousTarget; duration: i64
});
action!(SetGroupWholeBan, "set_group_whole_ban" => () { group_id: i64; enable: bool });
action!(SetGroupAdmin, "set_group_admin" => () { group_id: i64, user_id: i64; enable: bool });
action!(SetGroupAnonymous, "set_group_anonymous" => () { group_id: i64; enable: bool });
action!(SetGroupCard, "set_group_card" => () { group_id: i64, user_id: i64; card: String });
action!(SetGroupName, "set_group_name" => () { group_id: i64, group_name: String; });
action!(SetGroupLeave, "set_group_leave" => () { group_id: i64; is_dismiss: bool });
action!(SetGroupSpecialTitle, "set_group_special_title" => () {
    group_id: i64, user_id: i64; special_title: String, duration: i64
});
action!(SetFriendAddRequest, "set_friend_add_request" => () { flag: String; approve: bool, remark: String });

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupRequestType {
    Add,
    Invite,
}

action!(SetGroupAddRequest, "set_group_add_request" => () {
    flag: String, sub_type: GroupRequestType; approve: bool, reason: String
});
action!(GetLoginInfo, "get_login_info" => LoginInfo { ; });
action!(GetStrangerInfo, "get_stranger_info" => StrangerInfo { user_id: i64; no_cache: bool });
action!(GetFriendList, "get_friend_list" => Vec<FriendInfo> { ; });
action!(GetGroupInfo, "get_group_info" => GroupInfo { group_id: i64; no_cache: bool });
action!(GetGroupList, "get_group_list" => Vec<GroupInfo> { ; });
action!(GetGroupMemberInfo, "get_group_member_info" => MemberInfo { group_id: i64, user_id: i64; no_cache: bool });
action!(GetGroupMemberList, "get_group_member_list" => Vec<MemberInfo> { group_id: i64; });

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HonorType {
    Talkative,
    Performer,
    Legend,
    StrongNewbie,
    Emotion,
    All,
}

action!(GetGroupHonorInfo, "get_group_honor_info" => GroupHonor { group_id: i64, #[serde(rename = "type")] kind: HonorType; });
action!(GetCookies, "get_cookies" => Cookies { ; domain: String });
action!(GetCsrfToken, "get_csrf_token" => CsrfToken { ; });
action!(GetCredentials, "get_credentials" => Credentials { ; domain: String });

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RecordFormat {
    Mp3,
    Amr,
    Wma,
    M4a,
    Spx,
    Ogg,
    Wav,
    Flac,
}

action!(GetRecord, "get_record" => FileInfo { file: String, out_format: RecordFormat; });
action!(GetImage, "get_image" => FileInfo { file: String; });
action!(CanSendImage, "can_send_image" => Capability { ; });
action!(CanSendRecord, "can_send_record" => Capability { ; });
action!(GetStatus, "get_status" => ImplementationStatus { ; });
action!(GetVersionInfo, "get_version_info" => VersionInfo { ; });
action!(SetRestart, "set_restart" => () { ; delay: i64 });
action!(CleanCache, "clean_cache" => () { ; });
