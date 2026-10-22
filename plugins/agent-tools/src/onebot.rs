use crate::{Route, route};
use serde_json::json;

const OBJECT: &str = "/module/onebot-tools";

pub fn routes() -> Vec<Route> {
    let group_id = json!({"type":"integer","description":"QQ group ID. Omit to use the current conversation's group."});
    let user_id = json!({"type":"integer","description":"QQ account ID, not a nickname. Use sender IDs from the conversation or query group members."});
    let no_cache = json!({"type":"boolean","default":false,"description":"Request fresh information from the gateway rather than its cache."});
    let page = json!({
        "type":"object",
        "properties":{
            "offset":{"type":"integer","minimum":0,"default":0},
            "limit":{"type":"integer","minimum":1,"maximum":100,"default":50}
        },
        "additionalProperties":false
    });
    let message = json!({"type":"string","minLength":1,"description":"Message text with optional CQ at/reply/face/image codes. Images must use media:<sha256> references returned by tools; never supply base64, URLs or filesystem paths. Literal CQ brackets must be escaped as &#91; and &#93;."});
    vec![
        route(
            "onebot_get_login_info",
            "Query the connected bot's own QQ account ID and nickname.",
            json!({"type":"object","properties":{},"additionalProperties":false}),
            OBJECT,
            "/get_login_info",
        ),
        route(
            "onebot_get_friend_list",
            "List the bot's QQ friends with account IDs, nicknames and remarks. Returns items, total and next_offset; paginate until next_offset is null when needed.",
            page.clone(),
            OBJECT,
            "/get_friend_list",
        ),
        route(
            "onebot_get_group_list",
            "List QQ groups the bot has joined, with group IDs, names and membership counts. Returns items, total and next_offset; paginate as needed. Membership does not imply an Agent is enabled in that group.",
            page,
            OBJECT,
            "/get_group_list",
        ),
        route(
            "onebot_get_group_info",
            "Query a QQ group's name, member count and capacity. Defaults to the current group.",
            json!({"type":"object","properties":{"group_id":group_id,"no_cache":no_cache},"additionalProperties":false}),
            OBJECT,
            "/get_group_info",
        ),
        route(
            "onebot_get_group_member_list",
            "List QQ group members with account IDs, nicknames, group cards and owner/admin/member roles. Defaults to the current group. Returns items, total and next_offset; use pagination instead of assuming the first page is the entire group.",
            json!({
                "type":"object","properties":{
                    "group_id":group_id,
                    "offset":{"type":"integer","minimum":0,"default":0},
                    "limit":{"type":"integer","minimum":1,"maximum":100,"default":50}
                },"additionalProperties":false
            }),
            OBJECT,
            "/get_group_member_list",
        ),
        route(
            "onebot_get_group_member_info",
            "Query a particular member's group card, role, title, join time and other gateway-provided profile fields. Defaults to the current group; use the bot's account ID to inspect its own role.",
            json!({"type":"object","properties":{"group_id":group_id,"user_id":user_id,"no_cache":no_cache},"required":["user_id"],"additionalProperties":false}),
            OBJECT,
            "/get_group_member_info",
        ),
        route(
            "onebot_get_stranger_info",
            "Query a QQ user's public profile by account ID, including nickname, sex and age as provided by the gateway. This does not require assuming the user is a friend or belongs to the current group.",
            json!({"type":"object","properties":{"user_id":user_id,"no_cache":no_cache},"required":["user_id"],"additionalProperties":false}),
            OBJECT,
            "/get_stranger_info",
        ),
        route(
            "onebot_send_group_message",
            "Immediately send a separate QQ group message and return its actual message_id. Defaults to the current group; an explicit group_id can target another joined group. Supports text and CQ at/reply/face/managed-image codes. Successful calls have already sent the message: do not repeat that content in your assistant reply. Ordinary replies to the current conversation are sent automatically and do not require this tool.",
            json!({"type":"object","properties":{"group_id":group_id,"message":message},"required":["message"],"additionalProperties":false}),
            OBJECT,
            "/send_group_message",
        ),
        route(
            "onebot_send_private_message",
            "Immediately send a separate private QQ message and return its actual message_id. The gateway determines whether the recipient can be reached; do not claim delivery if it fails. Supports text and CQ face/managed-image codes. A successful call has already sent the message; do not repeat it as an assistant reply.",
            json!({"type":"object","properties":{"user_id":user_id,"message":message},"required":["user_id","message"],"additionalProperties":false}),
            OBJECT,
            "/send_private_message",
        ),
    ]
}
