use std::convert::Infallible;

use forest_onebot::{CallError, CallMode, Client, Outcome, ProtocolError, Status, Transport};
use forest_onebot::{
    action::*,
    message::{Message, Segment},
    response::*,
};
use serde_json::json;

struct Fixture(&'static [u8]);

impl Transport for Fixture {
    type Error = Infallible;

    async fn request(&self, _: &str, _: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
        Ok(self.0.to_vec())
    }
}

#[test]
fn request_types_encode_valid_destinations_and_omit_optional_defaults() {
    let request = SendMsg::Group {
        group_id: 12,
        message: Message::Segments(vec![Segment::at("34"), Segment::text("hello")]),
        auto_escape: None,
    };
    assert_eq!(
        serde_json::to_value(request).unwrap(),
        json!({
            "message_type":"group", "group_id":12,
            "message":[{"type":"at","data":{"qq":"34"}}, {"type":"text","data":{"text":"hello"}}]
        })
    );
    assert_eq!(serde_json::to_value(GetLoginInfo {}).unwrap(), json!({}));
    assert_eq!(
        serde_json::to_value(SetGroupAnonymousBan {
            group_id: 12,
            target: AnonymousTarget::Flag {
                anonymous_flag: "flag".into()
            },
            duration: Some(0),
        })
        .unwrap(),
        json!({"group_id":12,"anonymous_flag":"flag","duration":0})
    );
    assert_eq!(
        serde_json::to_value(GetGroupHonorInfo {
            group_id: 12,
            kind: HonorType::StrongNewbie,
        })
        .unwrap(),
        json!({"group_id":12,"type":"strong_newbie"})
    );
}

#[tokio::test]
async fn completion_submission_and_remote_failure_remain_distinct() {
    let client = Client::new(Fixture(
        br#"{"status":"ok","retcode":0,"data":{"message_id":-17}}"#,
    ));
    let request = SendGroupMsg {
        group_id: 12,
        message: Message::Cq("hello".into()),
        auto_escape: Some(true),
    };
    assert_eq!(
        client.call(&request).await.unwrap(),
        Outcome::Completed(SentMessage { message_id: -17 })
    );

    let client = Client::new(Fixture(br#"{"status":"async","retcode":1,"data":null}"#));
    assert_eq!(client.call(&request).await.unwrap(), Outcome::Submitted);

    let client = Client::new(Fixture(
        br#"{"status":"failed","retcode":1400,"data":{"detail":"denied"},"wording":"no access"}"#,
    ));
    let Err(CallError::Api(failure)) = client.call(&request).await else {
        panic!("expected remote failure")
    };
    assert_eq!(failure.retcode, 1400);
    assert_eq!(failure.wording.as_deref(), Some("no access"));
    assert_eq!(failure.data, json!({"detail":"denied"}));

    let client = Client::new(Fixture(br#"{"status":"ok","retcode":0,"data":null}"#));
    assert_eq!(
        client.call(&DeleteMsg { message_id: 17 }).await.unwrap(),
        Outcome::Completed(())
    );
    assert!(matches!(
        client.call(&GetLoginInfo {}).await,
        Err(CallError::Decode(_))
    ));
}

#[tokio::test]
async fn invalid_protocol_frames_are_not_coerced_into_success() {
    let client = Client::new(Fixture(br#"{"status":"ok","retcode":1,"data":null}"#));
    assert!(matches!(
        client.call(&CleanCache {}).await,
        Err(CallError::Protocol(ProtocolError::InconsistentStatus {
            status: Status::Ok,
            retcode: 1
        }))
    ));
    for response in [
        br#"{"status":"ok","retcode":"0","data":null}"#.as_slice(),
        br#"{"retcode":0,"data":null}"#,
        br#"{"status":"unknown","retcode":0,"data":null}"#,
        b"not-json",
    ] {
        assert!(matches!(
            Client::new(Fixture(response)).call(&CleanCache {}).await,
            Err(CallError::Decode(_))
        ));
    }
}

#[tokio::test]
async fn async_io_and_onebot_action_modes_are_independent() {
    struct CheckMode;
    impl Transport for CheckMode {
        type Error = Infallible;
        async fn request(&self, action: &str, params: Vec<u8>) -> Result<Vec<u8>, Self::Error> {
            assert_eq!(action, "send_like_rate_limited");
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&params).unwrap(),
                json!({"user_id":34})
            );
            Ok(br#"{"status":"async","retcode":1,"data":null}"#.to_vec())
        }
    }
    assert_eq!(
        Client::new(CheckMode)
            .call_with_mode(
                &SendLike {
                    user_id: 34,
                    times: None
                },
                CallMode::RateLimited
            )
            .await
            .unwrap(),
        Outcome::Submitted
    );
}

#[test]
fn response_shapes_follow_top_level_message_type_and_nullable_online() {
    let message: MessageInfo = serde_json::from_value(json!({
        "time":1,"message_id":2,"real_id":3,"message_type":"group",
        "sender":{"user_id":34,"role":"admin"},"message":[{"type":"text","data":{"text":"hello"}}]
    }))
    .unwrap();
    assert!(
        matches!(message.source, MessageSource::Group {sender} if sender.user.user_id == 34 && sender.role == Some(Role::Admin))
    );
    let member: MemberInfo = serde_json::from_value(json!({
        "group_id":12,"user_id":34,"nickname":"example","card":"","sex":"unknown","age":0,
        "join_time":1,"last_sent_time":2,"level":"1","role":"member","unfriendly":false
    }))
    .unwrap();
    assert_eq!(member.area, None);
    assert_eq!(member.title, None);
    let status: ImplementationStatus =
        serde_json::from_value(json!({"online":null,"good":false,"extra":17})).unwrap();
    assert_eq!(status.online, None);
    assert_eq!(status.extensions.get("extra"), Some(&json!(17)));
}

#[test]
fn message_representations_preserve_cq_and_typed_media_without_io() {
    for wire in [
        json!("[CQ:at,qq=all]&amp;"),
        json!({"type":"image","data":{"file":"https://example.invalid/a.png","cache":"0"}}),
        json!([
            {"type":"reply","data":{"id":"-17"}},
            {"type":"face","data":{"id":"1"}},
            {"type":"rps","data":{}},
            {"type":"dice","data":{}},
            {"type":"shake","data":{}},
            {"type":"record","data":{"file":"record.silk","magic":"1"}},
            {"type":"video","data":{"file":"video.mp4"}},
            {"type":"poke","data":{"type":"1","id":"2"}},
            {"type":"anonymous","data":{"ignore":"1"}},
            {"type":"share","data":{"url":"https://example.invalid","title":"example"}},
            {"type":"contact","data":{"type":"qq","id":"34"}},
            {"type":"location","data":{"lat":"1.0","lon":"2.0"}},
            {"type":"music","data":{"type":"qq","id":"1"}},
            {"type":"music","data":{"type":"custom","url":"u","audio":"a","title":"t"}},
            {"type":"forward","data":{"id":"f"}},
            {"type":"node","data":{"id":"1"}},
            {"type":"node","data":{"user_id":"34","nickname":"example","content":[{"type":"text","data":{"text":"hello"}}]}},
            {"type":"xml","data":{"data":"<example/>"}},
            {"type":"json","data":{"data":"{}"}}
        ]),
    ] {
        let message: Message = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(message).unwrap(), wire);
    }
    assert!(serde_json::from_value::<Message>(json!({"type":"unknown","data":{}})).is_err());
    assert!(serde_json::from_value::<Message>(json!({"type":"image","data":{}})).is_err());
}
