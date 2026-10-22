use crate::{Agent, content::MemoryId, memory, state::Shared, types::Error};
use forest_onebot::{
    cq,
    forest::{self as onebot, OneBotError, SendGroupMessage, SentMessage},
    message::{Message, Segment},
};
use forest_sdk::host;
impl Agent {
    pub(crate) async fn deliver(
        &self,
        session: &Shared,
        turn: &MemoryId,
        body: &str,
    ) -> Result<(), Error> {
        let segments = cq::decode(body).map_err(|e| Error::Delivery(e.to_string()))?;
        // Images are resolved by Memory; the model cannot name a host path or arbitrary URL.
        for segment in &segments {
            if !matches!(
                segment,
                Segment::Text { .. }
                    | Segment::At { .. }
                    | Segment::Reply { .. }
                    | Segment::Face { .. }
                    | Segment::Image(_)
            ) {
                return Err(Error::Delivery("unsupported CQ output segment".into()));
            }
        }
        let mut segments = segments;
        for segment in &mut segments {
            if let Segment::Image(image) = segment {
                let data: Result<String, memory::Error> = host::call(
                    memory::OBJECT.parse().unwrap(),
                    "/media/read".parse().unwrap(),
                    &image.file,
                )
                .await?;
                let data = data.map_err(Error::Memory)?;
                let (_, bytes) = data
                    .split_once(";base64,")
                    .ok_or_else(|| Error::Delivery("invalid managed image".into()))?;
                image.file = format!("base64://{bytes}");
                image.options = Default::default();
            }
        }
        let (group, bot) = {
            let s = session.borrow();
            (s.data.group, s.data.bot)
        };
        let sent: Result<SentMessage, OneBotError> = host::call(
            onebot::OBJECT.parse().unwrap(),
            onebot::SEND_GROUP_MESSAGE.parse().unwrap(),
            &SendGroupMessage {
                group_id: group,
                message: Message::Segments(segments),
                auto_escape: Some(false),
            },
        )
        .await?;
        let sent = sent.map_err(|e| Error::Delivery(format!("{e:?}")))?;
        let result: Result<(), memory::Error> = host::call(
            memory::OBJECT.parse().unwrap(),
            "/delivery".parse().unwrap(),
            &memory::Delivery {
                turn: turn.clone(),
                self_id: bot,
                message_id: sent.message_id,
            },
        )
        .await?;
        result.map_err(Error::Memory)
    }
    pub(crate) async fn report(&self, session: &Shared, error: &str) {
        let group = session.borrow().data.group;
        let result = host::send(
            onebot::OBJECT.parse().unwrap(),
            onebot::SEND_GROUP_MESSAGE.parse().unwrap(),
            &SendGroupMessage {
                group_id: group,
                message: Message::Segments(vec![Segment::text(format!("Agent 调用失败：{error}"))]),
                auto_escape: Some(true),
            },
        )
        .await;
        if let Err(error) = result {
            self.note(session, format!("error report dispatch: {error}"))
                .await;
        }
    }
}
