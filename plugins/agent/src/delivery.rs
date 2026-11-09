use crate::{Agent, content::MemoryId, memory, output, state::Shared, types::Error};
use forest_onebot::message::{Message, Segment};
use forest_sdk::host;

impl Agent {
    pub(crate) async fn deliver(
        &self,
        session: &Shared,
        turn: &MemoryId,
        body: &str,
    ) -> Result<(), Error> {
        let (group, bot) = {
            let s = session.borrow();
            (s.data.group, s.data.bot)
        };
        let report: Result<output::DeliveryReport, output::Error> = host::call(
            output::OBJECT.parse().unwrap(),
            output::SEND_MARKDOWN.parse().unwrap(),
            &output::MarkdownOutput {
                group_id: group,
                bot_id: bot,
                bot_name: self.settings.bot_name.clone(),
                source: body.into(),
            },
        )
        .await?;
        let report = report.map_err(|e| Error::Delivery(format!("{e:?}")))?;
        for sent in report.sent {
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
            result.map_err(Error::Memory)?;
        }
        report
            .outcome
            .map_err(|e| Error::Delivery(format!("{e:?}")))
    }

    pub(crate) async fn report(&self, session: &Shared, error: &str) {
        let (group, bot) = {
            let s = session.borrow();
            (s.data.group, s.data.bot)
        };
        let result = host::send(
            output::OBJECT.parse().unwrap(),
            output::SEND.parse().unwrap(),
            &output::GroupOutput {
                group_id: group,
                bot_id: bot,
                bot_name: self.settings.bot_name.clone(),
                message: Message::Segments(vec![Segment::text(format!("Agent 调用失败：{error}"))]),
            },
        )
        .await;
        if let Err(error) = result {
            self.note(session, format!("error report dispatch: {error}"))
                .await;
        }
    }
}
