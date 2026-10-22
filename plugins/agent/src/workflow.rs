use crate::{
    Agent,
    content::*,
    context::{self, ContextItem},
    memory, provider,
    state::*,
    tools,
    types::Error,
};
use forest_protocol::{AsValue, service::LogLevel};
use forest_sdk::host;

async fn memory_call<I: AsValue, O: AsValue>(interface: &str, input: &I) -> Result<O, Error> {
    let output: Result<O, memory::Error> = host::call(
        memory::OBJECT.parse().unwrap(),
        interface.parse().unwrap(),
        input,
    )
    .await?;
    output.map_err(Error::Memory)
}
impl Agent {
    pub(crate) async fn note(&self, session: &Shared, message: String) {
        session.borrow_mut().diagnostic(message.clone());
        let _ = host::log(LogLevel::Warn, message).await;
    }
    pub(crate) async fn drive(&self, session: &Shared) -> Result<(), Error> {
        loop {
            self.ingest(session).await?;
            if session.borrow().has_trigger()
                && !session
                    .borrow()
                    .data
                    .pending
                    .iter()
                    .any(|p| p.trigger && p.accepted.is_some())
            {
                continue;
            }
            if session.borrow().has_trigger() {
                if !session.borrow().working {
                    self.maintain(session).await?;
                }
                session.borrow_mut().working = true;
                if let Err(error) = self.work(session).await {
                    let message = format!("{error:?}");
                    self.note(session, message.clone()).await;
                    let mut s = session.borrow_mut();
                    // Selected input remains owned by the session even if preparation failed.
                    let staged = std::mem::take(&mut s.data.staged);
                    for p in staged {
                        s.data.window.push(Block::Message {
                            unit: p.accepted.unwrap(),
                            recalls: vec![],
                        });
                    }
                    s.error = Some(message);
                }
                if session.borrow().has_trigger() {
                    continue;
                }
            }
            let report = {
                let mut s = session.borrow_mut();
                s.working = false;
                s.drain_ordinary();
                s.error.take()
            };
            if let Some(error) = report {
                self.report(session, &error).await;
            }
            self.maintain(session).await?;
            if session.borrow().data.pending.is_empty() {
                return Ok(());
            }
        }
    }
    async fn ingest(&self, session: &Shared) -> Result<(), Error> {
        // A bounded snapshot prevents a busy group from extending this phase forever.
        let count = session
            .borrow()
            .data
            .pending
            .iter()
            .filter(|p| p.accepted.is_none())
            .count();
        for _ in 0..count {
            let (index, message) = {
                let s = session.borrow();
                let index = s
                    .data
                    .pending
                    .iter()
                    .position(|p| p.accepted.is_none())
                    .unwrap();
                (index, s.data.pending[index].message.clone())
            };
            let accepted: memory::Accepted =
                memory_call("/accept", &memory::Incoming::Group(Box::new(message))).await?;
            let mut s = session.borrow_mut();
            if !accepted.fresh && !s.data.pending[index].restored {
                s.data.pending.remove(index);
            } else {
                s.data.pending[index].accepted = Some(accepted.unit);
            }
        }
        Ok(())
    }
    async fn maintain(&self, session: &Shared) -> Result<(), Error> {
        let prefix = session.borrow().clip_prefix(
            self.settings.high_water as usize,
            self.settings.low_water as usize,
        );
        if prefix == 0 {
            return Ok(());
        }
        let ids = session.borrow().data.window[..prefix]
            .iter()
            .flat_map(Block::units)
            .map(|u| u.id.clone())
            .collect::<Vec<_>>();
        memory_call::<_, ()>("/archive", &ids).await?;
        session.borrow_mut().data.window.drain(..prefix);
        Ok(())
    }
    async fn stage(&self, session: &Shared) -> Result<Vec<memory::RecallLink>, Error> {
        while session.borrow().has_trigger()
            && !session
                .borrow()
                .data
                .pending
                .iter()
                .any(|p| p.trigger && p.accepted.is_some())
        {
            self.ingest(session).await?;
        }
        if !session.borrow_mut().stage() {
            return Ok(vec![]);
        }
        let mut links = Vec::new();
        loop {
            let Some(p) = session.borrow().data.staged.first().cloned() else {
                break;
            };
            let unit = p.accepted.unwrap();
            let recalls = if p.trigger {
                match memory_call::<_, memory::Region>("/recall", &unit.id).await {
                    Ok(region) => {
                        links.extend(Session::links_for(&unit.id, region.seeds));
                        region.units.into_iter().map(|u| u.id).collect()
                    }
                    // Unavailable RAG is observable; it doesn't erase the actual conversation.
                    Err(error) => {
                        self.note(session, format!("recall {:?}: {error:?}", unit.id))
                            .await;
                        vec![]
                    }
                }
            } else {
                vec![]
            };
            let mut s = session.borrow_mut();
            s.data.window.push(Block::Message { unit, recalls });
            s.data.staged.remove(0);
        }
        Ok(links)
    }
    async fn format(
        &self,
        session: &Shared,
        blocks: &[Block],
        multimodal: bool,
    ) -> Result<Vec<provider::Message>, Error> {
        let mut order = Vec::new();
        for block in blocks {
            match block {
                Block::Message { unit, recalls } => {
                    order.extend(recalls.iter().map(|id| (true, id.clone())));
                    order.push((false, unit.id.clone()));
                }
                Block::Step {
                    units,
                    closed: true,
                } => order.extend(units.iter().map(|unit| (false, unit.id.clone()))),
                Block::Step { closed: false, .. } => {} // Kept as raw facts after interrupted reload.
            }
        }
        let projection: memory::Projection = memory_call(
            "/project",
            &memory::Project {
                ids: order.iter().map(|(_, id)| id.clone()).collect(),
                images: multimodal,
            },
        )
        .await?;
        let items = order
            .into_iter()
            .zip(projection.units)
            .map(|((recall, _), unit)| {
                if recall {
                    ContextItem::Recall(unit)
                } else {
                    ContextItem::Content(unit.content)
                }
            })
            .collect();
        let identity = {
            let s = session.borrow();
            context::PromptIdentity {
                bot_id: s.data.bot,
                bot_name: self.settings.bot_name.clone(),
                group_id: s.data.group,
                group_name: None,
            }
        };
        let output: Result<Vec<provider::Message>, context::Error> = host::call(
            context::OBJECT.parse().unwrap(),
            context::FORMAT.parse().unwrap(),
            &context::FormatInput {
                identity,
                items,
                multimodal,
                images: projection
                    .images
                    .into_iter()
                    .map(|i| context::ImageInput {
                        reference: i.reference,
                        image: i.image,
                    })
                    .collect(),
            },
        )
        .await?;
        output.map_err(Error::Context)
    }
    async fn work(&self, session: &Shared) -> Result<(), Error> {
        let old_blocks = session.borrow().data.window.len();
        let anchor = session
            .borrow()
            .data
            .pending
            .iter()
            .rev()
            .find(|p| p.trigger && p.accepted.is_some())
            .unwrap()
            .accepted
            .as_ref()
            .unwrap()
            .id
            .clone();
        let mut links = self.stage(session).await?;
        // The first freeze may remove an old prefix, but never the selected trigger batch.
        let prefix = session
            .borrow()
            .clip_prefix(
                self.settings.high_water as usize,
                self.settings.low_water as usize,
            )
            .min(old_blocks);
        if prefix > 0 {
            let ids = session.borrow().data.window[..prefix]
                .iter()
                .flat_map(Block::units)
                .map(|u| u.id.clone())
                .collect::<Vec<_>>();
            memory_call::<_, ()>("/archive", &ids).await?;
            session.borrow_mut().data.window.drain(..prefix);
        }
        let capabilities: Result<provider::Capabilities, provider::Error> = host::call(
            provider::OBJECT.parse().unwrap(),
            provider::CAPABILITIES.parse().unwrap(),
            &(),
        )
        .await?;
        let multimodal = capabilities.map_err(Error::Provider)?.multimodal;
        let tools: Result<tools::Catalog, tools::Error> = host::call(
            tools::OBJECT.parse().unwrap(),
            "/describe".parse().unwrap(),
            &(),
        )
        .await?;
        let tools = tools.map_err(|e| Error::Protocol(format!("{e:?}")))?;
        let mut request = provider::Request {
            messages: vec![],
            tools,
            tool_choice: None,
        };
        let mut formatted = 0;
        let mut turn = 0;
        loop {
            let (blocks, end) = {
                let s = session.borrow();
                (s.data.window[formatted..].to_vec(), s.data.window.len())
            };
            let messages = self.format(session, &blocks, multimodal).await?;
            request.messages.extend(
                messages
                    .into_iter()
                    .skip(usize::from(!request.messages.is_empty())),
            );
            formatted = end;
            turn += 1;
            let result: Result<Result<provider::Completion, provider::Error>, _> = host::call(
                provider::OBJECT.parse().unwrap(),
                provider::COMPLETE.parse().unwrap(),
                &request,
            )
            .await;
            let result = result
                .map_err(Error::from)
                .and_then(|r| r.map_err(Error::Provider));
            match result {
                Err(error) => {
                    let message = format!("{error:?}");
                    self.note(session, message.clone()).await;
                    session.borrow_mut().error = Some(message);
                    links.clear();
                    self.ingest(session).await?;
                    if !session.borrow().has_trigger() {
                        return Ok(());
                    }
                    links = self.stage(session).await?;
                    continue;
                }
                Ok(completion) => {
                    let choice = completion
                        .choices
                        .into_iter()
                        .find(|c| c.index == 0)
                        .ok_or_else(|| {
                            Error::Protocol("provider returned no primary choice".into())
                        })?;
                    // Received messages precede the generated turn in the reception trunk,
                    // while remaining outside the active model input until its next boundary.
                    self.ingest(session).await?;
                    let assistant = choice.message;
                    let calls = assistant.tool_calls.clone().unwrap_or_default();
                    let accepted: memory::Accepted = memory_call(
                        "/accept",
                        &memory::Incoming::Generated {
                            group: anchor.group,
                            key: format!("run:{}/turn:{turn}", anchor.sequence),
                            content: MemoryContent::ModelTurn(ModelTurn {
                                time: completion.created,
                                assistant: assistant.clone(),
                            }),
                        },
                    )
                    .await?;
                    let turn_id = accepted.unit.id.clone();
                    session.borrow_mut().data.window.push(Block::Step {
                        units: vec![accepted.unit],
                        closed: calls.is_empty(),
                    });
                    if !links.is_empty() {
                        if let Err(error) = memory_call::<_, ()>("/link", &links).await {
                            self.note(session, format!("recall links: {error:?}")).await;
                        }
                        links.clear();
                    }
                    let visible = assistant
                        .content
                        .as_deref()
                        .filter(|s| !s.trim().is_empty())
                        .or_else(|| {
                            assistant
                                .refusal
                                .as_deref()
                                .filter(|s| !s.trim().is_empty())
                        });
                    if let Some(body) = visible
                        && let Err(error) = self.deliver(session, &turn_id, body).await
                    {
                        self.note(session, format!("{error:?}")).await;
                    }
                    let continuing = !calls.is_empty();
                    for (ordinal, call) in calls.into_iter().enumerate() {
                        let provider::ToolCall::Function { id, function } = call;
                        let bot = session.borrow().data.bot;
                        let output: Result<Result<String, tools::Error>, _> = host::call(
                            tools::OBJECT.parse().unwrap(),
                            "/invoke".parse().unwrap(),
                            &tools::Invocation {
                                group_id: anchor.group,
                                bot_id: bot,
                                name: function.name,
                                arguments_json: function.arguments,
                            },
                        )
                        .await;
                        let content = match output {
                            Ok(Ok(body)) => body,
                            Ok(Err(error)) => {
                                serde_json::json!({"error":format!("{error:?}")}).to_string()
                            }
                            Err(error) => {
                                serde_json::json!({"dispatch_error":error.to_string()}).to_string()
                            }
                        };
                        self.ingest(session).await?;
                        let time = host::now().await? / 1000;
                        let accepted: memory::Accepted = memory_call(
                            "/accept",
                            &memory::Incoming::Generated {
                                group: anchor.group,
                                key: format!("run:{}/turn:{turn}/tool:{ordinal}", anchor.sequence),
                                content: MemoryContent::ToolResult(ToolResult {
                                    time,
                                    tool_call_id: id,
                                    content,
                                }),
                            },
                        )
                        .await?;
                        let mut s = session.borrow_mut();
                        let Block::Step { units, .. } = s.data.window.last_mut().unwrap() else {
                            unreachable!("work loop owns the window")
                        };
                        units.push(accepted.unit);
                    }
                    if continuing {
                        let mut s = session.borrow_mut();
                        let Block::Step { closed, .. } = s.data.window.last_mut().unwrap() else {
                            unreachable!()
                        };
                        *closed = true;
                    }
                    self.ingest(session).await?;
                    if !continuing && !session.borrow().has_trigger() {
                        return Ok(());
                    }
                    links = self.stage(session).await?;
                }
            }
        }
    }
}
