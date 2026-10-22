//! Memory owns its database, graph and index. The host only executes resource operations.
pub mod content;
mod embedding;
mod graph;
mod index;
mod media;
pub mod openai;
mod projection;
mod store;
pub mod types;
use content::*;
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
};
use store::*;
use types::*;

pub struct Memory {
    object: Token,
    settings: Settings,
    retrieval: graph::Retrieval,
    wake_pending: Cell<bool>,
    index_ready: futures::lock::Mutex<bool>,
    media_locks: RefCell<BTreeMap<String, Rc<futures::lock::Mutex<()>>>>,
}
impl Module for Memory {
    async fn create(context: CreateContext) -> Result<Self, String> {
        let settings: Settings =
            host::config_get(context.object.clone(), "/settings".parse().unwrap())
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
        if settings.collection.is_empty()
            || !settings
                .collection
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        {
            return Err("invalid collection name".into());
        }
        let retrieval = graph::Retrieval::parse(&settings.retrieval_json)?;
        store::initialize().await.map_err(|e| format!("{e:?}"))?;
        Ok(Self {
            object: context.object,
            settings,
            retrieval,
            wake_pending: Cell::new(false),
            index_ready: futures::lock::Mutex::new(false),
            media_locks: RefCell::new(BTreeMap::new()),
        })
    }
    fn handlers() -> Handlers<Self> {
        let read = callers(vec![
            SubjectPattern::Path("/module/**".parse().unwrap()),
            SubjectPattern::Path("/user/admin".parse().unwrap()),
        ]);
        let agent = callers(vec![
            SubjectPattern::Path("/module/agent".parse().unwrap()),
            SubjectPattern::Path("/user/admin".parse().unwrap()),
        ]);
        let admin = callers(vec![SubjectPattern::Path("/user/admin".parse().unwrap())]);
        Handlers::new()
            .with(
                "/accept".parse().unwrap(),
                false,
                agent.clone(),
                Self::accept,
            )
            .with(
                "/archive".parse().unwrap(),
                false,
                agent.clone(),
                Self::archive,
            )
            .with("/link".parse().unwrap(), false, agent.clone(), Self::link)
            .with("/delivery".parse().unwrap(), false, agent, Self::delivery)
            .with("/read".parse().unwrap(), true, read.clone(), Self::read)
            .with(
                "/project".parse().unwrap(),
                true,
                read.clone(),
                Self::project,
            )
            .with(
                "/media/read".parse().unwrap(),
                true,
                read.clone(),
                Self::read_media,
            )
            .with("/recall".parse().unwrap(), true, read, Self::recall)
            .with(
                "/index/advance".parse().unwrap(),
                false,
                callers(vec![
                    SubjectPattern::SelfModule,
                    SubjectPattern::Path("/module/agent".parse().unwrap()),
                    SubjectPattern::Path("/user/admin".parse().unwrap()),
                ]),
                Self::advance,
            )
            .with(
                "/index/rebuild".parse().unwrap(),
                false,
                admin.clone(),
                Self::rebuild,
            )
            .with("/status".parse().unwrap(), true, admin, Self::status)
    }
}
impl Memory {
    async fn read_media(
        self: Rc<Self>,
        _: CallContext,
        reference: String,
    ) -> Result<String, Error> {
        self.image(&reference).await
    }
    async fn accept(self: Rc<Self>, _: CallContext, input: Incoming) -> Result<Accepted, Error> {
        let accepted = store::accept(input).await?;
        // Also resumes durable index work left by a previous instance.
        if let Err(error) = self.wake().await {
            let _ = host::log(
                forest_protocol::service::LogLevel::Warn,
                format!("index wake: {error:?}"),
            )
            .await;
        }
        Ok(accepted)
    }
    async fn archive(self: Rc<Self>, _: CallContext, ids: Vec<MemoryId>) -> Result<(), Error> {
        store::archive(&ids, &self.settings.collection).await?;
        if let Err(error) = self.wake().await {
            let _ = host::log(
                forest_protocol::service::LogLevel::Warn,
                format!("index wake: {error:?}"),
            )
            .await;
        }
        Ok(())
    }
    async fn read(
        self: Rc<Self>,
        _: CallContext,
        ids: Vec<MemoryId>,
    ) -> Result<Vec<MemoryUnit>, Error> {
        store::read(&ids, true).await
    }
    async fn link(self: Rc<Self>, _: CallContext, links: Vec<RecallLink>) -> Result<(), Error> {
        store::link(links).await
    }
    async fn delivery(self: Rc<Self>, _: CallContext, input: Delivery) -> Result<(), Error> {
        execute(vec![stmt(
            "INSERT INTO deliveries VALUES (?,?,?,?) ON CONFLICT DO NOTHING",
            vec![
                int(input.self_id),
                int(input.turn.group),
                int(input.message_id as i64),
                int(input.turn.sequence),
            ],
        )])
        .await?;
        Ok(())
    }
    async fn recall(self: Rc<Self>, _: CallContext, id: MemoryId) -> Result<Region, Error> {
        let mut units = store::read(&[id], false).await?;
        let unit = units.pop().ok_or(Error::NotFound)?;
        let seeds = self.seeds(&unit).await?;
        self.region(seeds).await
    }
    async fn rebuild(self: Rc<Self>, _: CallContext, _: ()) -> Result<(), Error> {
        // One page at a time; no complete history is materialized in Wasm.
        let mut cursor = 0;
        loop {
            let rows=execute(vec![stmt("SELECT rowid,group_id,sequence FROM units WHERE archived=1 AND rowid>? ORDER BY rowid LIMIT 64",vec![int(cursor)])]).await?;
            if rows[0].rows.is_empty() {
                break;
            }
            let mut ids = Vec::new();
            for row in &rows[0].rows {
                cursor = number(&row[0])?;
                ids.push(MemoryId {
                    group: number(&row[1])?,
                    sequence: number(&row[2])?,
                });
            }
            store::archive(&ids, &self.settings.collection).await?;
        }
        execute(vec![stmt(
            "UPDATE vectors SET state='pending',error=NULL WHERE space=?",
            vec![text(&self.settings.collection)],
        )])
        .await?;
        *self.index_ready.lock().await = false;
        self.wake().await
    }
    async fn status(self: Rc<Self>, _: CallContext, _: ()) -> Result<String, Error> {
        let rows = execute(vec![
            stmt(
                "SELECT count(*),coalesce(sum(archived),0) FROM units",
                vec![],
            ),
            stmt(
                "SELECT state,count(*) FROM vectors WHERE space=? GROUP BY state",
                vec![text(&self.settings.collection)],
            ),
        ])
        .await?;
        let index = rows[1]
            .rows
            .iter()
            .map(|row| Ok((string(&row[0])?.to_owned(), number(&row[1])?)))
            .collect::<Result<BTreeMap<_, _>, Error>>()?;
        Ok(serde_json::json!({"units":number(&rows[0].rows[0][0])?,"archived":number(&rows[0].rows[0][1])?,"index":index}).to_string())
    }
}
forest_sdk::export_module!(super::Memory);
pub fn manifest() -> Manifest {
    Manifest {
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Memory".into()),
            description: Some(
                "Raw multi-trunk memory, multimodal indexing and bounded associative recall".into(),
            ),
        },
        module: "module.wasm".into(),
        configs: vec![ConfigDeclaration {
            key: "/settings".parse().unwrap(),
            schema: Settings::as_schema(),
            default: Settings::default().as_value(),
            access: AccessPolicy {
                readers: vec![SubjectPattern::SelfModule],
                writers: vec![SubjectPattern::Path("/user/admin".parse().unwrap())],
                callers: vec![],
            },
        }],
        provides: Memory::handlers().declarations(),
        uses: vec![
            InterfaceUse {
                object: embedding::OBJECT.parse().unwrap(),
                interface: embedding::EMBED.parse().unwrap(),
                input: Vec::<embedding::Input>::as_schema(),
                output: Result::<embedding::Embeddings, embedding::Error>::as_schema(),
            },
            InterfaceUse {
                object: OBJECT.parse().unwrap(),
                interface: "/index/advance".parse().unwrap(),
                input: <()>::as_schema(),
                output: Result::<(), Error>::as_schema(),
            },
        ],
        subscriptions: vec![],
    }
}
