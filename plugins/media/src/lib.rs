//! Persistent managed media, independent of producers and conversation memory.
pub mod contract;
mod fetch;
mod store;

use contract::{Blob, Error, OBJECT, Source};
use forest_protocol::{manifest::callers, *};
use forest_sdk::{Handlers, Module, host};
use std::{cell::Cell, rc::Rc};

pub struct Media {
    wake_pending: Cell<bool>,
}

impl Module for Media {
    type LifecycleError = forest_sdk::HostError;

    async fn create(_: CreateContext) -> Result<Self, Self::LifecycleError> {
        store::initialize().await?;
        Ok(Self {
            wake_pending: Cell::new(false),
        })
    }

    fn handlers() -> Handlers<Self> {
        let readers = callers(vec![
            SubjectPattern::Path("/module/**".parse().unwrap()),
            SubjectPattern::Path("/user/admin".parse().unwrap()),
        ]);
        let producers = callers(vec![
            SubjectPattern::Path("/module/typst".parse().unwrap()),
            SubjectPattern::Path("/module/ai-paint".parse().unwrap()),
            SubjectPattern::Path("/user/admin".parse().unwrap()),
        ]);
        let memory = callers(vec![
            SubjectPattern::Path("/module/memory".parse().unwrap()),
            SubjectPattern::Path("/user/admin".parse().unwrap()),
        ]);
        Handlers::new()
            .with("/put".parse().unwrap(), producers, Self::put)
            .with("/register".parse().unwrap(), memory, Self::register)
            .with("/get".parse().unwrap(), readers, Self::get)
            .with(
                "/acquire".parse().unwrap(),
                callers(vec![SubjectPattern::SelfModule]),
                Self::acquire,
            )
    }
}

impl Media {
    async fn put(self: Rc<Self>, _: CallContext, blob: Blob) -> Result<String, Error> {
        if blob.bytes.is_empty()
            || !blob.mime.starts_with("image/")
            || blob.mime.contains([';', '\r', '\n'])
        {
            return Err(Error::InvalidInput(
                "nonempty image bytes and image MIME type required".into(),
            ));
        }
        store::put(blob).await
    }

    async fn register(self: Rc<Self>, _: CallContext, source: Source) -> Result<(), Error> {
        validate_reference(&source.reference)?;
        store::register(source).await?;
        if !self.wake_pending.replace(true)
            && let Err(error) =
                host::send(OBJECT.parse().unwrap(), "/acquire".parse().unwrap(), &()).await
        {
            self.wake_pending.set(false);
            return Err(error.into());
        }
        Ok(())
    }

    async fn get(self: Rc<Self>, _: CallContext, reference: String) -> Result<Blob, Error> {
        self.read(reference).await
    }
    async fn read(&self, reference: String) -> Result<Blob, Error> {
        validate_reference(&reference)?;
        let key = format!("/blob/{}", &reference["media:".len()..]);
        let _guard = host::lock(key.parse().unwrap()).await?;
        match store::read(&reference).await? {
            store::Stored::Bytes(blob) => Ok(blob),
            store::Stored::Source(source) => {
                let blob = fetch::fetch(&source).await?;
                store::cache(reference, &blob).await?;
                Ok(blob)
            }
        }
    }
    async fn acquire(self: Rc<Self>, _: CallContext, _: ()) -> Result<(), Error> {
        let _guard = host::lock("/acquisition".parse().unwrap()).await?;
        self.wake_pending.set(false);
        while let Some(reference) = store::pending().await? {
            if let Err(error) = self.read(reference.clone()).await {
                store::failed(&reference, &format!("{error:?}")).await?;
                let _ = host::log(
                    service::LogLevel::Warn,
                    format!("media acquisition {reference}: {error:?}"),
                )
                .await;
            }
        }
        Ok(())
    }
}

fn validate_reference(reference: &str) -> Result<(), Error> {
    let Some(hash) = reference.strip_prefix("media:") else {
        return Err(Error::InvalidInput("expected media:<sha256>".into()));
    };
    if hash.len() != 64 || !hash.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::InvalidInput(
            "invalid managed media reference".into(),
        ));
    }
    Ok(())
}

impl From<DispatchError> for Error {
    fn from(e: DispatchError) -> Self {
        Self::Host(e.to_string())
    }
}
impl From<service::ServiceError> for Error {
    fn from(e: service::ServiceError) -> Self {
        Self::Storage(e.to_string())
    }
}

forest_sdk::export_module!(super::Media);

pub fn manifest() -> Manifest {
    Manifest {
        events: vec![],
        activated: None,
        package: PackageInfo {
            id: OBJECT.parse().unwrap(),
            version: "0.1.0".into(),
            name: Some("Managed media".into()),
            description: Some("Persistent image bytes and stable source references".into()),
            ..Default::default()
        },
        module: "module.wasm".into(),
        lifecycle_error: <Media as Module>::LifecycleError::as_schema(),
        configs: vec![],
        provides: Media::handlers().declarations(),
        uses: vec![InterfaceUse {
            object: OBJECT.parse().unwrap(),
            interface: "/acquire".parse().unwrap(),
            input: <()>::as_schema(),
            output: Result::<(), Error>::as_schema(),
        }],
        subscriptions: vec![],
    }
}
