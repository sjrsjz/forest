//! Host administration adapter. The scheduler contains no shell commands or Bot policy.
pub mod contract;
pub mod packages;

use contract::*;
use forest_protocol::{
    manifest::callers,
    service::{ConfigKey, ConfigSet, LogLevel},
    *,
};
use forest_runtime::{
    ConfigWriter, Inspector, LifecycleSnapshot, NativeInterface, Scheduler, StopMode,
};
use packages::{LocalPackage as CatalogPackage, Packages, ReloadRequest};
use std::{future::Future, sync::Arc};

#[derive(Clone)]
struct Control {
    inspector: Inspector,
    packages: Packages,
    writer: ConfigWriter,
    logs: forest_runtime::LogWriter,
}

/// The host explicitly grants its administrator handle to the selected frontend.
/// Callers cannot supply or replace the authenticated ConfigWriter subject.
pub fn install(
    scheduler: &Scheduler,
    packages: Packages,
    administrator: ConfigWriter,
    permitted_callers: Vec<SubjectPattern>,
) -> anyhow::Result<()> {
    let control = Arc::new(Control {
        inspector: scheduler.inspector(),
        packages,
        writer: administrator,
        logs: scheduler.logs(),
    });
    register(
        scheduler,
        &permitted_callers,
        "/snapshot",
        control.clone(),
        |c, _, _: ()| async move { c.snapshot() },
    )?;
    register(
        scheduler,
        &permitted_callers,
        "/packages",
        control.clone(),
        |c, _, _: ()| async move { c.packages().await },
    )?;
    register(
        scheduler,
        &permitted_callers,
        "/load",
        control.clone(),
        |c, context, object: String| async move {
            let packages = c.packages.clone();
            let target = object.clone();
            c.manage(context, format!("load {object}"), async move {
                let directory = packages.directory_for(&target.parse()?).await?;
                packages.load(&directory).await
            })
            .await
        },
    )?;
    register(
        scheduler,
        &permitted_callers,
        "/reload",
        control.clone(),
        |c, context, request: Stop| async move {
            let object: Token = request
                .object
                .parse()
                .map_err(|e| Error::Management(format!("{e}")))?;
            if object == context.caller {
                return Err(Error::CallingModule);
            }
            let packages = c.packages.clone();
            c.manage(
                context,
                format!("reload {} {:?}", request.object, request.mode),
                async move {
                    let directory = packages.directory_for(&object).await?;
                    packages
                        .reload(ReloadRequest {
                            directory,
                            object,
                            mode: mode(request.mode),
                        })
                        .await
                },
            )
            .await
        },
    )?;
    register(
        scheduler,
        &permitted_callers,
        "/unload",
        control.clone(),
        |c, context, request: Stop| async move {
            let object: Token = request
                .object
                .parse()
                .map_err(|e| Error::Management(format!("{e}")))?;
            if object == context.caller {
                return Err(Error::CallingModule);
            }
            let packages = c.packages.clone();
            c.manage(
                context,
                format!("unload {} {:?}", request.object, request.mode),
                async move { packages.unload(&object, mode(request.mode)).await },
            )
            .await
        },
    )?;
    register(
        scheduler,
        &permitted_callers,
        "/force",
        control.clone(),
        |c, context, object: String| async move {
            let object: Token = object
                .parse()
                .map_err(|e| Error::Management(format!("{e}")))?;
            if object == context.caller {
                return Err(Error::CallingModule);
            }
            let result = c
                .packages
                .force(&object)
                .map_err(|e| Error::Management(e.to_string()));
            c.audit(&context, &format!("force {object}"), &result);
            result
        },
    )?;
    register(
        scheduler,
        &permitted_callers,
        "/config/keys",
        control.clone(),
        |c, _, object: String| async move {
            c.inspector
                .config_keys(
                    object
                        .parse()
                        .map_err(|e| Error::Configuration(format!("{e}")))?,
                )
                .await
                .map(|keys| {
                    keys.into_iter()
                        .map(|k| k.key.to_string())
                        .collect::<Vec<String>>()
                })
                .map_err(|e| Error::Inspection(e.to_string()))
        },
    )?;
    register(
        scheduler,
        &permitted_callers,
        "/config/get",
        control.clone(),
        |c, _, target: ConfigAddress| async move {
            let entry = c
                .inspector
                .config_entry(
                    target
                        .object
                        .parse()
                        .map_err(|e| Error::Configuration(format!("{e}")))?,
                    target
                        .key
                        .parse()
                        .map_err(|e| Error::Configuration(format!("{e}")))?,
                )
                .await
                .map_err(|e| Error::Inspection(e.to_string()))?;
            Ok(ConfigEntry {
                schema: entry.schema().to_string(),
                default_value: entry.default_value().to_string(),
                value: entry.value().to_string(),
            })
        },
    )?;
    register(
        scheduler,
        &permitted_callers,
        "/config/set",
        control,
        |c, context, request: ConfigValue| async move {
            let value = forest_serde::parser::parse_value(&request.value)
                .map_err(|e| Error::Configuration(e.to_string()))?;
            let operation = format!(
                "config set {} {}",
                request.target.object, request.target.key
            );
            let result = c
                .writer
                .set(ConfigSet {
                    target: ConfigKey {
                        object: request
                            .target
                            .object
                            .parse()
                            .map_err(|e| Error::Configuration(format!("{e}")))?,
                        key: request
                            .target
                            .key
                            .parse()
                            .map_err(|e| Error::Configuration(format!("{e}")))?,
                    },
                    value,
                })
                .await
                .map_err(|e| Error::Configuration(e.to_string()));
            c.audit(&context, &operation, &result);
            result
        },
    )?;
    Ok(())
}

fn register<I, T, F, Fut>(
    scheduler: &Scheduler,
    allowed: &[SubjectPattern],
    path: &str,
    control: Arc<Control>,
    handler: F,
) -> anyhow::Result<()>
where
    I: AsSchema + AsValue,
    T: AsSchema + AsValue,
    F: Fn(Arc<Control>, CallContext, I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T, Error>> + Send + 'static,
{
    scheduler.register_native(NativeInterface::new(
        OBJECT.parse()?,
        path.parse()?,
        true,
        callers(allowed.to_vec()),
        move |context, input| handler(control.clone(), context, input),
    ))
}

fn mode(mode: Mode) -> StopMode {
    match mode {
        Mode::Drain => StopMode::Drain,
        Mode::AtYield => StopMode::AtYield,
        Mode::Force => StopMode::Force,
    }
}
impl Control {
    fn snapshot(&self) -> Result<Snapshot, Error> {
        let snapshot = self
            .inspector
            .snapshot()
            .map_err(|e| Error::Inspection(e.to_string()))?;
        Ok(Snapshot {
            parallelism: snapshot.parallelism as i64,
            modules: snapshot
                .modules
                .into_iter()
                .map(|m| ModuleView {
                    object: m.object.to_string(),
                    version: m.published.version,
                    name: m.published.name,
                    phase: match m.lifecycle {
                        LifecycleSnapshot::Creating { .. } => Phase::Creating,
                        LifecycleSnapshot::Serving { .. } => Phase::Serving,
                        LifecycleSnapshot::Stopping { .. } => Phase::Stopping,
                        LifecycleSnapshot::Destroying { .. } => Phase::Destroying,
                        LifecycleSnapshot::Releasing { .. } => Phase::Releasing,
                        LifecycleSnapshot::Failed { reason } => Phase::Failed(reason),
                    },
                    queued: (m.queued_next + m.interfaces.iter().map(|i| i.queued).sum::<usize>())
                        as i64,
                    dependencies: m
                        .uses
                        .into_iter()
                        .map(|u| Endpoint {
                            object: u.object.to_string(),
                            interface: u.interface.to_string(),
                        })
                        .collect(),
                })
                .collect(),
        })
    }
    async fn packages(&self) -> Result<Vec<LocalPackage>, Error> {
        Ok(self
            .packages
            .list()
            .await
            .map_err(|e| Error::Management(e.to_string()))?
            .into_iter()
            .map(|p| match p {
                CatalogPackage::Ready {
                    directory,
                    manifest,
                } => LocalPackage::Ready {
                    directory,
                    object: manifest.package.id.to_string(),
                    version: manifest.package.version,
                },
                CatalogPackage::Invalid { directory, message } => {
                    LocalPackage::Invalid { directory, message }
                }
            })
            .collect())
    }
    // A started lifecycle operation remains host-owned if its caller is interrupted.
    async fn manage(
        self: Arc<Self>,
        context: CallContext,
        operation: String,
        future: impl Future<Output = anyhow::Result<()>> + Send + 'static,
    ) -> Result<(), Error> {
        tokio::spawn(async move {
            let result = future
                .await
                .map_err(|e| Error::Management(format!("{e:#}")));
            self.audit(&context, &operation, &result);
            result
        })
        .await
        .map_err(|e| Error::Management(e.to_string()))?
    }
    fn audit(&self, context: &CallContext, operation: &str, result: &Result<(), Error>) {
        self.logs.write(
            if result.is_ok() {
                LogLevel::Info
            } else {
                LogLevel::Warn
            },
            OBJECT.parse().unwrap(),
            format!(
                "origin={} caller={} {operation}: {result:?}",
                context.origin, context.caller
            ),
        );
    }
}
