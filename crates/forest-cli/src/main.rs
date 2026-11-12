use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use forest_protocol::{SubjectPattern, TokenPattern, service::LogLevel};
use forest_runtime::{RuntimeOptions, Scheduler, StopMode};
#[cfg(feature = "webui")]
use std::net::SocketAddr;
use std::path::PathBuf;
mod logs;

#[derive(Parser)]
#[command(name = "forest", about = "Forest generic Wasm scheduler")]
struct Arguments {
    #[cfg(feature = "webui")]
    #[arg(long)]
    webui: Option<SocketAddr>,
    #[cfg(feature = "webui")]
    #[arg(long, default_value = "FOREST_WEB_PASSWORD")]
    web_password_env: String,
    #[cfg(feature = "webui")]
    #[arg(long, default_value = "webui/dist")]
    webui_assets: PathBuf,
    #[arg(long, default_value = "packages")]
    packages_directory: PathBuf,
    #[arg(long, help = "Package roots to load from the catalog")]
    package: Vec<PathBuf>,
    #[arg(long, help = "Module allowed to use the host administration service")]
    controller: Vec<TokenPattern>,
    #[arg(long, default_value = "data/forest.sqlite3")]
    database: PathBuf,
    #[arg(long, default_value = "data/modules")]
    data_directory: PathBuf,
    #[arg(long, value_enum, default_value = "drain")]
    shutdown_mode: Mode,
}
#[derive(Clone, Copy, ValueEnum)]
enum Mode {
    Drain,
    Yield,
    Force,
}
impl From<Mode> for StopMode {
    fn from(value: Mode) -> Self {
        match value {
            Mode::Drain => Self::Drain,
            Mode::Yield => Self::AtYield,
            Mode::Force => Self::Force,
        }
    }
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    let args = Arguments::parse();
    if let Some(parent) = args.database.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let scheduler = Scheduler::new(
        &args.database,
        RuntimeOptions {
            data_directory: args.data_directory,
            ..Default::default()
        },
    )
    .await?;
    let _stderr = logs::Stderr::start(scheduler.logs().reader());
    tokio::fs::create_dir_all(&args.packages_directory).await?;
    let catalog = forest_control::packages::Packages::new(
        &args.packages_directory,
        scheduler.module_manager(),
    )
    .await?;
    forest_control::install(
        &scheduler,
        catalog,
        scheduler.config_writer("/user/admin".parse()?),
        args.controller
            .into_iter()
            .map(SubjectPattern::Path)
            .collect(),
    )?;

    let mut servers = tokio::task::JoinSet::<Result<()>>::new();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    #[cfg(feature = "webui")]
    if let Some(address) = args.webui {
        let password = std::env::var(&args.web_password_env)
            .with_context(|| format!("missing environment variable {}", args.web_password_env))?;
        let console = forest_webui::Console::bind(
            scheduler.inspector(),
            scheduler.module_manager(),
            scheduler.config_writer("/user/admin".parse()?),
            forest_webui::Options {
                address,
                password,
                assets: args.webui_assets,
                packages: args.packages_directory.clone(),
            },
        )
        .await?;
        scheduler.logs().write(
            LogLevel::Info,
            "/core/runtime".parse()?,
            format!("Forest console at http://{}", console.address()),
        );
        servers.spawn(console.serve(async move {
            let _ = stopped.await;
        }));
    }
    #[cfg(not(feature = "webui"))]
    drop(stopped);

    let mut roots = Vec::new();
    let mut declarations = Vec::new();
    let mut available = Vec::new();
    for directory in args.package {
        let package = scheduler.package(&directory).await?;
        roots.push(package.manifest().package.id.clone());
        declarations.push((directory, package.manifest().clone()));
        available.push(package);
    }
    let mut directories = tokio::fs::read_dir(&args.packages_directory).await?;
    while let Some(entry) = directories.next_entry().await? {
        let candidate = entry.path();
        if candidate.is_dir() && !declarations.iter().any(|(p, _)| p == &candidate) {
            match forest_package::read_manifest(&candidate) {
                Ok(manifest) => {
                    // Explicit roots can also be catalog entries under an absolute path.
                    if !declarations
                        .iter()
                        .any(|(_, m)| m.package.id == manifest.package.id)
                    {
                        declarations.push((candidate, manifest));
                    }
                }
                Err(error) => scheduler.logs().write(
                    LogLevel::Warn,
                    "/core/runtime".parse()?,
                    format!("package {}: {error}", candidate.display()),
                ),
            }
        }
    }
    let selected = scheduler.plan_manifests(
        &declarations
            .iter()
            .map(|(_, m)| m.clone())
            .collect::<Vec<_>>(),
        &roots,
    )?;
    for (directory, manifest) in declarations {
        if !roots.contains(&manifest.package.id) && selected.contains(&manifest.package.id) {
            available.push(scheduler.package(directory).await?);
        }
    }
    scheduler.load_roots(available, &roots).await?;
    let result = tokio::select! {
        signal = tokio::signal::ctrl_c() => { signal?; Ok(()) },
        Some(result) = servers.join_next(), if !servers.is_empty() => result.context("console task")?,
    };
    // Keep the administration endpoint available while a Drain request waits.
    let shutdown = scheduler.shutdown(args.shutdown_mode.into()).await;
    let _ = stop.send(());
    while let Some(result) = servers.join_next().await {
        result.context("console task")??;
    }
    shutdown?;
    result
}
