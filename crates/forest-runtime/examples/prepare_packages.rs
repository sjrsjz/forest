//! Populate the host's compilation cache without creating guest instances or doing I/O for them.
use anyhow::{Context, Result, ensure};
use forest_runtime::{RuntimeOptions, Scheduler};
use std::{env, path::PathBuf, time::Instant};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let data_directory = PathBuf::from(
        args.next()
            .context("usage: prepare_packages DATA_DIRECTORY PACKAGE...")?,
    );
    let packages = args.map(PathBuf::from).collect::<Vec<_>>();
    ensure!(
        !packages.is_empty(),
        "provide at least one package directory"
    );
    let started = Instant::now();
    let scheduler = Scheduler::in_memory(RuntimeOptions {
        data_directory,
        ..RuntimeOptions::default()
    })
    .await?;
    for directory in packages {
        let package_started = Instant::now();
        let package = scheduler.package(&directory).await?;
        println!(
            "{} {:.3}s",
            package.manifest().package.id,
            package_started.elapsed().as_secs_f64()
        );
    }
    println!("total {:.3}s", started.elapsed().as_secs_f64());
    Ok(())
}
