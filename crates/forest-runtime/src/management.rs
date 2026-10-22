//! Host-only module lifecycle capability. It cannot invoke interfaces, register
//! native services, write configuration, or publish events.
use crate::{Package, Scheduler, StopMode, core::Core};
use anyhow::{Context, Result};
use forest_protocol::{Manifest, Token};
use std::{
    path::Path,
    sync::{Arc, Weak},
};

#[derive(Clone)]
pub struct ModuleManager {
    core: Weak<Core>,
}

impl Scheduler {
    pub fn module_manager(&self) -> ModuleManager {
        ModuleManager {
            core: Arc::downgrade(&self.core),
        }
    }
}

impl ModuleManager {
    fn scheduler(&self) -> Result<Scheduler> {
        Ok(Scheduler {
            core: self.core.upgrade().context("host stopped")?,
        })
    }
    pub async fn package(&self, directory: impl AsRef<Path>) -> Result<Package> {
        self.scheduler()?.package(directory).await
    }
    pub fn plan(&self, available: &[Manifest], roots: &[Token]) -> Result<Vec<Token>> {
        self.scheduler()?.plan_manifests(available, roots)
    }
    pub async fn load(&self, available: Vec<Package>, roots: &[Token]) -> Result<()> {
        self.scheduler()?.load_roots(available, roots).await
    }
    pub async fn reload(&self, package: Package, mode: StopMode) -> Result<()> {
        self.scheduler()?.reload(package, mode).await
    }
    pub async fn unload(&self, object: &Token, mode: StopMode) -> Result<()> {
        self.scheduler()?.unload(object, mode).await
    }
    pub fn force(&self, object: &Token) -> Result<()> {
        self.scheduler()?.force(object)
    }
}
