//! Host-only module lifecycle capability. It cannot invoke interfaces, register
//! native services, write configuration, or publish events.
use crate::{Package, Scheduler, StopMode, core::Core};
use anyhow::{Context, Result};
use forest_protocol::{Manifest, Token};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Weak},
};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestKind {
    Load,
    Reload,
    Unload,
}

#[derive(Clone, Debug, Serialize, thiserror::Error)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ManagementFailure {
    #[error("management request cancelled")]
    Cancelled,
    #[error("host stopped")]
    HostStopped,
    #[error("{object}: {message}")]
    Preparation { object: Token, message: String },
    #[error("target changed while preparing {object}")]
    TargetChanged { object: Token },
    #[error("{object}: {failure}")]
    Instance {
        object: Token,
        #[source]
        failure: crate::InstanceFailure,
    },
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum RequestState {
    Preparing,
    Waiting,
    Committed,
    Succeeded,
    Cancelled,
    Failed { failure: ManagementFailure },
}
impl RequestState {
    pub fn pending(&self) -> bool {
        matches!(self, Self::Preparing | Self::Waiting)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct RequestSnapshot {
    pub id: RequestId,
    pub kind: RequestKind,
    pub objects: Vec<Token>,
    pub mode: StopMode,
    #[serde(flatten)]
    pub state: RequestState,
}

#[derive(Clone, Debug)]
pub struct ReloadTarget {
    pub object: Token,
    pub directory: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum CancelError {
    #[error("unknown management request")]
    NotFound,
    #[error("management request has already committed")]
    AlreadyCommitted,
    #[error("host stopped")]
    HostStopped,
}

pub struct ManagementRequest {
    pub(crate) core: Weak<Core>,
    pub(crate) status: watch::Receiver<RequestSnapshot>,
}
impl ManagementRequest {
    pub fn snapshot(&self) -> RequestSnapshot {
        self.status.borrow().clone()
    }
    pub fn cancel(&self) -> std::result::Result<(), CancelError> {
        self.core
            .upgrade()
            .ok_or(CancelError::HostStopped)?
            .cancel_change(self.snapshot().id)
    }
    pub async fn wait(mut self) -> std::result::Result<(), ManagementFailure> {
        loop {
            match self.status.borrow_and_update().state.clone() {
                RequestState::Succeeded => return Ok(()),
                RequestState::Cancelled => return Err(ManagementFailure::Cancelled),
                RequestState::Failed { failure } => return Err(failure),
                _ => {}
            }
            self.status
                .changed()
                .await
                .map_err(|_| ManagementFailure::HostStopped)?;
        }
    }
}

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
    pub fn request_reload_from(
        &self,
        replacements: Vec<ReloadTarget>,
        mode: StopMode,
    ) -> Result<ManagementRequest> {
        self.scheduler()?.request_reload_from(replacements, mode)
    }
    pub fn cancel(&self, request: RequestId) -> std::result::Result<(), CancelError> {
        self.core
            .upgrade()
            .ok_or(CancelError::HostStopped)?
            .cancel_change(request)
    }
    pub async fn unload(&self, object: &Token, mode: StopMode) -> Result<()> {
        self.scheduler()?.unload(object, mode).await
    }
    pub async fn unload_many(&self, objects: Vec<Token>, mode: StopMode) -> Result<()> {
        self.scheduler()?.unload_many(objects, mode).await
    }
    pub fn force(&self, object: &Token) -> Result<()> {
        self.scheduler()?.force(object)
    }
}
