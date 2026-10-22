//! Host configuration writes with a bound, authenticated source. No ACL bypass.
use crate::{Scheduler, core::Core};
use forest_protocol::{
    DispatchError, DispatchResult, Token,
    service::{ConfigKey, ConfigSet},
};
use forest_util::acl::Access;
use std::sync::{Arc, Weak};

#[derive(Clone)]
pub struct ConfigWriter {
    core: Weak<Core>,
    subject: Token,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigWriteError {
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
    #[error(transparent)]
    Configuration(#[from] forest_config::ConfigError),
}

impl Scheduler {
    /// The host authenticates this source before granting the handle. Requests
    /// made through it cannot replace the source or obtain a writable Store.
    pub fn config_writer(&self, subject: Token) -> ConfigWriter {
        ConfigWriter {
            core: Arc::downgrade(&self.core),
            subject,
        }
    }
}

impl ConfigWriter {
    pub fn check(&self, target: &ConfigKey) -> DispatchResult<()> {
        let core = self.core.upgrade().ok_or(DispatchError::HostStopped)?;
        let directory = core.directory.lock().unwrap();
        Core::config_access_as(&directory, &self.subject, target, Access::Write)
    }

    pub async fn set(&self, input: ConfigSet) -> Result<(), ConfigWriteError> {
        let core = self.core.upgrade().ok_or(DispatchError::HostStopped)?;
        {
            let directory = core.directory.lock().unwrap();
            Core::config_access_as(&directory, &self.subject, &input.target, Access::Write)?;
        }
        core.services.config_set(input).await?;
        Ok(())
    }
}
