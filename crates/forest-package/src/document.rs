use crate::{Error, files::Files};
use forest_protocol::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The disk format contains file references; the scheduler's Manifest contains values.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Document {
    pub package: PackageInfo,
    pub module: Module,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub configs: Vec<Config>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provides: Vec<Provides>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub uses: Vec<Uses>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subscriptions: Vec<EventSubscription>,
    pub sha256: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct Module {
    pub file: String,
    pub lifecycle_error: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub key: Token,
    pub schema: String,
    pub default: String,
    pub access: Policy,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Provides {
    pub path: Token,
    pub input: String,
    pub output: String,
    pub reentrant: bool,
    pub access: Policy,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Uses {
    pub object: Token,
    pub interface: Token,
    pub input: String,
    pub output: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    readers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    writers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    callers: Vec<String>,
}

fn subjects(subjects: Vec<String>) -> Result<Vec<SubjectPattern>, Error> {
    subjects
        .into_iter()
        .map(|subject| {
            if subject == "self" {
                Ok(SubjectPattern::SelfModule)
            } else {
                subject
                    .parse()
                    .map(SubjectPattern::Path)
                    .map_err(|source| Error::Subject { subject, source })
            }
        })
        .collect()
}

impl Policy {
    fn resolve(self) -> Result<AccessPolicy, Error> {
        Ok(AccessPolicy {
            readers: subjects(self.readers)?,
            writers: subjects(self.writers)?,
            callers: subjects(self.callers)?,
        })
    }
}

impl From<AccessPolicy> for Policy {
    fn from(policy: AccessPolicy) -> Self {
        fn strings(values: Vec<SubjectPattern>) -> Vec<String> {
            values
                .into_iter()
                .map(|value| match value {
                    SubjectPattern::SelfModule => "self".into(),
                    SubjectPattern::Path(path) => path.to_string(),
                })
                .collect()
        }
        Self {
            readers: strings(policy.readers),
            writers: strings(policy.writers),
            callers: strings(policy.callers),
        }
    }
}

impl Document {
    pub fn resolve(self, files: &mut Files) -> Result<Manifest, Error> {
        files.require(&self.module.file)?;
        for path in [&self.package.readme, &self.package.license_file]
            .into_iter()
            .flatten()
        {
            files.read(path)?;
        }
        let lifecycle_error = files.schema(&self.module.lifecycle_error)?;
        let configs = self
            .configs
            .into_iter()
            .map(|c| {
                Ok(ConfigDeclaration {
                    key: c.key,
                    schema: files.schema(&c.schema)?,
                    default: files.value(&c.default)?,
                    access: c.access.resolve()?,
                })
            })
            .collect::<Result<_, Error>>()?;
        let provides = self
            .provides
            .into_iter()
            .map(|p| {
                Ok(InterfaceDeclaration {
                    path: p.path,
                    input: files.schema(&p.input)?,
                    output: files.schema(&p.output)?,
                    reentrant: p.reentrant,
                    access: p.access.resolve()?,
                })
            })
            .collect::<Result<_, Error>>()?;
        let uses = self
            .uses
            .into_iter()
            .map(|u| {
                Ok(InterfaceUse {
                    object: u.object,
                    interface: u.interface,
                    input: files.schema(&u.input)?,
                    output: files.schema(&u.output)?,
                })
            })
            .collect::<Result<_, Error>>()?;
        let manifest = Manifest {
            package: self.package,
            module: self.module.file,
            lifecycle_error,
            configs,
            provides,
            uses,
            subscriptions: self.subscriptions,
        };
        manifest.validate()?;
        Ok(manifest)
    }
}
