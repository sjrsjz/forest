use crate::{AccessPolicy, Schema, Token, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub package: PackageInfo,
    pub module: String,
    pub configs: Vec<ConfigDeclaration>,
    pub provides: Vec<InterfaceDeclaration>,
    pub uses: Vec<InterfaceUse>,
    pub subscriptions: Vec<EventSubscription>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageInfo {
    pub id: Token,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigDeclaration {
    pub key: Token,
    pub schema: Schema,
    pub default: Value,
    pub access: AccessPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InterfaceDeclaration {
    pub path: Token,
    pub input: Schema,
    pub output: Schema,
    pub reentrant: bool,
    pub access: AccessPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InterfaceUse {
    pub object: Token,
    pub interface: Token,
    pub input: Schema,
    pub output: Schema,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventSubscription {
    pub source: Token,
    pub event: Token,
    pub interface: Token,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid manifest: {0}")]
pub struct ManifestError(pub String);

impl Manifest {
    pub fn validate(&self) -> Result<(), ManifestError> {
        let fail = |message: &str| ManifestError(message.into());
        if self.package.version.is_empty() {
            return Err(fail("empty release marker"));
        }
        if self.module.is_empty()
            || self.module.contains('\\')
            || self.module.contains(':')
            || self
                .module
                .split('/')
                .any(|p| p.is_empty() || p == "." || p == "..")
        {
            return Err(fail("module must be a relative package file"));
        }
        let mut keys = HashSet::new();
        for config in &self.configs {
            if !keys.insert(&config.key) {
                return Err(fail("duplicate configuration path"));
            }
            if !config.default.check(&config.schema, false) {
                return Err(fail("invalid default"));
            }
            if !config.access.callers.is_empty() {
                return Err(fail("configuration cannot grant calls"));
            }
        }
        let mut paths = HashSet::new();
        for interface in &self.provides {
            if !paths.insert(&interface.path) {
                return Err(fail("duplicate interface path"));
            }
            interface.validate()?;
        }
        let mut uses = HashSet::new();
        for dependency in &self.uses {
            if !uses.insert((&dependency.object, &dependency.interface)) {
                return Err(fail("duplicate interface use"));
            }
        }
        let mut subscriptions = HashSet::new();
        for subscription in &self.subscriptions {
            if !paths.contains(&subscription.interface) {
                return Err(fail("subscription handler missing"));
            }
            if !subscriptions.insert((
                &subscription.source,
                &subscription.event,
                &subscription.interface,
            )) {
                return Err(fail("duplicate subscription"));
            }
        }
        Ok(())
    }
}

pub fn equivalent(a: &Schema, b: &Schema) -> bool {
    a.subof(b) && b.subof(a)
}

impl InterfaceDeclaration {
    /// Whether this implementation can serve the required input -> output.
    /// Inputs are contravariant; the complete Result output is covariant.
    pub fn satisfies(&self, input: &Schema, output: &Schema) -> bool {
        input.subof(&self.input) && self.output.subof(output)
    }

    pub fn validate(&self) -> Result<(), ManifestError> {
        if !self.access.readers.is_empty() || !self.access.writers.is_empty() {
            return Err(ManifestError("interface grants must use callers".into()));
        }
        let valid_output = matches!(self.output.root(), forest_serde::adt::SchemaEnum::Variant(variants)
            if variants.len()==2 && variants.contains_key("Ok") && variants.contains_key("Err"));
        if !valid_output {
            return Err(ManifestError(
                "interface output must be Result<T, E>".into(),
            ));
        }
        Ok(())
    }
}

pub fn callers(patterns: Vec<crate::SubjectPattern>) -> AccessPolicy {
    AccessPolicy {
        readers: vec![],
        writers: vec![],
        callers: patterns,
    }
}
