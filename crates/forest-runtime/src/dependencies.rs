//! Static plans use only manifests. Planning never starts modules or edits the
//! user's enabled roots; execution remains an explicit management operation.
use crate::core::{Directory, State};
use crate::{Package, Scheduler, StopMode};
use anyhow::{Context, Result, ensure};
use forest_protocol::{Manifest, Token};
use std::collections::HashMap;

pub(crate) fn dependents(directory: &Directory, object: &Token) -> Vec<Token> {
    let mut users: Vec<_> = directory
        .slots
        .iter()
        .filter(|(id, slot)| {
            *id != object
                && (slot.published.uses.iter().any(|u| &u.object == object)
                    || slot
                        .published
                        .subscriptions
                        .iter()
                        .any(|s| &s.source == object))
        })
        .map(|(id, _)| id.clone())
        .collect();
    users.sort();
    users
}

impl Scheduler {
    /// Plan from external declarations without compiling or executing Wasm.
    pub fn plan_manifests(&self, available: &[Manifest], roots: &[Token]) -> Result<Vec<Token>> {
        let mut candidates = HashMap::new();
        for manifest in available {
            let id = manifest.package.id.clone();
            ensure!(
                candidates.insert(id.clone(), manifest).is_none(),
                "duplicate candidate object {id}"
            );
        }
        plan(&self.core.directory.lock().unwrap(), candidates, roots)
    }
    /// Returns the dependency closure as a deterministic set of packages.
    /// Packages already present in the directory are left in place.
    pub fn plan_load(&self, available: Vec<Package>, roots: &[Token]) -> Result<Vec<Package>> {
        let mut candidates = HashMap::new();
        for package in available {
            let id = package.manifest().package.id.clone();
            ensure!(
                candidates.insert(id, package).is_none(),
                "duplicate candidate object"
            );
        }
        let sequence = plan(
            &self.core.directory.lock().unwrap(),
            candidates
                .iter()
                .map(|(id, p)| (id.clone(), p.manifest()))
                .collect(),
            roots,
        )?;
        Ok(sequence
            .into_iter()
            .map(|id| candidates.remove(&id).unwrap())
            .collect())
    }
    pub async fn load_roots(&self, available: Vec<Package>, roots: &[Token]) -> Result<()> {
        let packages = self.plan_load(available, roots)?;
        if packages.is_empty() {
            return Ok(());
        }
        self.load_many(packages).await
    }
    pub fn dependents(&self, object: &Token) -> Vec<Token> {
        dependents(&self.core.directory.lock().unwrap(), object)
    }
    pub async fn shutdown(&self, mode: StopMode) -> Result<()> {
        loop {
            let changed = self.core.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let requests: Vec<_> = self
                .core
                .directory
                .lock()
                .unwrap()
                .changes
                .values()
                .map(|c| c.snapshot())
                .collect();
            if requests.is_empty() {
                break;
            }
            for request in requests {
                if request.state.pending() {
                    let _ = self.cancel(request.id);
                } else if mode == StopMode::Force {
                    self.force(&request.objects[0])?;
                }
            }
            if self.core.directory.lock().unwrap().changes.is_empty() {
                break;
            }
            changed.await;
        }
        let objects = self
            .core
            .directory
            .lock()
            .unwrap()
            .slots
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        if objects.is_empty() {
            return Ok(());
        }
        self.unload_many(objects, mode).await
    }
}

fn plan(
    directory: &Directory,
    candidates: HashMap<Token, &Manifest>,
    roots: &[Token],
) -> Result<Vec<Token>> {
    let mut selected = HashMap::new();
    let mut pending = roots.to_vec();
    while let Some(id) = pending.pop() {
        if selected.contains_key(&id)
            || directory
                .slots
                .get(&id)
                .is_some_and(|slot| !matches!(slot.state, State::Failed(_)))
        {
            continue;
        }
        let manifest = *candidates
            .get(&id)
            .with_context(|| format!("missing package {id}"))?;
        for dependency in &manifest.uses {
            if dependency.object != id
                && !directory.has_native(&dependency.object, &dependency.interface)
            {
                pending.push(dependency.object.clone());
            }
        }
        for subscription in &manifest.subscriptions {
            if subscription.source != id
                && !directory
                    .events
                    .contains_key(&(subscription.source.clone(), subscription.event.clone()))
            {
                pending.push(subscription.source.clone());
            }
        }
        selected.insert(id, manifest);
    }
    let mut selected = selected.into_keys().collect::<Vec<_>>();
    selected.sort();
    Ok(selected)
}
