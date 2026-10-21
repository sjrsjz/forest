//! Static plans use only manifests. Planning never starts modules or edits the
//! user's enabled roots; execution remains an explicit management operation.
use crate::core::{Directory, State};
use crate::{Package, Scheduler, StopMode};
use anyhow::{Context, Result, ensure};
use forest_protocol::{Manifest, Token};
use std::collections::{HashMap, HashSet};

fn visit(
    object: &Token,
    manifests: &HashMap<Token, &Manifest>,
    active: &mut HashSet<Token>,
    done: &mut HashSet<Token>,
    order: &mut Vec<Token>,
) -> Result<()> {
    if done.contains(object) {
        return Ok(());
    }
    ensure!(
        active.insert(object.clone()),
        "cyclic module dependency at {object}"
    );
    let manifest = manifests[object];
    for dependency in &manifest.uses {
        if dependency.object != *object && manifests.contains_key(&dependency.object) {
            visit(&dependency.object, manifests, active, done, order)?;
        }
    }
    active.remove(object);
    done.insert(object.clone());
    order.push(object.clone());
    Ok(())
}

pub(crate) fn order(directory: &Directory, candidate: Option<&Manifest>) -> Result<Vec<Token>> {
    let mut manifests: HashMap<_, _> = directory
        .slots
        .iter()
        .map(|(id, slot)| (id.clone(), slot.published.as_ref()))
        .collect();
    if let Some(candidate) = candidate {
        manifests.insert(candidate.package.id.clone(), candidate);
    }
    let mut roots: Vec<_> = manifests.keys().cloned().collect();
    roots.sort();
    let (mut active, mut done, mut order) = (HashSet::new(), HashSet::new(), Vec::new());
    for root in roots {
        visit(&root, &manifests, &mut active, &mut done, &mut order)?;
    }
    Ok(order)
}

pub(crate) fn dependents(directory: &Directory, object: &Token) -> Vec<Token> {
    let mut users: Vec<_> = directory
        .slots
        .iter()
        .filter(|(id, slot)| {
            *id != object && slot.published.uses.iter().any(|u| &u.object == object)
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
    /// Returns a dependency-first plan for the explicitly enabled roots.
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
        for package in self.plan_load(available, roots)? {
            self.load(package).await?;
        }
        Ok(())
    }
    pub fn dependents(&self, object: &Token) -> Vec<Token> {
        dependents(&self.core.directory.lock().unwrap(), object)
    }
    pub async fn shutdown(&self, mode: StopMode) -> Result<()> {
        let mut sequence = order(&self.core.directory.lock().unwrap(), None)?;
        sequence.reverse();
        for object in sequence {
            self.unload(&object, mode).await?;
        }
        Ok(())
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
        selected.insert(id, manifest);
    }
    let (mut active, mut done, mut sequence) = (HashSet::new(), HashSet::new(), Vec::new());
    for root in roots {
        if selected.contains_key(root) {
            visit(root, &selected, &mut active, &mut done, &mut sequence)?;
        }
    }
    Ok(sequence)
}
