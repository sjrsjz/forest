//! Management requests. Pending requests never close a serving module's ingress.
//! The directory lock orders admission, group commitment and group publication.
use super::*;
use crate::driver::{Activation, PreparedDriver};
use crate::management::*;
use futures::{StreamExt, stream::FuturesUnordered};
use tokio::sync::watch;

pub(crate) enum Source {
    Prepared(Package),
    Directory(ReloadTarget),
}
impl Source {
    fn object(&self) -> &Token {
        match self {
            Self::Prepared(p) => &p.manifest.package.id,
            Self::Directory(t) => &t.object,
        }
    }
}
pub(crate) enum Action {
    Load(Vec<Package>),
    Reload(Vec<Source>),
    Unload(Vec<Token>),
}
impl Action {
    fn kind(&self) -> RequestKind {
        match self {
            Self::Load(_) => RequestKind::Load,
            Self::Reload(_) => RequestKind::Reload,
            Self::Unload(_) => RequestKind::Unload,
        }
    }
    fn objects(&self) -> Vec<Token> {
        match self {
            Self::Load(packages) => packages
                .iter()
                .map(|p| p.manifest.package.id.clone())
                .collect(),
            Self::Reload(sources) => sources.iter().map(|s| s.object().clone()).collect(),
            Self::Unload(objects) => objects.clone(),
        }
    }
}
enum Before {
    Absent,
    Failed,
    Serving(u64),
}
pub(crate) struct Change {
    pub status: watch::Sender<RequestSnapshot>,
    before: HashMap<Token, Before>,
    pub replacements: HashMap<Token, Package>,
}
impl Change {
    pub fn snapshot(&self) -> RequestSnapshot {
        self.status.borrow().clone()
    }
}

impl Core {
    pub(crate) fn begin_change(
        self: &Arc<Self>,
        action: Action,
        mode: StopMode,
    ) -> Result<ManagementRequest> {
        let kind = action.kind();
        let mut objects = action.objects();
        ensure!(!objects.is_empty(), "empty management request");
        objects.sort();
        ensure!(
            objects.windows(2).all(|w| w[0] != w[1]),
            "duplicate target object"
        );
        let id = RequestId(self.next_request.fetch_add(1, Ordering::Relaxed));
        let snapshot = RequestSnapshot {
            id,
            kind,
            objects: objects.clone(),
            mode,
            state: RequestState::Preparing,
        };
        let (status, receive) = watch::channel(snapshot);
        {
            let mut directory = self.directory.lock().unwrap();
            for change in directory.changes.values() {
                ensure!(
                    change
                        .snapshot()
                        .objects
                        .iter()
                        .all(|o| !objects.contains(o)),
                    "module already has a management request"
                );
            }
            let mut before = HashMap::new();
            for object in objects {
                let prior = match directory.slots.get(&object).map(|s| &s.state) {
                    None if kind == RequestKind::Load => Before::Absent,
                    Some(State::Failed(_)) if kind != RequestKind::Reload => Before::Failed,
                    Some(State::Serving(live)) if kind != RequestKind::Load => {
                        Before::Serving(live.instance.id)
                    }
                    Some(State::Failed(_)) => {
                        anyhow::bail!("reload of a failed object uses load(package)")
                    }
                    None => anyhow::bail!("unknown module {object}"),
                    _ => anyhow::bail!(
                        "module {object} is already loaded or changing lifecycle phase"
                    ),
                };
                before.insert(object, prior);
            }
            directory.changes.insert(
                id,
                Change {
                    status,
                    before,
                    replacements: HashMap::new(),
                },
            );
        }
        let core = self.clone();
        tokio::spawn(async move {
            if let Err(failure) = core.execute_change(id, action).await {
                core.finish_change(id, Err(failure));
            }
        });
        self.changed.notify_waiters();
        Ok(ManagementRequest {
            core: Arc::downgrade(self),
            status: receive,
        })
    }

    pub(crate) fn cancel_change(&self, id: RequestId) -> std::result::Result<(), CancelError> {
        let mut directory = self.directory.lock().unwrap();
        let change = directory.changes.get(&id).ok_or(CancelError::NotFound)?;
        if !change.snapshot().state.pending() {
            return Err(CancelError::AlreadyCommitted);
        }
        // Pending has changed no slot or ingress; cancellation has nothing to replay.
        let change = directory.changes.remove(&id).unwrap();
        change
            .status
            .send_modify(|s| s.state = RequestState::Cancelled);
        drop(directory);
        self.changed.notify_waiters();
        Ok(())
    }

    pub(crate) fn force_change(&self, object: &Token) -> Result<()> {
        let mut directory = self.directory.lock().unwrap();
        let change = directory
            .changes
            .values()
            .find(|c| c.snapshot().objects.contains(object))
            .context("module has no management request")?;
        let snapshot = change.snapshot();
        let mut changed = snapshot.mode != StopMode::Force;
        if changed {
            change.status.send_modify(|s| s.mode = StopMode::Force);
        }
        let mut interrupted = false;
        if !snapshot.state.pending() {
            for object in &snapshot.objects {
                if let Some(slot) = directory.slots.get_mut(object) {
                    let mode_changed = if let State::Stopping { mode, .. } = &mut slot.state {
                        let different = *mode != StopMode::Force;
                        *mode = StopMode::Force;
                        different
                    } else {
                        false
                    };
                    if let Some(instance) = slot.state.instance() {
                        let newly_forced = !instance.force.swap(true, Ordering::AcqRel);
                        if newly_forced || mode_changed {
                            instance.mailbox.changed.notify_one();
                        }
                        interrupted |= newly_forced;
                        changed |= newly_forced || mode_changed;
                    }
                }
            }
        }
        if interrupted {
            self.engine.increment_epoch();
        }
        drop(directory);
        if changed {
            self.changed.notify_waiters();
        }
        Ok(())
    }

    fn finish_change_in(
        &self,
        directory: &mut Directory,
        id: RequestId,
        result: std::result::Result<(), ManagementFailure>,
    ) {
        if let Some(change) = directory.changes.remove(&id) {
            change.status.send_modify(|s| {
                s.state = match result {
                    Ok(()) => RequestState::Succeeded,
                    Err(ManagementFailure::Cancelled) => RequestState::Cancelled,
                    Err(failure) => RequestState::Failed { failure },
                }
            });
        }
        self.changed.notify_waiters();
    }

    fn finish_change(&self, id: RequestId, result: std::result::Result<(), ManagementFailure>) {
        self.finish_change_in(&mut self.directory.lock().unwrap(), id, result);
    }

    fn check_before(
        &self,
        directory: &Directory,
        change: &Change,
    ) -> std::result::Result<(), ManagementFailure> {
        for (object, before) in &change.before {
            let current = directory.slots.get(object).map(|s| &s.state);
            let valid = match (before, current) {
                (Before::Absent, None) | (Before::Failed, Some(State::Failed(_))) => true,
                (Before::Serving(id), Some(State::Serving(live))) => live.instance.id == *id,
                _ => false,
            };
            if !valid {
                return Err(match current {
                    Some(State::Failed(failure)) => ManagementFailure::Instance {
                        object: object.clone(),
                        failure: failure.clone(),
                    },
                    _ => ManagementFailure::TargetChanged {
                        object: object.clone(),
                    },
                });
            }
        }
        Ok(())
    }

    fn check_catalog<'a>(
        &self,
        directory: &Directory,
        snapshot: &RequestSnapshot,
        packages: impl IntoIterator<Item = &'a Package>,
    ) -> Result<()> {
        let mut catalog: HashMap<_, _> = directory
            .slots
            .iter()
            .map(|(id, s)| (id.clone(), s.published.as_ref()))
            .collect();
        // Committed candidates reserve their future contracts until publication.
        for change in directory.changes.values() {
            let s = change.snapshot();
            if matches!(s.state, RequestState::Committed) {
                if s.kind == RequestKind::Unload {
                    for object in &s.objects {
                        catalog.remove(object);
                    }
                } else {
                    for (object, package) in &change.replacements {
                        catalog.insert(object.clone(), package.manifest());
                    }
                }
            }
        }
        if snapshot.kind == RequestKind::Unload {
            for object in &snapshot.objects {
                catalog.remove(object);
            }
        }
        for package in packages {
            let manifest = package.manifest();
            let object = &manifest.package.id;
            ensure!(
                !directory.natives.keys().any(|(id, _)| id == object),
                "reserved native object {object}"
            );
            ensure!(
                !directory.events.keys().any(|(source, _)| source == object),
                "module identity conflicts with host event source"
            );
            for reserved in ["/user", "/system"] {
                ensure!(
                    !reserved.parse::<Token>()?.is_prefix_of(object),
                    "module identity uses a reserved principal namespace"
                );
            }
            if snapshot.kind == RequestKind::Reload {
                package.check_replacement(&directory.slots[object].published)?;
            }
            catalog.insert(object.clone(), manifest);
        }
        for manifest in catalog.values() {
            self.check_dependencies(directory, manifest, &catalog)
                .with_context(|| format!("{} declaration", manifest.package.id))?;
        }
        Ok(())
    }

    // This also runs before START, so a ready reload can claim an idle group
    // before the drivers bind any of its queued requests to the old instances.
    pub(super) fn commit_before_start(&self, directory: &mut Directory, object: &Token) {
        let id = directory.changes.iter().find_map(|(id, change)| {
            let snapshot = change.snapshot();
            (matches!(snapshot.state, RequestState::Waiting) && snapshot.objects.contains(object))
                .then_some(*id)
        });
        if let Some(id) = id {
            if let Err(failure) = self.try_commit_change(directory, id) {
                self.finish_change_in(directory, id, Err(failure));
            }
        }
    }

    fn try_commit_change(
        &self,
        directory: &mut Directory,
        id: RequestId,
    ) -> std::result::Result<bool, ManagementFailure> {
        let change = &directory.changes[&id];
        let snapshot = change.snapshot();
        match snapshot.state {
            RequestState::Committed => return Ok(true),
            RequestState::Waiting => {}
            _ => return Ok(false),
        }
        self.check_before(directory, change)?;
        let quiet = snapshot.objects.iter().all(|o| {
            directory.slots.get(o).is_none_or(|s| {
                let (queued_requests, queued_bytes) = match &s.state {
                    State::Serving(live) if snapshot.kind == RequestKind::Reload => {
                        live.queued_budget()
                    }
                    _ => (0, 0),
                };
                s.budget.requests.available_permits() + queued_requests == self.options.max_requests
                    && s.budget.bytes.available_permits() + queued_bytes
                        == self.options.max_input_bytes
            })
        });
        if snapshot.kind != RequestKind::Load && snapshot.mode == StopMode::Drain && !quiet {
            return Ok(false);
        }
        self.check_catalog(directory, &snapshot, change.replacements.values())
            .map_err(|e| ManagementFailure::Preparation {
                object: snapshot.objects[0].clone(),
                message: format!("{e:#}"),
            })?;
        change
            .status
            .send_modify(|s| s.state = RequestState::Committed);
        if snapshot.kind != RequestKind::Load {
            for object in &snapshot.objects {
                let Slot {
                    published,
                    access,
                    budget,
                    state,
                } = directory.slots.remove(object).unwrap();
                let state = match state {
                    State::Serving(mut live) => {
                        let inbox = if snapshot.kind == RequestKind::Reload {
                            Ingress::Deferred(if snapshot.mode == StopMode::Drain {
                                live.take_queued()
                            } else {
                                VecDeque::new()
                            })
                        } else {
                            Ingress::Closed
                        };
                        if snapshot.mode == StopMode::Force {
                            live.instance.force.store(true, Ordering::Release);
                            self.engine.increment_epoch();
                        }
                        live.instance.mailbox.changed.notify_one();
                        State::Stopping {
                            live,
                            mode: snapshot.mode,
                            after: After::Change { request: id, inbox },
                        }
                    }
                    State::Failed(_) => State::Staged {
                        inbox: Ingress::Closed,
                        failure: None,
                    },
                    _ => unreachable!("validated management target"),
                };
                directory.slots.insert(
                    object.clone(),
                    Slot {
                        published,
                        access,
                        budget,
                        state,
                    },
                );
            }
        }
        self.changed.notify_waiters();
        Ok(true)
    }

    async fn execute_change(
        self: &Arc<Self>,
        id: RequestId,
        action: Action,
    ) -> std::result::Result<(), ManagementFailure> {
        let packages = match action {
            Action::Load(packages) => packages,
            Action::Unload(_) => vec![],
            Action::Reload(sources) => {
                let mut packages = Vec::new();
                for source in sources {
                    if !self.directory.lock().unwrap().changes.contains_key(&id) {
                        return Ok(());
                    }
                    let object = source.object().clone();
                    let package = match source {
                        Source::Prepared(p) => p,
                        Source::Directory(target) => Package::read(&self.engine, &target.directory)
                            .await
                            .map_err(|e| ManagementFailure::Preparation {
                                object: object.clone(),
                                message: format!("{e:#}"),
                            })?,
                    };
                    if package.manifest().package.id != object {
                        return Err(ManagementFailure::Preparation {
                            object,
                            message: "replacement changes module identity".into(),
                        });
                    }
                    packages.push(package);
                }
                packages
            }
        };
        {
            let mut directory = self.directory.lock().unwrap();
            let Some(change) = directory.changes.get(&id) else {
                return Ok(());
            };
            let snapshot = change.snapshot();
            self.check_before(&directory, change)?;
            self.check_catalog(&directory, &snapshot, &packages)
                .map_err(|e| ManagementFailure::Preparation {
                    object: snapshot.objects[0].clone(),
                    message: format!("{e:#}"),
                })?;
            let change = directory.changes.get_mut(&id).unwrap();
            change.replacements = packages
                .iter()
                .map(|p| (p.manifest.package.id.clone(), p.clone()))
                .collect();
            change
                .status
                .send_modify(|s| s.state = RequestState::Waiting);
        }
        let snapshot = loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let committed = {
                let mut directory = self.directory.lock().unwrap();
                let Some(change) = directory.changes.get(&id) else {
                    return Ok(());
                };
                let snapshot = change.snapshot();
                self.try_commit_change(&mut directory, id)?
                    .then_some(snapshot)
            };
            if let Some(snapshot) = committed {
                break snapshot;
            }
            changed.await;
        };
        self.changed.notify_waiters();
        if snapshot.kind != RequestKind::Load {
            self.wait_staged(&snapshot.objects).await;
        }
        if snapshot.kind == RequestKind::Unload {
            let failure = {
                let mut directory = self.directory.lock().unwrap();
                let mut failure = None;
                let mode = directory.changes[&id].snapshot().mode;
                for object in &snapshot.objects {
                    let slot = directory.slots.remove(object).unwrap();
                    if let State::Staged {
                        failure: Some(error),
                        ..
                    } = slot.state
                    {
                        let forced = mode == StopMode::Force
                            && matches!(
                                error,
                                crate::InstanceFailure::Dispatch {
                                    error: DispatchError::Interrupted
                                }
                            );
                        if !forced && failure.is_none() {
                            failure = Some(ManagementFailure::Instance {
                                object: object.clone(),
                                failure: error,
                            });
                        }
                    }
                }
                failure
            };
            self.finish_change(id, failure.map_or(Ok(()), Err));
            return Ok(());
        }
        self.create_group(id, packages).await
    }

    async fn wait_staged(&self, objects: &[Token]) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if objects.iter().all(|o| {
                matches!(
                    self.directory.lock().unwrap().slots[o].state,
                    State::Staged { .. }
                )
            }) {
                return;
            }
            changed.await;
        }
    }

    async fn create_group(
        self: &Arc<Self>,
        id: RequestId,
        packages: Vec<Package>,
    ) -> std::result::Result<(), ManagementFailure> {
        let instances = {
            let mut directory = self.directory.lock().unwrap();
            let kind = directory.changes[&id].snapshot().kind;
            let mut instances = Vec::new();
            for package in packages {
                let instance = self
                    .instance(package)
                    .expect("instance identity allocation");
                let object = instance.object.clone();
                let (budget, inbox, published, access) = match directory.slots.remove(&object) {
                    Some(Slot {
                        budget,
                        state: State::Staged { inbox, .. },
                        published,
                        access,
                    }) => (budget, inbox, published, access),
                    Some(Slot {
                        budget,
                        state: State::Failed(_),
                        ..
                    }) if kind == RequestKind::Load => (
                        budget,
                        Ingress::Closed,
                        instance.package.manifest.clone(),
                        instance.package.access.clone(),
                    ),
                    None if kind == RequestKind::Load => (
                        Arc::new(Budget::new(&self.options, self.changed.clone())),
                        Ingress::Closed,
                        instance.package.manifest.clone(),
                        instance.package.access.clone(),
                    ),
                    _ => unreachable!("committed creation target"),
                };
                directory.slots.insert(
                    object,
                    Slot {
                        published,
                        access,
                        budget,
                        state: State::Creating {
                            instance: instance.clone(),
                            inbox,
                        },
                    },
                );
                instances.push(instance);
            }
            instances
        };
        self.changed.notify_waiters();
        // Every declared default exists before any member can run create.
        for instance in &instances {
            if let Err(error) = self.services.declare(&instance.package.manifest).await {
                let failure: crate::InstanceFailure = DispatchError::ExecutionFailed(format!(
                    "configuration declarations: {error:#}"
                ))
                .into();
                for member in &instances {
                    self.begin_releasing(member, Some(failure.clone()));
                    self.finish_releasing(member);
                }
                return Err(ManagementFailure::Instance {
                    object: instance.object.clone(),
                    failure,
                });
            }
        }
        let mut activations = Vec::new();
        let mut ready = FuturesUnordered::new();
        for instance in &instances {
            let PreparedDriver {
                ready: receive,
                activate,
            } = crate::driver::spawn(self.clone(), instance.clone());
            let object = instance.object.clone();
            ready.push(async move {
                (
                    object,
                    receive
                        .await
                        .unwrap_or_else(|_| Err(DispatchError::HostStopped.into())),
                )
            });
            activations.push(activate);
        }
        let mut failure = None;
        while let Some((object, result)) = ready.next().await {
            if let Err(error) = result {
                failure = Some(ManagementFailure::Instance {
                    object,
                    failure: error,
                });
                break;
            }
        }
        if failure.is_none() {
            let mut directory = self.directory.lock().unwrap();
            for instance in &instances {
                if instance.force.load(Ordering::Acquire)
                    || !matches!(
                        directory.slots[&instance.object].state,
                        State::Creating { .. }
                    )
                {
                    failure = Some(ManagementFailure::Instance {
                        object: instance.object.clone(),
                        failure: DispatchError::Interrupted.into(),
                    });
                    break;
                }
            }
            if failure.is_none() {
                for instance in &instances {
                    let Slot { budget, state, .. } =
                        directory.slots.remove(&instance.object).unwrap();
                    let State::Creating { inbox, .. } = state else {
                        unreachable!("initialized group member")
                    };
                    let mut live = Live::new(instance.clone());
                    if let Ingress::Deferred(queue) = inbox {
                        for request in queue {
                            live.enqueue(request);
                        }
                    }
                    directory.slots.insert(
                        instance.object.clone(),
                        Slot {
                            budget,
                            published: instance.package.manifest.clone(),
                            access: instance.package.access.clone(),
                            state: State::Serving(live),
                        },
                    );
                }
                let change = directory.changes.remove(&id).unwrap();
                change
                    .status
                    .send_modify(|s| s.state = RequestState::Succeeded);
            }
        }
        if let Some(failure) = failure {
            for activate in activations {
                let _ = activate.send(Activation::Discard);
            }
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if instances.iter().all(|i| {
                    matches!(
                        self.directory.lock().unwrap().slots[&i.object].state,
                        State::Failed(_)
                    )
                }) {
                    break;
                }
                changed.await;
            }
            return Err(failure);
        }
        for activate in activations {
            let _ = activate.send(Activation::Serve);
        }
        for instance in &instances {
            self.logs.write(
                service::LogLevel::Info,
                instance.object.clone(),
                format!(
                    "module {} is Serving (instance {})",
                    instance.object, instance.id
                ),
            );
        }
        self.changed.notify_waiters();
        Ok(())
    }
}
