use super::*;

impl Core {
    pub(crate) fn instance(self: &Arc<Self>, package: Package) -> Result<Arc<Instance>> {
        let object = package.manifest.package.id.clone();
        Ok(Arc::new(Instance {
            id: self.next_instance.fetch_add(1, Ordering::Relaxed),
            object,
            package,
            core: Arc::downgrade(self),
            mailbox: Arc::new(Mailbox::default()),
            operations: Operations::default(),
            force: AtomicBool::new(false),
        }))
    }
    pub(super) fn check_dependencies(
        &self,
        directory: &Directory,
        manifest: &Manifest,
        catalog: &HashMap<Token, &Manifest>,
    ) -> Result<()> {
        for dependency in &manifest.uses {
            let declaration = if let Some(native) = directory
                .natives
                .get(&(dependency.object.clone(), dependency.interface.clone()))
            {
                Some(&native.declaration)
            } else {
                catalog.get(&dependency.object).and_then(|manifest| {
                    manifest
                        .provides
                        .iter()
                        .find(|p| p.path == dependency.interface)
                })
            };
            let declaration = declaration.context("interface dependency is not loaded")?;
            ensure!(
                declaration.satisfies(&dependency.input, &dependency.output),
                "incompatible dependency {} {}",
                dependency.object,
                dependency.interface
            );
        }
        for subscription in &manifest.subscriptions {
            let input = if let Some(event) = directory
                .events
                .get(&(subscription.source.clone(), subscription.event.clone()))
            {
                &event.input
            } else {
                let producer = catalog
                    .get(&subscription.source)
                    .context("event producer is not loaded")?;
                let event = producer
                    .events
                    .iter()
                    .find(|event| event.path == subscription.event)
                    .context("event is not declared")?;
                let acl = AclTable::new([AclEntry {
                    entity: subscription.source.clone(),
                    key: event.path.clone(),
                    owner: subscription.source.clone(),
                    access: event.access.clone(),
                }])?;
                ensure!(
                    acl.check(
                        &manifest.package.id,
                        &subscription.source,
                        &event.path,
                        Access::Read
                    ),
                    "event subscription denied by readers"
                );
                &event.input
            };
            let handler = manifest
                .provides
                .iter()
                .find(|p| p.path == subscription.interface)
                .unwrap();
            ensure!(
                input.subof(&handler.input),
                "incompatible event {} {} -> {}",
                subscription.source,
                subscription.event,
                handler.path
            );
            let acl = AclTable::new([AclEntry {
                entity: manifest.package.id.clone(),
                key: handler.path.clone(),
                owner: manifest.package.id.clone(),
                access: handler.access.clone(),
            }])?;
            ensure!(
                acl.check(
                    &subscription.source,
                    &manifest.package.id,
                    &handler.path,
                    Access::Call
                ),
                "event source denied by handler callers"
            );
        }
        if let Some(path) = &manifest.activated {
            let handler = manifest.provides.iter().find(|p| &p.path == path).unwrap();
            let acl = AclTable::new([AclEntry {
                entity: manifest.package.id.clone(),
                key: path.clone(),
                owner: manifest.package.id.clone(),
                access: handler.access.clone(),
            }])?;
            ensure!(
                acl.check(
                    &"/system/lifecycle".parse()?,
                    &manifest.package.id,
                    path,
                    Access::Call
                ),
                "activation handler denies lifecycle source"
            );
        }
        Ok(())
    }
    pub(crate) fn turn(&self, instance: &Instance, tasks_empty: bool) -> crate::driver::Turn {
        let mut directory = self.directory.lock().unwrap();
        self.commit_before_start(&mut directory, &instance.object);
        let slot = directory.slots.get_mut(&instance.object).unwrap();
        match &slot.state {
            State::Serving(_) => {}
            State::Stopping {
                live,
                mode: StopMode::Drain,
                ..
            } if tasks_empty && !live.pending() => {
                return crate::driver::Turn::Stop(StopMode::Drain);
            }
            State::Stopping {
                mode: StopMode::Drain,
                ..
            } => {}
            State::Stopping { mode, .. } => return crate::driver::Turn::Stop(*mode),
            _ => unreachable!("ordinary scheduler phase"),
        }
        match instance.mailbox.pop() {
            Some(crate::ready::Work::Start(interface)) => {
                let live = match &mut slot.state {
                    State::Serving(live) | State::Stopping { live, .. } => live,
                    _ => unreachable!("schedulable phase"),
                };
                // Remove readiness and bind the request under the same lock as
                // admission and management commitment.
                crate::driver::Turn::Start(Box::new(live.begin(&interface)))
            }
            Some(crate::ready::Work::Resume(id)) => crate::driver::Turn::Resume(id),
            None => crate::driver::Turn::Wait,
        }
    }
    pub(crate) fn may_resume(&self, instance: &Instance) -> bool {
        let directory = self.directory.lock().unwrap();
        matches!(
            directory.slots[&instance.object].state,
            State::Serving(_)
                | State::Stopping {
                    mode: StopMode::Drain,
                    ..
                }
        )
    }
    pub(crate) fn complete(
        &self,
        instance: &Instance,
        started: Started,
        result: DispatchResult<Value>,
    ) {
        let _directory = self.directory.lock().unwrap();
        let result = if instance.force.load(Ordering::Acquire) {
            Err(DispatchError::Interrupted)
        } else {
            result
        };
        started.request.outcome.settle(result);
    }
    pub(crate) fn reject_old_queue(&self, instance: &Instance, error: &DispatchError) {
        let mut directory = self.directory.lock().unwrap();
        let State::Stopping { live, .. } =
            &mut directory.slots.get_mut(&instance.object).unwrap().state
        else {
            unreachable!()
        };
        for inbox in live.interfaces.values_mut() {
            for request in inbox.queue.drain(..) {
                request.outcome.settle(Err(error.clone()));
            }
        }
    }
    pub(crate) fn enter_destroying(&self, instance: &Arc<Instance>) -> bool {
        let mut directory = self.directory.lock().unwrap();
        if instance.force.load(Ordering::Acquire) {
            return false;
        }
        let Slot {
            published,
            access,
            budget,
            state,
        } = directory.slots.remove(&instance.object).unwrap();
        let State::Stopping { live, after, .. } = state else {
            unreachable!()
        };
        assert!(!live.pending());
        directory.slots.insert(
            instance.object.clone(),
            Slot {
                published,
                access,
                budget,
                state: State::Destroying {
                    instance: instance.clone(),
                    after,
                },
            },
        );
        true
    }
    pub(crate) fn abort_creation(&self, instance: &Arc<Instance>) {
        let mut directory = self.directory.lock().unwrap();
        let Slot {
            published,
            access,
            budget,
            state,
        } = directory.slots.remove(&instance.object).unwrap();
        let State::Creating { inbox, .. } = state else {
            unreachable!("unpublished initialized instance")
        };
        inbox.reject(&DispatchError::Interrupted);
        directory.slots.insert(
            instance.object.clone(),
            Slot {
                published,
                access,
                budget,
                state: State::Destroying {
                    instance: instance.clone(),
                    after: After::Fail(DispatchError::Interrupted.into()),
                },
            },
        );
    }
    pub(crate) fn begin_releasing(
        &self,
        instance: &Arc<Instance>,
        reported: Option<crate::InstanceFailure>,
    ) {
        let failure = reported
            .clone()
            .unwrap_or_else(|| DispatchError::Interrupted.into());
        let error = failure.dispatch_error(&instance.object);
        let mut directory = self.directory.lock().unwrap();
        let Slot {
            published,
            access,
            budget,
            state,
        } = directory.slots.remove(&instance.object).unwrap();
        let after = match state {
            State::Creating { inbox, .. } => {
                inbox.reject(&error);
                After::Fail(failure)
            }
            State::Serving(live) => {
                live.reject_queued(&error);
                After::Fail(failure)
            }
            State::Stopping { live, after, .. } => {
                live.reject_queued(&error);
                after
            }
            State::Destroying { after, .. } => after,
            _ => unreachable!("driver releases its instance once"),
        };
        directory.slots.insert(
            instance.object.clone(),
            Slot {
                published,
                access,
                budget,
                state: State::Releasing {
                    instance: instance.clone(),
                    after,
                    failure: reported,
                },
            },
        );
        drop(directory);
        self.changed.notify_waiters();
    }
    pub(crate) fn finish_releasing(&self, instance: &Arc<Instance>) {
        let mut directory = self.directory.lock().unwrap();
        let Slot {
            published,
            access,
            budget,
            state,
        } = directory.slots.remove(&instance.object).unwrap();
        let State::Releasing {
            instance: current,
            after,
            failure,
        } = state
        else {
            unreachable!("released instance")
        };
        assert_eq!(current.id, instance.id);
        let state = match after {
            After::Fail(reason) => State::Failed(reason),
            After::Change { inbox, .. } => State::Staged { inbox, failure },
        };
        directory.slots.insert(
            instance.object.clone(),
            Slot {
                published,
                access,
                budget,
                state,
            },
        );
        drop(directory);
        self.changed.notify_waiters();
    }
}
