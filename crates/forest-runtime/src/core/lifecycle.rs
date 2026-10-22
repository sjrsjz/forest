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
    ) -> Result<()> {
        crate::dependencies::order(directory, Some(manifest))?;
        for dependency in &manifest.uses {
            let declaration = if dependency.object == manifest.package.id {
                manifest
                    .provides
                    .iter()
                    .find(|p| p.path == dependency.interface)
            } else if let Some(native) = directory
                .natives
                .get(&(dependency.object.clone(), dependency.interface.clone()))
            {
                Some(&native.declaration)
            } else {
                directory.slots.get(&dependency.object).and_then(|slot| {
                    slot.published
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
            let event = directory
                .events
                .get(&(subscription.source.clone(), subscription.event.clone()))
                .context("host event unavailable")?;
            let handler = manifest
                .provides
                .iter()
                .find(|p| p.path == subscription.interface)
                .unwrap();
            ensure!(
                event.input.subof(&handler.input),
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
                "host event source denied by handler callers"
            );
        }
        Ok(())
    }
    pub(super) fn stop(
        &self,
        object: &Token,
        mode: StopMode,
        package: Option<Package>,
    ) -> Result<oneshot::Receiver<std::result::Result<(), String>>> {
        let (completion, receive) = oneshot::channel();
        let mut directory = self.directory.lock().unwrap();
        if let Some(package) = &package {
            self.check_dependencies(&directory, &package.manifest)?;
        } else {
            let users = crate::dependencies::dependents(&directory, object);
            ensure!(users.is_empty(), "module {object} is required by {users:?}");
        }
        let slot = directory.slots.get_mut(object).context("unknown module")?;
        if let Some(package) = &package {
            package.check_replacement(&slot.published)?;
        }
        if matches!(slot.state, State::Failed(_)) {
            ensure!(
                package.is_none(),
                "reload of a failed object uses load(package)"
            );
            directory.slots.remove(object);
            let _ = completion.send(Ok(()));
            return Ok(receive);
        }
        ensure!(
            matches!(slot.state, State::Serving(_)),
            "module already changing lifecycle phase"
        );
        let Slot {
            published,
            access,
            budget,
            state,
        } = directory.slots.remove(object).unwrap();
        let State::Serving(live) = state else {
            unreachable!()
        };
        let after = match package {
            Some(package) => After::Reload {
                package,
                inbox: VecDeque::new(),
                completion,
            },
            None => After::Unload(completion),
        };
        if mode == StopMode::Force {
            live.instance.force.store(true, Ordering::Release);
            self.engine.increment_epoch();
        }
        live.instance.mailbox.changed.notify_one();
        directory.slots.insert(
            object.clone(),
            Slot {
                published,
                access,
                budget,
                state: State::Stopping { live, mode, after },
            },
        );
        Ok(receive)
    }
}

impl Core {
    pub(crate) fn activate(&self, instance: &Arc<Instance>) -> Result<()> {
        let mut directory = self.directory.lock().unwrap();
        ensure!(
            !instance.force.load(Ordering::Acquire),
            "initialization interrupted"
        );
        let Slot {
            published: _,
            access: _,
            budget,
            state,
        } = directory
            .slots
            .remove(&instance.object)
            .expect("initializing slot");
        let State::Creating {
            instance: current,
            inbox,
            completion,
        } = state
        else {
            unreachable!()
        };
        assert_eq!(current.id, instance.id);
        let mut live = Live::new(instance.clone());
        if let CreatingInbox::Deferred(inbox) = inbox {
            for request in inbox {
                live.enqueue(request);
            }
        }
        directory.slots.insert(
            instance.object.clone(),
            Slot {
                published: instance.package.manifest.clone(),
                access: instance.package.access.clone(),
                budget,
                state: State::Serving(live),
            },
        );
        drop(directory);
        self.logs.write(
            service::LogLevel::Info,
            instance.object.clone(),
            format!(
                "module {} is Serving (instance {})",
                instance.object, instance.id
            ),
        );
        let _ = completion.send(Ok(()));
        Ok(())
    }
    pub(crate) fn turn(&self, instance: &Instance, tasks_empty: bool) -> crate::driver::Turn {
        let mut directory = self.directory.lock().unwrap();
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
                // Remove readiness, take the request and transfer its interface permit
                // under the same lock used by enqueue/complete.
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
        let mut directory = self.directory.lock().unwrap();
        let live = match &mut directory.slots.get_mut(&instance.object).unwrap().state {
            State::Serving(live) | State::Stopping { live, .. } => live,
            _ => unreachable!(),
        };
        live.complete(started.permit);
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
    pub(crate) fn begin_releasing(&self, instance: &Arc<Instance>, error: Option<DispatchError>) {
        let error = error.unwrap_or(DispatchError::Interrupted);
        let mut directory = self.directory.lock().unwrap();
        let Slot {
            published,
            access,
            budget,
            state,
        } = directory.slots.remove(&instance.object).unwrap();
        let after = match state {
            State::Creating {
                inbox, completion, ..
            } => {
                if let CreatingInbox::Deferred(inbox) = inbox {
                    for request in inbox {
                        request.outcome.settle(Err(error.clone()));
                    }
                }
                let _ = completion.send(Err(error.to_string()));
                After::Fail(error.to_string())
            }
            State::Serving(live) => {
                live.reject_queued(&error);
                After::Fail(error.to_string())
            }
            State::Stopping { live, after, .. } => {
                live.reject_queued(&error);
                after
            }
            State::Destroying { after, .. } => after,
            _ => unreachable!("driver releases its live instance once"),
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
                },
            },
        );
    }
    pub(crate) fn finish_releasing(self: &Arc<Self>, instance: &Arc<Instance>) {
        let next = {
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
            } = state
            else {
                unreachable!()
            };
            assert_eq!(current.id, instance.id);
            match after {
                After::Unload(completion) => {
                    let _ = completion.send(Ok(()));
                    None
                }
                After::Fail(reason) => {
                    directory.slots.insert(
                        instance.object.clone(),
                        Slot {
                            published,
                            access,
                            budget,
                            state: State::Failed(reason),
                        },
                    );
                    None
                }
                After::Reload {
                    package,
                    inbox,
                    completion,
                } => {
                    let next = self
                        .instance(package)
                        .expect("candidate metadata already validated");
                    directory.slots.insert(
                        instance.object.clone(),
                        Slot {
                            published,
                            access,
                            budget,
                            state: State::Creating {
                                instance: next.clone(),
                                inbox: CreatingInbox::Deferred(inbox),
                                completion,
                            },
                        },
                    );
                    Some(next)
                }
            }
        };
        if let Some(next) = next {
            crate::driver::spawn(self.clone(), next);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::Turn;

    #[tokio::test]
    async fn selecting_serial_work_transfers_its_permit_before_new_admission() {
        let root = tempfile::tempdir().unwrap();
        let scheduler = Scheduler::in_memory(RuntimeOptions {
            data_directory: root.path().into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let core = &scheduler.core;
        let object: Token = "/module/serial-test".parse().unwrap();
        let interface: Token = "/serial".parse().unwrap();
        let caller: Token = "/user/test".parse().unwrap();
        let manifest = Arc::new(Manifest {
            package: PackageInfo {
                id: object.clone(),
                version: "test".into(),
                name: None,
                description: None,
            },
            module: "module.wasm".into(),
            configs: vec![],
            uses: vec![],
            subscriptions: vec![],
            provides: vec![InterfaceDeclaration {
                path: interface.clone(),
                input: i64::as_schema(),
                output: Result::<i64, String>::as_schema(),
                reentrant: false,
                access: manifest::callers(vec![SubjectPattern::Path(
                    "/user/test".parse().unwrap(),
                )]),
            }],
        });
        let access = Arc::new(AccessTables::compile(&manifest).unwrap());
        let instance = core
            .instance(Package {
                manifest: manifest.clone(),
                access: access.clone(),
                // This test exercises admission and ownership; it never executes guest code.
                module: wasmtime::Module::new(&core.engine, "(module)").unwrap(),
            })
            .unwrap();
        core.directory.lock().unwrap().slots.insert(
            object.clone(),
            Slot {
                published: manifest,
                access,
                budget: Arc::new(Budget::new(&core.options)),
                state: State::Serving(Live::new(instance.clone())),
            },
        );
        scheduler
            .send(caller.clone(), object.clone(), interface.clone(), &1i64)
            .unwrap();
        let Turn::Start(first) = core.turn(&instance, true) else {
            panic!("first request")
        };
        // Reproduce arrival after selecting work, before the driver enters Wasm.
        scheduler
            .send(caller, object.clone(), interface, &2i64)
            .unwrap();
        assert!(matches!(core.turn(&instance, false), Turn::Wait));
        core.complete(&instance, *first, Ok(Ok::<i64, String>(1).as_value()));
        let Turn::Start(second) = core.turn(&instance, true) else {
            panic!("second request after release")
        };
        assert_eq!(second.request.invocation.input, 2i64.as_value());
        core.complete(&instance, *second, Ok(Ok::<i64, String>(2).as_value()));
        assert!(matches!(core.turn(&instance, true), Turn::Wait));
        core.directory.lock().unwrap().slots.clear();
    }
}
