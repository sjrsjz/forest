use super::*;
use forest_protocol::service::{EmitRequest, EventDelivery, IoEvent};

impl Core {
    pub(super) fn activation_request(
        &self,
        instance: &Instance,
        budget: &Budget,
        interface: &Token,
    ) -> Request {
        let invocation = Invocation {
            interface: interface.clone(),
            input: ().as_value(),
            context: CallContext {
                origin: instance.object.clone().into(),
                caller: "/system/lifecycle".parse().unwrap(),
            },
        };
        Request {
            binding: Binding::Instance,
            bytes: abi::encode(&invocation).expect("activation control frame"),
            invocation,
            admission: budget.activation(),
            outcome: Outcome::Send {
                logs: self.logs.clone(),
                object: instance.object.clone(),
            },
        }
    }

    pub(crate) fn check_callback(
        &self,
        instance: &Instance,
        interface: &Token,
    ) -> DispatchResult<()> {
        let directory = self.directory.lock().unwrap();
        Self::permission_in(&directory, instance, &HostAccess::Communication)?;
        let declaration = instance
            .package
            .manifest
            .provides
            .iter()
            .find(|p| &p.path == interface)
            .ok_or_else(|| DispatchError::InterfaceNotFound(interface.clone()))?;
        if !IoEvent::as_schema().subof(&declaration.input) {
            return Err(DispatchError::InputContract);
        }
        if !instance.package.access.calls.check(
            &"/system/io".parse().unwrap(),
            &instance.object,
            interface,
            Access::Call,
        ) {
            return Err(DispatchError::Denied);
        }
        Ok(())
    }

    pub(crate) async fn notify_instance(
        &self,
        instance: &Instance,
        interface: &Token,
        event: IoEvent,
    ) -> DispatchResult<()> {
        let invocation = Invocation {
            interface: interface.clone(),
            input: event.as_value(),
            context: CallContext {
                origin: instance.object.clone().into(),
                caller: "/system/io".parse().unwrap(),
            },
        };
        let bytes = abi::encode(&invocation).map_err(|_| DispatchError::FrameTooLarge)?;
        // A notification larger than the entire configured input capacity cannot
        // become admissible by waiting. Reject it before registering a waiter.
        if bytes.len() > self.options.max_input_bytes {
            return Err(DispatchError::QueueFull);
        }
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut directory = self.directory.lock().unwrap();
                Self::permission_in(&directory, instance, &HostAccess::Communication)?;
                let slot = directory.slots.get_mut(&instance.object).unwrap();
                let State::Serving(live) = &mut slot.state else {
                    return Err(DispatchError::Interrupted);
                };
                match slot.budget.reserve_notification(bytes.len()) {
                    Ok(admission) => {
                        live.enqueue(Request {
                            binding: Binding::Instance,
                            invocation,
                            bytes,
                            admission,
                            outcome: Outcome::Send {
                                logs: self.logs.clone(),
                                object: instance.object.clone(),
                            },
                        });
                        return Ok(());
                    }
                    Err(DispatchError::QueueFull) => {}
                    Err(error) => return Err(error),
                }
            }
            changed.await;
        }
    }

    pub(crate) fn emit(
        &self,
        instance: &Instance,
        inherited: Origin,
        request: EmitRequest,
    ) -> DispatchResult<Vec<EventDelivery>> {
        let mut directory = self.directory.lock().unwrap();
        Self::permission_in(&directory, instance, &HostAccess::Communication)?;
        let declaration = instance
            .package
            .manifest
            .events
            .iter()
            .find(|e| e.path == request.event)
            .ok_or_else(|| DispatchError::InterfaceNotFound(request.event.clone()))?;
        if !request.input.check(&declaration.input, false) {
            return Err(DispatchError::InputContract);
        }
        let targets: Vec<_> = directory
            .slots
            .iter()
            .flat_map(|(object, slot)| {
                slot.published
                    .subscriptions
                    .iter()
                    .filter(|s| s.source == instance.object && s.event == request.event)
                    .map(|s| (object.clone(), s.interface.clone()))
            })
            .collect();
        let origin = match request.subject {
            Some(subject) => Origin::External {
                issuer: instance.object.clone(),
                subject,
            },
            None => inherited,
        };
        Ok(targets
            .into_iter()
            .map(|(object, interface)| {
                let slot = directory.slots.get_mut(&object).unwrap();
                let result = (|| {
                    let invocation = Invocation {
                        interface: interface.clone(),
                        input: request.input.clone(),
                        context: CallContext {
                            origin: origin.clone(),
                            caller: instance.object.clone(),
                        },
                    };
                    let bytes =
                        abi::encode(&invocation).map_err(|_| DispatchError::FrameTooLarge)?;
                    let accepted = Request {
                        binding: Binding::Object,
                        admission: slot.budget.reserve(bytes.len())?,
                        bytes,
                        invocation,
                        outcome: Outcome::Send {
                            logs: self.logs.clone(),
                            object: object.clone(),
                        },
                    };
                    match &mut slot.state {
                        State::Serving(live) => live.enqueue(accepted),
                        State::Stopping {
                            after:
                                After::Change {
                                    inbox: Ingress::Deferred(queue),
                                    ..
                                },
                            ..
                        }
                        | State::Destroying {
                            after:
                                After::Change {
                                    inbox: Ingress::Deferred(queue),
                                    ..
                                },
                            ..
                        }
                        | State::Releasing {
                            after:
                                After::Change {
                                    inbox: Ingress::Deferred(queue),
                                    ..
                                },
                            ..
                        }
                        | State::Staged {
                            inbox: Ingress::Deferred(queue),
                            ..
                        }
                        | State::Creating {
                            inbox: Ingress::Deferred(queue),
                            ..
                        } => queue.push_back(accepted),
                        _ => return Err(DispatchError::Unavailable(object.clone())),
                    }
                    Ok(())
                })();
                EventDelivery {
                    object,
                    interface,
                    result,
                }
            })
            .collect())
    }
}
