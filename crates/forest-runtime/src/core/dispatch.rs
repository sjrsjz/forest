use super::*;

impl Core {
    pub(crate) fn permission(
        &self,
        instance: &Instance,
        access: HostAccess<'_>,
    ) -> DispatchResult<ModulePhase> {
        let directory = self.directory.lock().unwrap();
        Self::permission_in(&directory, instance, &access)
    }
    fn permission_in(
        directory: &Directory,
        instance: &Instance,
        access: &HostAccess<'_>,
    ) -> DispatchResult<ModulePhase> {
        let slot = directory
            .slots
            .get(&instance.object)
            .ok_or(DispatchError::Interrupted)?;
        let current = slot.state.instance().ok_or(DispatchError::Interrupted)?;
        if current.id != instance.id || instance.force.load(Ordering::Acquire) {
            return Err(DispatchError::Interrupted);
        }
        let phase = slot.state.phase();
        match phase {
            ModulePhase::Creating if matches!(access, HostAccess::Communication) => {
                Err(DispatchError::LifecycleRestricted)
            }
            ModulePhase::Destroying
                if matches!(
                    access,
                    HostAccess::Communication | HostAccess::Network | HostAccess::Wait
                ) =>
            {
                Err(DispatchError::LifecycleRestricted)
            }
            ModulePhase::Releasing | ModulePhase::Failed => Err(DispatchError::Interrupted),
            _ => Ok(phase),
        }
    }
    fn authorize_use(
        instance: Option<&Instance>,
        request: &forest_protocol::service::CallRequest,
        output: &Schema,
    ) -> DispatchResult<()> {
        if let Some(instance) = instance {
            let dependency = instance
                .package
                .manifest
                .uses
                .iter()
                .find(|u| u.object == request.object && u.interface == request.interface)
                .ok_or(DispatchError::Denied)?;
            if !output.subof(&dependency.output) {
                return Err(DispatchError::OutputContract);
            }
            if !request.input.check(&dependency.input, false) {
                return Err(DispatchError::InputContract);
            }
        }
        Ok(())
    }
    fn admit(
        &self,
        context: CallContext,
        instance: Option<&Instance>,
        request: forest_protocol::service::CallRequest,
        outcome: Outcome,
    ) -> DispatchResult<Admitted> {
        let mut directory = self.directory.lock().unwrap();
        if let Some(instance) = instance {
            Self::permission_in(&directory, instance, &HostAccess::Communication)?;
        }
        if let Some(native) = directory
            .natives
            .get(&(request.object.clone(), request.interface.clone()))
            .cloned()
        {
            Self::authorize_use(instance, &request, &native.declaration.output)?;
            if !native.access.check(
                &context.caller,
                &native.object,
                &native.declaration.path,
                Access::Call,
            ) {
                return Err(DispatchError::Denied);
            }
            if !request.input.check(&native.declaration.input, false) {
                return Err(DispatchError::InputContract);
            }
            let bytes = abi::encode(&request.input).map_err(|_| DispatchError::InputContract)?;
            let admission = directory.native_budgets[&request.object].reserve(bytes.len())?;
            return Ok(Admitted::Native {
                native,
                context,
                input: request.input,
                outcome,
                _admission: admission,
            });
        }
        let slot = directory
            .slots
            .get_mut(&request.object)
            .ok_or_else(|| DispatchError::ObjectNotFound(request.object.clone()))?;
        let declaration = slot
            .published
            .provides
            .iter()
            .find(|p| p.path == request.interface)
            .ok_or_else(|| DispatchError::InterfaceNotFound(request.interface.clone()))?;
        Self::authorize_use(instance, &request, &declaration.output)?;
        if !slot.access.calls.check(
            &context.caller,
            &request.object,
            &declaration.path,
            Access::Call,
        ) {
            return Err(DispatchError::Denied);
        }
        if !request.input.check(&declaration.input, false) {
            return Err(DispatchError::InputContract);
        }
        let invocation = Invocation {
            interface: request.interface,
            input: request.input,
            context,
        };
        let bytes = abi::encode(&invocation).map_err(|_| DispatchError::InputContract)?;
        let accepted = Request {
            admission: slot.budget.reserve(bytes.len())?,
            invocation,
            bytes,
            outcome,
        };
        match &mut slot.state {
            State::Serving(live) => live.enqueue(accepted),
            State::Stopping {
                after: After::Reload { inbox, .. },
                ..
            }
            | State::Destroying {
                after: After::Reload { inbox, .. },
                ..
            }
            | State::Releasing {
                after: After::Reload { inbox, .. },
                ..
            } => inbox.push_back(accepted),
            State::Creating {
                inbox: CreatingInbox::Deferred(inbox),
                ..
            } => inbox.push_back(accepted),
            _ => return Err(DispatchError::Unavailable(request.object)),
        }
        Ok(Admitted::Wasm)
    }
    pub fn send(
        &self,
        context: CallContext,
        instance: Option<&Instance>,
        request: forest_protocol::service::CallRequest,
    ) -> DispatchResult<()> {
        let admitted = self.admit(context, instance, request, Outcome::Send)?;
        if let Admitted::Native {
            native,
            context,
            input,
            outcome,
            _admission,
        } = admitted
        {
            tokio::spawn(run_native(native, context, input, outcome, _admission));
        }
        Ok(())
    }
    pub async fn call(
        self: &Arc<Self>,
        context: CallContext,
        instance: Option<&Instance>,
        request: forest_protocol::service::CallRequest,
    ) -> DispatchResult<Value> {
        let (reply, receive) = oneshot::channel();
        let admitted = self.admit(context, instance, request, Outcome::Call(reply))?;
        if let Admitted::Native {
            native,
            context,
            input,
            outcome,
            _admission,
        } = admitted
        {
            tokio::spawn(run_native(native, context, input, outcome, _admission));
        }
        receive.await.unwrap_or(Err(DispatchError::HostStopped))
    }
}

impl Core {
    pub(crate) fn submit<T: serde::Serialize + Send + 'static>(
        &self,
        instance: &Arc<Instance>,
        signal: &Arc<crate::ready::TaskSignal>,
        access: HostAccess<'_>,
        future: impl Future<Output = DispatchResult<T>> + Send + 'static,
    ) -> u64 {
        let directory = self.directory.lock().unwrap();
        let permission = Self::permission_in(&directory, instance, &access).and_then(|_| {
            if let HostAccess::Configuration(key, operation) = &access {
                Self::config_access_in(&directory, instance, key, *operation)
            } else {
                Ok(())
            }
        });
        instance
            .operations
            .start(signal, access.cancellable(), async move {
                permission?;
                future.await
            })
    }
}

enum Admitted {
    Wasm,
    Native {
        native: Arc<RegisteredNative>,
        context: CallContext,
        input: Value,
        outcome: Outcome,
        _admission: Admission,
    },
}
async fn run_native(
    native: Arc<RegisteredNative>,
    context: CallContext,
    input: Value,
    outcome: Outcome,
    admission: Admission,
) {
    let _permit = match &native.gate {
        NativeGate::Parallel => None,
        NativeGate::Serial(gate) => Some(
            gate.clone()
                .acquire_owned()
                .await
                .expect("native gate stays open"),
        ),
    };
    let value = (native.handler)(context, input).await;
    let result = if value.check(&native.declaration.output, false) {
        Ok(value)
    } else {
        Err(DispatchError::OutputContract)
    };
    outcome.settle(result);
    drop(admission);
}
