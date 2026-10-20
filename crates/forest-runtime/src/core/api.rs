use super::*;

impl Scheduler {
    pub async fn new(database: impl AsRef<Path>, options: RuntimeOptions) -> Result<Self> {
        let store = forest_config::ConfigStore::open(database).await?;
        Self::with_store(store, options).await
    }
    pub async fn in_memory(options: RuntimeOptions) -> Result<Self> {
        Self::with_store(forest_config::ConfigStore::open_in_memory().await?, options).await
    }
    pub async fn with_store(
        store: forest_config::ConfigStore,
        options: RuntimeOptions,
    ) -> Result<Self> {
        ensure!(
            options.parallelism > 0,
            "execution parallelism must be positive"
        );
        ensure!(
            options.max_requests > 0 && options.max_requests <= u32::MAX as usize,
            "invalid request capacity"
        );
        ensure!(
            options.max_input_bytes > 0 && options.max_input_bytes <= u32::MAX as usize,
            "invalid input capacity"
        );
        let mut config = wasmtime::Config::new();
        config.async_support(true).epoch_interruption(true);
        let engine = wasmtime::Engine::new(&config)?;
        let services = Services::new(
            store,
            &options.data_directory,
            options.http_proxy.as_deref(),
        )
        .await?;
        let ticker = EpochTicker::start(engine.clone())?;
        let run_queue = crate::fair::RunQueue::new(options.parallelism);
        let core = Arc::new(Core {
            engine: engine.clone(),
            options,
            services,
            run_queue,
            directory: Mutex::new(Directory {
                slots: HashMap::new(),
                natives: HashMap::new(),
                native_budgets: HashMap::new(),
                events: HashMap::new(),
            }),
            next_instance: AtomicU64::new(1),
            _ticker: ticker,
        });
        Ok(Self { core })
    }
    pub async fn package(&self, directory: impl AsRef<Path>) -> Result<Package> {
        Package::read(&self.core.engine, directory.as_ref()).await
    }
    pub fn phase(&self, object: &Token) -> Option<ModulePhase> {
        self.core
            .directory
            .lock()
            .unwrap()
            .slots
            .get(object)
            .map(|s| s.state.phase())
    }
    pub fn failure(&self, object: &Token) -> Option<String> {
        self.core
            .directory
            .lock()
            .unwrap()
            .slots
            .get(object)
            .and_then(|s| match &s.state {
                State::Failed(reason) => Some(reason.clone()),
                _ => None,
            })
    }
    pub fn register_native(&self, native: NativeInterface) -> Result<()> {
        native.declaration.validate()?;
        let mut directory = self.core.directory.lock().unwrap();
        ensure!(
            !directory.slots.contains_key(&native.object),
            "native object conflicts with Wasm object"
        );
        let key = (native.object.clone(), native.declaration.path.clone());
        ensure!(
            !directory.natives.contains_key(&key),
            "duplicate native interface"
        );
        directory
            .native_budgets
            .entry(native.object.clone())
            .or_insert_with(|| Arc::new(Budget::new(&self.core.options)));
        let access = AclTable::new([AclEntry {
            entity: native.object.clone(),
            key: native.declaration.path.clone(),
            owner: native.object.clone(),
            access: native.declaration.access.clone(),
        }])?;
        let gate = if native.declaration.reentrant {
            NativeGate::Parallel
        } else {
            NativeGate::Serial(Arc::new(Semaphore::new(1)))
        };
        directory.natives.insert(
            key,
            Arc::new(RegisteredNative {
                object: native.object,
                declaration: native.declaration,
                handler: native.handler,
                access,
                gate,
            }),
        );
        Ok(())
    }
    pub fn register_event(&self, event: EventDefinition) -> Result<()> {
        let mut directory = self.core.directory.lock().unwrap();
        let key = (event.source.clone(), event.event.clone());
        ensure!(!directory.events.contains_key(&key), "duplicate host event");
        ensure!(
            !directory.slots.contains_key(&event.source),
            "host event source conflicts with Wasm identity"
        );
        directory.events.insert(key, event);
        Ok(())
    }
    pub async fn load(&self, package: Package) -> Result<()> {
        let (reply, receive) = oneshot::channel();
        let instance = self.core.instance(package.clone())?;
        {
            let mut directory = self.core.directory.lock().unwrap();
            self.core
                .check_dependencies(&directory, &package.manifest)?;
            ensure!(
                !directory
                    .natives
                    .keys()
                    .any(|(object, _)| object == &instance.object),
                "reserved native object"
            );
            if let Some(slot) = directory.slots.get(&instance.object) {
                ensure!(
                    matches!(slot.state, State::Failed(_)),
                    "module already loaded"
                );
            }
            for reserved in ["/user", "/system"] {
                ensure!(
                    !reserved.parse::<Token>()?.is_prefix_of(&instance.object),
                    "module identity uses a reserved principal namespace"
                );
            }
            ensure!(
                !directory
                    .events
                    .keys()
                    .any(|(source, _)| source == &instance.object),
                "module identity conflicts with a host event source"
            );
            directory.slots.insert(
                instance.object.clone(),
                Slot {
                    published: package.manifest.clone(),
                    access: package.access.clone(),
                    budget: Arc::new(Budget::new(&self.core.options)),
                    state: State::Creating {
                        instance: instance.clone(),
                        inbox: CreatingInbox::Closed,
                        completion: reply,
                    },
                },
            );
        }
        crate::driver::spawn(self.core.clone(), instance);
        receive
            .await
            .context("module driver stopped during initialization")?
            .map_err(anyhow::Error::msg)
    }
    pub async fn unload(&self, object: &Token, mode: StopMode) -> Result<()> {
        let receive = self.core.stop(object, mode, None)?;
        receive
            .await
            .context("module driver stopped during unload")?
            .map_err(anyhow::Error::msg)
    }
    pub async fn reload(&self, package: Package, mode: StopMode) -> Result<()> {
        let object = package.manifest.package.id.clone();
        let receive = self.core.stop(&object, mode, Some(package))?;
        receive
            .await
            .context("module driver stopped during reload")?
            .map_err(anyhow::Error::msg)
    }
    pub fn force(&self, object: &Token) -> Result<()> {
        let mut directory = self.core.directory.lock().unwrap();
        let slot = directory.slots.get_mut(object).context("unknown module")?;
        let instance = slot
            .state
            .instance()
            .context("module is not executing")?
            .clone();
        match &mut slot.state {
            State::Stopping { mode, .. } => *mode = StopMode::Force,
            State::Creating { .. } | State::Destroying { .. } => {}
            _ => anyhow::bail!("force requires an initialization or an existing stop operation"),
        }
        instance.force.store(true, Ordering::Release);
        self.core.engine.increment_epoch();
        instance.mailbox.changed.notify_one();
        Ok(())
    }
    pub async fn call<I: AsValue, O: AsValue>(
        &self,
        caller: Token,
        object: Token,
        interface: Token,
        input: &I,
    ) -> DispatchResult<O> {
        let value = self
            .core
            .call(
                CallContext {
                    origin: caller.clone(),
                    caller,
                },
                None,
                forest_protocol::service::CallRequest {
                    object,
                    interface,
                    input: input.as_value(),
                },
            )
            .await?;
        O::from_value(value).map_err(|_| DispatchError::OutputContract)
    }
    pub fn send<I: AsValue>(
        &self,
        caller: Token,
        object: Token,
        interface: Token,
        input: &I,
    ) -> DispatchResult<()> {
        self.core.send(
            CallContext {
                origin: caller.clone(),
                caller,
            },
            None,
            forest_protocol::service::CallRequest {
                object,
                interface,
                input: input.as_value(),
            },
        )
    }
    pub fn notify<I: AsValue>(
        &self,
        source: &Token,
        event: &Token,
        origin: Token,
        input: &I,
    ) -> Vec<(Token, DispatchResult<()>)> {
        let value = input.as_value();
        let targets = {
            let directory = self.core.directory.lock().unwrap();
            let Some(definition) = directory.events.get(&(source.clone(), event.clone())) else {
                return vec![(
                    source.clone(),
                    Err(DispatchError::InterfaceNotFound(event.clone())),
                )];
            };
            if !value.check(&definition.input, false) {
                return vec![(source.clone(), Err(DispatchError::InputContract))];
            }
            directory
                .slots
                .iter()
                .flat_map(|(object, slot)| {
                    slot.published
                        .subscriptions
                        .iter()
                        .filter(|s| &s.source == source && &s.event == event)
                        .map(|s| (object.clone(), s.interface.clone()))
                })
                .collect::<Vec<_>>()
        };
        targets
            .into_iter()
            .map(|(object, interface)| {
                let result = self.core.send(
                    CallContext {
                        origin: origin.clone(),
                        caller: source.clone(),
                    },
                    None,
                    forest_protocol::service::CallRequest {
                        object: object.clone(),
                        interface,
                        input: value.clone(),
                    },
                );
                (object, result)
            })
            .collect()
    }
}
