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
        let cache_directory = options.data_directory.join(".wasmtime-cache");
        tokio::fs::create_dir_all(&cache_directory).await?;
        let mut cache_config = wasmtime::CacheConfig::new();
        cache_config.with_directory(tokio::fs::canonicalize(&cache_directory).await?);
        let mut config = wasmtime::Config::new();
        config
            .epoch_interruption(true)
            .cache(Some(wasmtime::Cache::new(cache_config)?));
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
            logs: crate::LogWriter::default(),
            run_queue,
            changed: Arc::new(Notify::new()),
            directory: Mutex::new(Directory {
                changes: HashMap::new(),
                host_config: crate::io_settings::IoSettings::access(),
                slots: HashMap::new(),
                natives: HashMap::new(),
                native_budgets: HashMap::new(),
                events: HashMap::new(),
            }),
            next_instance: AtomicU64::new(1),
            next_request: AtomicU64::new(1),
            _ticker: ticker,
        });
        Ok(Self { core })
    }
    pub async fn package(&self, directory: impl AsRef<Path>) -> Result<Package> {
        let started = std::time::Instant::now();
        let package = Package::read(&self.core.engine, directory.as_ref()).await?;
        self.core.logs.write(
            forest_protocol::service::LogLevel::Info,
            "/core/runtime".parse().unwrap(),
            format!(
                "Prepared {} in {:.3}s",
                package.manifest().package.id,
                started.elapsed().as_secs_f64()
            ),
        );
        Ok(package)
    }
    pub fn logs(&self) -> crate::LogWriter {
        self.core.logs.clone()
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
    pub fn failure(&self, object: &Token) -> Option<crate::InstanceFailure> {
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
        ensure!(
            directory
                .changes
                .values()
                .all(|c| !c.snapshot().objects.contains(&native.object)),
            "native object conflicts with a reserved module"
        );
        let key = (native.object.clone(), native.declaration.path.clone());
        ensure!(
            !directory.natives.contains_key(&key),
            "duplicate native interface"
        );
        directory
            .native_budgets
            .entry(native.object.clone())
            .or_insert_with(|| {
                Arc::new(Budget::new(&self.core.options, self.core.changed.clone()))
            });
        let access = AclTable::new([AclEntry {
            entity: native.object.clone(),
            key: native.declaration.path.clone(),
            owner: native.object.clone(),
            access: native.declaration.access.clone(),
        }])?;
        directory.natives.insert(
            key,
            Arc::new(RegisteredNative {
                object: native.object,
                declaration: native.declaration,
                handler: native.handler,
                access,
            }),
        );
        Ok(())
    }
    pub fn register_event(&self, event: EventDefinition) -> Result<()> {
        let mut directory = self.core.directory.lock().unwrap();
        ensure!(
            directory
                .changes
                .values()
                .all(|c| !c.snapshot().objects.contains(&event.source)),
            "event source conflicts with a reserved module"
        );
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
        self.load_many(vec![package]).await
    }
    pub async fn load_many(&self, packages: Vec<Package>) -> Result<()> {
        self.core
            .begin_change(changes::Action::Load(packages), StopMode::Drain)?
            .wait()
            .await
            .map_err(Into::into)
    }
    pub async fn unload(&self, object: &Token, mode: StopMode) -> Result<()> {
        self.unload_many(vec![object.clone()], mode).await
    }
    pub async fn unload_many(&self, objects: Vec<Token>, mode: StopMode) -> Result<()> {
        self.core
            .begin_change(changes::Action::Unload(objects), mode)?
            .wait()
            .await
            .map_err(Into::into)
    }
    pub async fn reload(&self, package: Package, mode: StopMode) -> Result<()> {
        self.request_reload(vec![package], mode)?
            .wait()
            .await
            .map_err(Into::into)
    }
    pub fn request_reload(
        &self,
        packages: Vec<Package>,
        mode: StopMode,
    ) -> Result<crate::ManagementRequest> {
        self.core.begin_change(
            changes::Action::Reload(
                packages
                    .into_iter()
                    .map(changes::Source::Prepared)
                    .collect(),
            ),
            mode,
        )
    }
    pub fn request_reload_from(
        &self,
        replacements: Vec<crate::ReloadTarget>,
        mode: StopMode,
    ) -> Result<crate::ManagementRequest> {
        self.core.begin_change(
            changes::Action::Reload(
                replacements
                    .into_iter()
                    .map(changes::Source::Directory)
                    .collect(),
            ),
            mode,
        )
    }
    pub fn cancel(&self, request: crate::RequestId) -> std::result::Result<(), crate::CancelError> {
        self.core.cancel_change(request)
    }
    pub fn force(&self, object: &Token) -> Result<()> {
        self.core.force_change(object)
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
