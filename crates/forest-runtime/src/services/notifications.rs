use super::*;
use std::{future::Future, sync::Weak};
use tokio::task::{AbortHandle, JoinSet};

impl Resources {
    fn notification(&self, future: impl Future<Output = ()> + Send + 'static) -> AbortHandle {
        let mut jobs = self.notifications.lock().unwrap();
        while let Some(joined) = jobs.try_join_next() {
            if let Err(error) = joined {
                assert!(
                    error.is_cancelled(),
                    "I/O notification task panicked: {error}"
                );
            }
        }
        jobs.spawn(future)
    }

    pub(crate) async fn stop_notifications(&self) {
        let mut jobs: JoinSet<()> = std::mem::take(&mut *self.notifications.lock().unwrap());
        jobs.abort_all();
        while let Some(joined) = jobs.join_next().await {
            if let Err(error) = joined {
                assert!(
                    error.is_cancelled(),
                    "I/O notification task panicked: {error}"
                );
            }
        }
    }

    pub(crate) fn ws_watch(
        &self,
        core: Weak<Core>,
        instance: Weak<Instance>,
        request: WebSocketWatch,
    ) -> std::result::Result<(), ServiceError> {
        let socket = self.socket(request.handle)?;
        let taken = Arc::new(Notify::new());
        {
            let mut mode = socket.mode.lock().unwrap();
            if !matches!(*mode, ReceiveMode::Pull) {
                return Err(ServiceError::InUse);
            }
            let _stream = socket.receive.try_lock().map_err(|_| ServiceError::InUse)?;
            *mode = ReceiveMode::Watched {
                pending: None,
                taken: taken.clone(),
            };
        }
        let watched = socket.clone();
        let abort = self.notification(async move {
            let mut stream = watched.receive.lock().await;
            loop {
                let result = receive_frame(&mut stream).await;
                let terminal = !matches!(&result, Ok(WebSocketEvent::Message(_)));
                {
                    let mut mode = watched.mode.lock().unwrap();
                    let ReceiveMode::Watched { pending, .. } = &mut *mode else {
                        unreachable!("watched receive mode is permanent");
                    };
                    assert!(pending.replace(result).is_none(), "one buffered WS result");
                }
                if !post(
                    &core,
                    &instance,
                    &request.interface,
                    IoEvent::WebSocketReady(
                        request.handle.try_into().expect("resource id exhausted"),
                    ),
                )
                .await
                {
                    break;
                }
                taken.notified().await;
                if terminal {
                    break;
                }
            }
        });
        *socket.watch.lock().unwrap() = Some(abort);
        Ok(())
    }

    pub(crate) fn timer_after(
        &self,
        core: Weak<Core>,
        instance: Weak<Instance>,
        request: TimerRequest,
    ) -> u64 {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.notification(async move {
            tokio::time::sleep(std::time::Duration::from_millis(request.delay_ms)).await;
            post(
                &core,
                &instance,
                &request.interface,
                IoEvent::Timer(id.try_into().expect("resource id exhausted")),
            )
            .await;
        });
        id
    }
}

async fn post(
    core: &Weak<Core>,
    instance: &Weak<Instance>,
    interface: &Token,
    event: IoEvent,
) -> bool {
    let (Some(core), Some(instance)) = (core.upgrade(), instance.upgrade()) else {
        return false;
    };
    match core.notify_instance(&instance, interface, event).await {
        Ok(()) => true,
        Err(DispatchError::Interrupted | DispatchError::LifecycleRestricted) => false,
        Err(error) => {
            core.logs.write(
                LogLevel::Error,
                instance.object.clone(),
                format!("I/O notification {interface}: {error}"),
            );
            false
        }
    }
}
