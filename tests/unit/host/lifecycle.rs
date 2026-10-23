use super::*;

struct Transport(mpsc::UnboundedSender<LifecycleReport>);

#[async_trait]
impl LifecycleTransport for Transport {
    async fn report(&self, report: LifecycleReport) -> Result<()> {
        self.0.send(report)?;
        Ok(())
    }
}

struct Executor;

#[async_trait]
impl ActorExecutor for Executor {
    fn supports(&self, _: &str) -> bool {
        true
    }
    fn resident(&self) -> Option<bool> {
        Some(true)
    }
    async fn invoke(
        &self,
        _: crate::actor::ActorMethodInvocation,
        _: Arc<dyn crate::actor::ActorDatabase>,
    ) -> Result<crate::actor::ActorMethodOutcome> {
        unreachable!()
    }
}

#[tokio::test(start_paused = true)]
async fn reports_activation_heartbeat_and_graceful_shutdown() -> Result<()> {
    let (sent, mut received) = mpsc::unbounded_channel();
    let (status, observed) = watch::channel(ActorStatus::default());
    let lifecycle = HostLifecycle::start(
        Arc::new(Transport(sent)),
        7,
        ResourceLimits::default(),
        Arc::new(Executor),
        observed,
    )?;
    let started = received.recv().await.unwrap();
    assert_eq!(started.kind, LifecycleKind::Started);
    assert_eq!(started.owner_epoch, 7);
    assert_eq!(started.sequence, 1);
    assert!(started.occurred_at_ms >= started.started_at_ms);
    status.send_modify(|status| {
        status.waiting.push(crate::host_leases::WaitingOperation {
            id: "request".into(),
            operation: "increment".into(),
        })
    });
    let heartbeat = received.recv().await.unwrap();
    assert_eq!(heartbeat.kind, LifecycleKind::Heartbeat);
    assert_eq!(heartbeat.sequence, 2);
    assert_eq!(heartbeat.resident, Some(true));
    assert_eq!(heartbeat.waiting[0].operation, "increment");
    lifecycle.draining();
    lifecycle.stop().await;
    let draining = received.recv().await.unwrap();
    assert_eq!(draining.kind, LifecycleKind::Draining);
    assert_eq!(draining.sequence, 3);
    let stopped = received.recv().await.unwrap();
    assert_eq!(stopped.kind, LifecycleKind::Stopped);
    assert_eq!(stopped.sequence, 4);
    assert_eq!(stopped.started_at_ms, started.started_at_ms);
    assert_eq!(stopped.resources, started.resources);
    assert_eq!(stopped.resident, Some(false));
    assert!(stopped.waiting.is_empty());
    Ok(())
}
