use super::*;

struct SleepingExecutor {
    inner: IncrementingExecutor,
    evictions: AtomicUsize,
}

#[async_trait]
impl ActorExecutor for SleepingExecutor {
    fn supports(&self, _: &str) -> bool {
        true
    }

    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        database: Arc<dyn ActorDatabase>,
    ) -> Result<ActorMethodOutcome> {
        self.inner.invoke(invocation, database).await
    }

    async fn evict(&self, _: bool, database: Arc<dyn ActorDatabase>) -> Result<()> {
        persist_fields(&*database, json!({"slept": true}), true).await?;
        self.evictions.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn draining_evicts_the_resident_actor_with_its_database() -> Result<()> {
    let executor = Arc::new(SleepingExecutor {
        inner: IncrementingExecutor {
            invocations: AtomicU64::new(0),
        },
        evictions: AtomicUsize::new(0),
    });
    let state = Arc::new(FakeStateTransport::default());
    let host = create_host(
        HostEndpoint {
            id: super::super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        state.clone(),
        Arc::new(EmptySocketPublisher),
        replication().await,
    );
    assert_eq!(invoke(&host, "first").await?, completed(1));
    assert_eq!(executor.evictions.load(Ordering::Relaxed), 0);
    host.drain(Duration::from_secs(1)).await?;
    assert_eq!(executor.evictions.load(Ordering::Relaxed), 1);
    let writes = state.writes.lock().unwrap();
    assert_eq!(writes.len(), 2);
    assert_eq!(StateSnapshot::decode(&writes[1])?.request_id, "evict");
    Ok(())
}
