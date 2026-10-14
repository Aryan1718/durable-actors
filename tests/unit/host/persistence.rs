use super::*;
use crate::storage::StateStream;
use tokio::sync::Semaphore;

struct ControlledWriter {
    started: mpsc::UnboundedSender<u64>,
    permits: Arc<Semaphore>,
    fail: bool,
}

#[async_trait]
impl SnapshotWriter for ControlledWriter {
    async fn write_snapshot(&self, plan: &WritePlan, _: bytes::Bytes) -> Result<StateWrite> {
        self.started.send(plan.state_version)?;
        self.permits.acquire().await?.forget();
        anyhow::ensure!(!self.fail, "storage rejected write");
        Ok(StateWrite::Written)
    }
}

#[tokio::test]
async fn saves_wait_for_storage_and_run_one_at_a_time() -> Result<()> {
    let (started, mut starts) = mpsc::unbounded_channel();
    let permits = Arc::new(Semaphore::new(0));
    let persistence = ActorPersistence::with_writer(
        Arc::new(ControlledWriter {
            started,
            permits: permits.clone(),
            fail: false,
        }),
        CancellationToken::new(),
    );
    let first = save(persistence.clone(), 1);
    assert_eq!(starts.recv().await, Some(1));
    let second = save(persistence.clone(), 2);
    tokio::task::yield_now().await;
    assert!(!first.is_finished());
    assert!(!second.is_finished());
    assert!(starts.try_recv().is_err());
    permits.add_permits(1);
    assert_eq!(first.await??, StateWrite::Written);
    assert_eq!(starts.recv().await, Some(2));
    assert!(!second.is_finished());
    permits.add_permits(1);
    assert_eq!(second.await??, StateWrite::Written);
    Ok(())
}

#[tokio::test]
async fn failed_save_stops_later_saves() -> Result<()> {
    let (started, mut starts) = mpsc::unbounded_channel();
    let permits = Arc::new(Semaphore::new(0));
    let persistence = ActorPersistence::with_writer(
        Arc::new(ControlledWriter {
            started,
            permits: permits.clone(),
            fail: true,
        }),
        CancellationToken::new(),
    );
    let first = save(persistence.clone(), 1);
    assert_eq!(starts.recv().await, Some(1));
    let second = save(persistence.clone(), 2);
    permits.add_permits(1);
    assert!(
        first
            .await?
            .unwrap_err()
            .to_string()
            .contains("storage rejected write")
    );
    assert!(second.await?.is_err());
    assert!(
        persistence
            .write_snapshot(&plan(3), bytes::Bytes::new())
            .await
            .is_err()
    );
    assert!(starts.recv().await.is_none());
    Ok(())
}

#[tokio::test]
async fn cancelling_a_caller_stops_the_save_queue() -> Result<()> {
    let (started, mut starts) = mpsc::unbounded_channel();
    let permits = Arc::new(Semaphore::new(0));
    let persistence = ActorPersistence::with_writer(
        Arc::new(ControlledWriter {
            started,
            permits: permits.clone(),
            fail: false,
        }),
        CancellationToken::new(),
    );
    let first = save(persistence.clone(), 1);
    assert_eq!(starts.recv().await, Some(1));
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert!(save(persistence, 2).await?.is_err());
    assert!(starts.recv().await.is_none());
    Ok(())
}

fn save(
    persistence: Arc<ActorPersistence>,
    version: u64,
) -> tokio::task::JoinHandle<Result<StateWrite>> {
    tokio::spawn(async move {
        persistence
            .write_snapshot(&plan(version), bytes::Bytes::new())
            .await
    })
}

fn plan(state_version: u64) -> WritePlan {
    WritePlan {
        stream: StateStream {
            session: "test".into(),
            prefix: "test/".into(),
            owner_epoch: 1,
            base_version: 0,
        },
        state_version,
        object_name: format!("test/{state_version}.json"),
    }
}
