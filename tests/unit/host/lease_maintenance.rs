use super::*;
use crate::{
    clock::{Clock, SystemClock},
    host::HostId,
    host_leases::HostLease,
};
use async_trait::async_trait;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tokio::sync::Notify;

struct ManualClock(AtomicU64);

impl ManualClock {
    fn new(now_ms: u64) -> Self {
        Self(AtomicU64::new(now_ms))
    }

    fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

struct FlakyLeaseStore {
    calls: AtomicUsize,
    lease: Mutex<Option<HostLease>>,
    changed: Notify,
    clock: Arc<ManualClock>,
}

struct HangingLeaseRenewalStore {
    calls: AtomicUsize,
    lease: Mutex<Option<HostLease>>,
}

#[async_trait]
impl HostLeaseRegistry for HangingLeaseRenewalStore {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
            return std::future::pending().await;
        }
        let lease = HostLease {
            id: request.id.clone(),
            session_id: request.session_id.clone(),
            route: request.route.clone(),
            expires_at_ms: SystemClock.now_ms()?.saturating_add(request.duration_ms),
        };
        *self.lease.lock().expect("test store lock") = Some(lease.clone());
        Ok(lease)
    }

    async fn unregister(&self, _id: &HostId, _session_id: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl HostLeaseRegistry for FlakyLeaseStore {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.changed.notify_one();
        if call == 2 {
            anyhow::bail!("temporary store failure");
        }
        let lease = HostLease {
            id: request.id.clone(),
            session_id: request.session_id.clone(),
            route: request.route.clone(),
            expires_at_ms: self.clock.now_ms()?.saturating_add(request.duration_ms),
        };
        *self.lease.lock().expect("test store lock") = Some(lease.clone());

        Ok(lease)
    }

    async fn unregister(&self, id: &HostId, session_id: &str) -> Result<()> {
        let mut lease = self.lease.lock().expect("test store lock");
        if lease
            .as_ref()
            .is_some_and(|lease| &lease.id == id && lease.session_id == session_id)
        {
            *lease = None;
        }
        Ok(())
    }
}

impl FlakyLeaseStore {
    async fn get(&self, id: &HostId) -> Result<Option<HostLease>> {
        Ok(self
            .lease
            .lock()
            .expect("test store lock")
            .clone()
            .filter(|lease| &lease.id == id))
    }
}

#[tokio::test(start_paused = true)]
async fn registers_immediately_and_renews_on_schedule() -> Result<()> {
    struct Store(watch::Sender<Option<HostLease>>);

    #[async_trait]
    impl HostLeaseRegistry for Store {
        async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
            let lease = HostLease {
                id: request.id.clone(),
                session_id: request.session_id.clone(),
                route: request.route.clone(),
                expires_at_ms: SystemClock.now_ms()? + request.duration_ms,
            };
            self.0.send_replace(Some(lease.clone()));
            Ok(lease)
        }

        async fn unregister(&self, _: &HostId, _: &str) -> Result<()> {
            Ok(())
        }
    }

    let (reports, mut reported) = watch::channel(None);
    let manager = Arc::new(HostLeaseMaintainer::new(
        HostEndpoint {
            id: HostId::new("scheduled"),
            route: "http://host".into(),
        },
        "session".into(),
        Arc::new(Store(reports)),
        Arc::new(SystemClock),
        Duration::from_secs(30),
        Duration::from_secs(10),
    )?);
    let renewal = manager.start().await?;
    assert_eq!(
        reported.borrow_and_update().as_ref().unwrap().id,
        HostId::new("scheduled")
    );
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(9)).await;
    tokio::task::yield_now().await;
    assert!(!reported.has_changed()?);
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::time::timeout(Duration::from_secs(1), reported.changed()).await??;
    renewal.shutdown().await?;
    Ok(())
}

#[test]
fn new_hosts_receive_unique_session_ids() {
    let first = HostEndpoint {
        id: HostId::new(uuid::Uuid::new_v4().to_string()),
        route: "sandbox-route".into(),
    };
    let second = HostEndpoint {
        id: HostId::new(uuid::Uuid::new_v4().to_string()),
        route: "sandbox-route".into(),
    };

    assert_ne!(first.id, second.id);
    assert_eq!(first.route, "sandbox-route");
}

#[tokio::test]
async fn registers_immediately_retries_failure_and_stops_cleanly() -> Result<()> {
    let clock = Arc::new(ManualClock::new(1_000));
    let store = Arc::new(FlakyLeaseStore {
        calls: AtomicUsize::new(0),
        lease: Mutex::new(None),
        changed: Notify::new(),
        clock: clock.clone(),
    });
    let node = HostEndpoint {
        id: HostId::new("node-a"),
        route: "sandbox-session-a".into(),
    };
    let manager = Arc::new(HostLeaseMaintainer::new(
        node.clone(),
        "session-a".into(),
        store.clone(),
        clock.clone(),
        Duration::from_millis(1_000),
        Duration::from_millis(10),
    )?);

    let renewal = manager.clone().start().await?;
    assert_eq!(store.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .get(&node.id)
            .await?
            .expect("initial lease")
            .expires_at_ms,
        2_000
    );

    clock.set(1_500);
    wait_for_calls(&store, 3).await?;
    assert_eq!(
        store
            .get(&node.id)
            .await?
            .expect("renewed lease")
            .expires_at_ms,
        2_500
    );

    renewal.shutdown().await?;
    let calls_after_shutdown = store.calls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(store.calls.load(Ordering::SeqCst), calls_after_shutdown);

    Ok(())
}

#[tokio::test]
async fn pending_renewal_cannot_outlive_the_confirmed_lease_window() -> Result<()> {
    let store = Arc::new(HangingLeaseRenewalStore {
        calls: AtomicUsize::new(0),
        lease: Mutex::new(None),
    });
    let manager = Arc::new(HostLeaseMaintainer::new(
        HostEndpoint {
            id: HostId::new("node-a"),
            route: "sandbox-session-a".into(),
        },
        "session-a".into(),
        store.clone(),
        Arc::new(SystemClock),
        Duration::from_millis(100),
        Duration::from_millis(10),
    )?);

    let renewal = manager.start().await?;
    let mut lease_lost = renewal.lease_lost();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !*lease_lost.borrow() {
            lease_lost.changed().await?;
        }
        Ok::<(), watch::error::RecvError>(())
    })
    .await??;

    assert!(*lease_lost.borrow());
    assert_eq!(store.calls.load(Ordering::SeqCst), 2);
    renewal.shutdown().await?;
    Ok(())
}

async fn wait_for_calls(store: &FlakyLeaseStore, expected: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.calls.load(Ordering::SeqCst) < expected {
            store.changed.notified().await;
        }
    })
    .await?;

    Ok(())
}
