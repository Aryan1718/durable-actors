use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::response::sse::Event;
use tokio::sync::{broadcast, mpsc};

use crate::request_traces::{
    TracePage, TraceRecord, TraceStore, live::LiveTraceUpdate, replay::ReplayQuery,
};

pub(super) async fn stream(
    store: TraceStore,
    project: String,
    mut updates: broadcast::Receiver<Arc<LiveTraceUpdate>>,
    sender: mpsc::Sender<Result<Event, Infallible>>,
    mut page: TracePage,
) {
    let interval = Duration::from_secs(30);
    let mut reconcile = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    if !send(&sender, &page).await {
        return;
    }
    loop {
        let update = tokio::select! {
            _ = sender.closed() => return,
            _ = reconcile.tick() => None,
            update = updates.recv() => match update {
                Ok(update) => Some(update),
                Err(broadcast::error::RecvError::Closed) => return,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    updates = updates.resubscribe();
                    None
                },
            },
        };
        if let Some(LiveTraceUpdate::Batch(events)) = update.as_deref() {
            if !apply_batch(&mut page, events, &project) {
                continue;
            }
        } else {
            page = match store.replay(&project, &ReplayQuery::default()).await {
                Ok(page) => page,
                Err(error) => {
                    tracing::warn!(%error, "Live trace reconciliation failed");
                    let _ = sender
                        .send(Ok(Event::default()
                            .event("error")
                            .data("Request history unavailable")))
                        .await;
                    return;
                }
            };
        }
        if !send(&sender, &page).await {
            return;
        }
    }
}

fn apply_batch(
    page: &mut TracePage,
    events: &[crate::request_traces::TraceEvent],
    project: &str,
) -> bool {
    page.records = events
        .iter()
        .filter(|event| event.trace.project_id == project)
        .map(|event| TraceRecord {
            sequence: event.trace.started_at_ms,
            event: event.clone(),
        })
        .collect();
    let Some(cursor) = page.records.iter().map(|record| record.sequence).max() else {
        return false;
    };
    page.cursor = cursor;
    page.snapshot = false;
    true
}

async fn send(sender: &mpsc::Sender<Result<Event, Infallible>>, page: &TracePage) -> bool {
    sender
        .send(Ok(Event::default()
            .event("requests")
            .id(&page.resume_cursor)
            .json_data(page)
            .expect("serializable trace page")))
        .await
        .is_ok()
}
