use std::collections::BTreeMap;

use super::{LifecycleEvent, LifecycleKind, STALE_AFTER_MS};
use crate::placement::{ActorResidency, ResidencyHistory, ResidencyPeriod};

pub(super) const EVENT_LIMIT: usize = 10_000;

pub(super) fn history(mut events: Vec<LifecycleEvent>, from: u64, to: u64) -> ResidencyHistory {
    let truncated = events.len() > EVENT_LIMIT;
    events.truncate(EVENT_LIMIT);
    let mut instances = BTreeMap::<String, Vec<LifecycleEvent>>::new();
    for event in events {
        instances
            .entry(event.actor.actor_id.clone())
            .or_default()
            .push(event);
    }
    let mut periods = Vec::new();
    for (actor_id, mut events) in instances {
        events.sort_by_key(|event| (event.report.owner_epoch, event.report.sequence));
        events.dedup_by_key(|event| (event.report.owner_epoch, event.report.sequence));
        let mut latest_time = 0;
        events.retain(|event| {
            let ordered = event.report.occurred_at_ms >= latest_time;
            latest_time = latest_time.max(event.report.occurred_at_ms);
            ordered
        });
        for (index, event) in events.iter().enumerate() {
            let start = event.report.occurred_at_ms.max(from);
            let end = events
                .get(index + 1)
                .map_or(to, |next| next.report.occurred_at_ms.min(to));
            let known_until = if event.report.kind == LifecycleKind::Stopped {
                end
            } else {
                event
                    .report
                    .occurred_at_ms
                    .saturating_add(STALE_AFTER_MS)
                    .min(end)
            };
            let status = match (event.report.kind, event.report.resident) {
                (LifecycleKind::Stopped, _) | (_, Some(false)) => ActorResidency::Dormant,
                (_, Some(true)) => ActorResidency::Live,
                _ => ActorResidency::Unknown,
            };
            push(&mut periods, &actor_id, status, start, known_until);
            push(
                &mut periods,
                &actor_id,
                ActorResidency::Unknown,
                start.max(known_until),
                end,
            );
        }
    }
    ResidencyHistory { periods, truncated }
}

fn push(
    periods: &mut Vec<ResidencyPeriod>,
    actor_id: &str,
    status: ActorResidency,
    from_ms: u64,
    to_ms: u64,
) {
    if from_ms >= to_ms {
        return;
    }
    if let Some(previous) = periods.last_mut()
        && previous.actor_id == actor_id
        && previous.status == status
        && previous.to_ms == from_ms
    {
        previous.to_ms = to_ms;
    } else {
        periods.push(ResidencyPeriod {
            actor_id: actor_id.to_owned(),
            status,
            from_ms,
            to_ms,
        });
    }
}
