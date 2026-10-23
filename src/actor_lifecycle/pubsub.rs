use anyhow::{Result, ensure};
use async_trait::async_trait;
use google_cloud_pubsub::{client::Publisher, model::Message};

use super::{UsagePublisher, UsageUpdate};

pub(super) struct PubSubPublisher(Publisher);

impl PubSubPublisher {
    pub(super) async fn connect(topic: &str) -> Result<Self> {
        ensure!(
            matches!(topic.split('/').collect::<Vec<_>>().as_slice(),
            ["projects", project, "topics", name] if !project.is_empty() && !name.is_empty()),
            "usage topic must be projects/<project>/topics/<topic>"
        );
        Ok(Self(Publisher::builder(topic).build().await?))
    }
}

#[async_trait]
impl UsagePublisher for PubSubPublisher {
    async fn publish(&self, event: &UsageUpdate) -> Result<()> {
        let mut attributes = vec![
            ("projectId", event.event.actor.project_id.as_str()),
            ("eventId", event.event.event_id.as_str()),
        ];
        if let Some(organization) = &event.event.organization_id {
            attributes.push(("organizationId", organization));
        }
        self.0
            .publish(
                Message::new()
                    .set_data(serde_json::to_vec(event)?)
                    .set_attributes(attributes),
            )
            .await?;
        Ok(())
    }
}
