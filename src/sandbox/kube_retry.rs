use std::{future::Future, time::Duration};

use backon::{ExponentialBuilder, Retryable};

// Priority and fairness rejects with 429 before processing, so even creates are safe to resend.
pub(super) async fn retry_throttled<T, F>(request: impl FnMut() -> F) -> kube::Result<T>
where
    F: Future<Output = kube::Result<T>>,
{
    request
        .retry(
            ExponentialBuilder::new()
                .with_jitter()
                .with_min_delay(Duration::from_millis(200))
                .with_max_delay(Duration::from_secs(5))
                .with_max_times(8),
        )
        .when(|error| matches!(error, kube::Error::Api(response) if response.code == 429))
        .notify(|_, delay| tracing::warn!(?delay, "kubernetes request throttled; retrying"))
        .await
}
