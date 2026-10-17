use super::*;

#[test]
fn executor_selection_matches_artifacts_and_prewarms_the_image_language() -> Result<()> {
    assert_eq!(
        executor_runtime(None, Some("python"))?,
        ActorRuntime::Python
    );
    assert_eq!(
        executor_runtime(None, Some("typescript"))?,
        ActorRuntime::Typescript
    );
    assert_eq!(
        executor_runtime(Some("/customer/actors.pyz"), None)?,
        ActorRuntime::Python
    );
    assert_eq!(
        executor_runtime(Some("/customer/actors.mjs"), None)?,
        ActorRuntime::Typescript
    );
    assert!(executor_runtime(Some("actors.pyz"), Some("typescript")).is_err());
    assert!(executor_runtime(Some("actors.mjs"), Some("python")).is_err());
    assert!(executor_runtime(None, Some("unknown")).is_err());
    Ok(())
}
