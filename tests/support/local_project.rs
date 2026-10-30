use std::path::{Path, PathBuf};

use anyhow::Result;

pub fn write_actor(project: &Path, body: &str) -> Result<()> {
    let sdk = Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk");
    std::fs::write(
        project.join("actors.ts"),
        format!(
            "import {{ Actor, Persisted, Ephemeral, type ActorSocket }} from {}; export class Counter extends Actor {{ {body} }}",
            serde_json::to_string(&sdk.join("dist/index.js"))?
        ),
    )?;
    std::fs::write(
        project.join("tsconfig.json"),
        serde_json::to_vec(&serde_json::json!({
            "compilerOptions": {
                "target": "ES2022", "module": "NodeNext", "moduleResolution": "NodeNext",
                "strict": true, "skipLibCheck": true, "types": ["node"],
                "typeRoots": [sdk.join("node_modules/@types")]
            },
            "include": ["actors.ts"]
        }))?,
    )?;
    Ok(())
}

pub fn sdk_host() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk/dist/host.js")
}
