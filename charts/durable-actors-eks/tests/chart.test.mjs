import assert from "node:assert/strict"
import { spawnSync } from "node:child_process"
import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { fileURLToPath } from "node:url"
import test from "node:test"

const chart = fileURLToPath(new URL("../", import.meta.url))
const helm = process.env.HELM ?? "helm"
function render(overrides = {}) {
    const directory = mkdtempSync(join(tmpdir(), "actors-eks-chart-"))
    try {
        const values = join(directory, "values.json")
        writeFileSync(values, JSON.stringify(overrides))
        return spawnSync(helm, ["template", "actors", chart, "--namespace", "actors", "-f", `${chart}tests/values.yaml`, "-f", values], { encoding: "utf8" })
    } finally {
        rmSync(directory, { recursive: true })
    }
}

function manifests(overrides) {
    const result = render(overrides)
    assert.equal(result.status, 0, result.stderr)
    return result.stdout
}

test("stores state and code in one S3 bucket with actor-scoped AWS credentials", () => {
    const output = manifests()
    assert.match(output, /DURABLE_ACTORS_STORAGE_PROVIDER, value: s3/)
    assert.match(output, /DURABLE_ACTORS_AWS_REGION, value: "us-west-2"/)
    assert.match(output, /DURABLE_ACTORS_AWS_ACTOR_ROLE_ARN, value: "arn:aws:iam::123456789012:role\/durable-actors-actor"/)
    for (const name of ["BUCKET", "ARTIFACT_BUCKET"]) assert.match(output, new RegExp(`DURABLE_ACTORS_${name}, value: "test-state"`))
    assert.match(output, /DURABLE_ACTORS_GKE_ZONE, value: "us-west-2a"/)
    assert.doesNotMatch(output, /iam\.gke\.io|DURABLE_ACTORS_RAPID|DURABLE_ACTORS_USAGE_TOPIC/)
})

test("isolates sandboxes in a release-specific namespace with constrained credentials and network access", () => {
    const output = manifests()
    assert.match(output, /namespace: actors-actors/)
    assert.match(output, /automountServiceAccountToken: false/)
    assert.match(output, /kind: NetworkPolicy/)
    assert.match(output, /169\.254\.0\.0\/16/)
    assert.match(output, /runAsNonRoot: true/)
})

for (const [name, overrides] of [
    ["GKE zone", { placement: { zone: "us-west4-a" } }],
    ["missing actor role", { storage: { actorRoleArn: "" } }],
    ["mutable control-plane image", { images: { controlPlane: { digest: "latest" } } }],
    ["insecure public URL", { publicUrl: "http://actors.example.com" }],
    ["shared trust namespace", { sandboxNamespace: "actors" }],
    ["several replicas without Redis", { replicaCount: 2 }]
]) test(`rejects ${name}`, () => assert.notEqual(render(overrides).status, 0))
