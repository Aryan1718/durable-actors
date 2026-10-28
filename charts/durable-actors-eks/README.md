# Durable Actors on AWS

Run Durable Actors on EKS with one Standard S3 bucket and PostgreSQL.

```yaml
publicUrl: https://actors.example.com
placement:
  zone: us-west-2a
storage:
  bucket: my-actor-state
  region: us-west-2
  actorRoleArn: arn:aws:iam::123456789012:role/durable-actors-actor
```

The cluster must provide:

- A node group for actors running gVisor: nodes labelled and tainted `sandbox.gke.io/runtime=gvisor`, and a `gvisor` RuntimeClass whose handler runs `runsc`, scheduled onto those nodes.
- IMDSv2 with a hop limit of 1 on actor nodes, and NetworkPolicy enforcement (for example, the VPC CNI's `enableNetworkPolicy`).
- An EKS Pod Identity association for the controller's service account (`<release>-durable-actors`) to a role that can read and write the bucket and assume `storage.actorRoleArn`. Don't associate a role with the sandbox namespace's `sandbox` service account.

`storage.actorRoleArn` must trust the controller role. Its permissions are the most any actor session gets; each session is narrowed to one actor's objects. Add a bucket lifecycle rule that aborts incomplete multipart uploads.

## S3 Express logs

For faster writes, add one or two S3 Express One Zone directory buckets in distinct zones. Each write is acknowledged once it is stored in every listed bucket; `storage.bucket` keeps ownership, code and the permanent archive.

```yaml
storage:
  express:
    buckets:
      - {bucket: actor-logs--use2-az1--x-s3, zone: use2-az1}
      - {bucket: actor-logs--use2-az2--x-s3, zone: use2-az2}
```

The controller role also needs `s3express:CreateSession` on these buckets. Actor hosts never receive Express credentials: they write through short-lived URLs, each signed by the controller for one log object.

Spare pools are not yet supported with S3, so every actor starts cold. Image, ingress, Redis and `credentialsSecret` settings match the [GKE chart](../durable-actors/README.md).
