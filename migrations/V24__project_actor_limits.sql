CREATE TABLE durable_actors_project_limits (
    project_id TEXT PRIMARY KEY,
    max_active_actors BIGINT CHECK (max_active_actors BETWEEN 1 AND 4294967295),
    actor_starts_per_second BIGINT CHECK (actor_starts_per_second BETWEEN 1 AND 4294967295),
    actor_start_burst BIGINT CHECK (actor_start_burst BETWEEN 1 AND 4294967295)
);
