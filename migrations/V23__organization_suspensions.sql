CREATE TABLE durable_actors_organization_suspensions (
    organization_id TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'admin' CHECK (source IN ('admin', 'billing')),
    PRIMARY KEY (organization_id, source)
);
