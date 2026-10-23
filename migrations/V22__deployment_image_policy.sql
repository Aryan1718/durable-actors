ALTER TABLE durable_actors_deployment
    ADD COLUMN image_policy TEXT NOT NULL DEFAULT 'platform'
    CHECK (image_policy IN ('platform', 'custom'));
