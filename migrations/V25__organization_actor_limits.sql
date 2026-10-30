-- Ownership cannot be inferred: fail before changing anything so an operator assigns it explicitly.
DO $$
DECLARE
    projects TEXT;
BEGIN
    SELECT string_agg(project_id, ', ' ORDER BY project_id) INTO projects
    FROM durable_actors_deployment
    WHERE organization_id IS NULL OR organization_id !~ '^[A-Za-z0-9._-]{1,64}$';
    IF projects IS NOT NULL THEN
        RAISE EXCEPTION 'deployments without a valid organization_id: %', projects
            USING HINT = 'Redeploy each project with organizationId, then restart. Redeploying replaces its hosts; editing the row would leave running hosts with credentials that lack an organization.';
    END IF;
    SELECT string_agg(l.project_id, ', ' ORDER BY l.project_id) INTO projects
    FROM durable_actors_project_limits l
    LEFT JOIN durable_actors_deployment d ON d.project_id = l.project_id
    WHERE d.project_id IS NULL;
    IF projects IS NOT NULL THEN
        RAISE EXCEPTION 'project limit overrides without a deployment: %', projects
            USING HINT = 'Re-create each override in durable_actors_organization_limits after this migration, or delete it, then restart.';
    END IF;
END $$;

ALTER TABLE durable_actors_deployment
    ALTER COLUMN organization_id SET NOT NULL,
    ADD CONSTRAINT durable_actors_deployment_organization_id_check
        CHECK (organization_id ~ '^[A-Za-z0-9._-]{1,64}$');

CREATE TABLE durable_actors_organization_limits (
    organization_id TEXT PRIMARY KEY,
    max_active_actors BIGINT CHECK (max_active_actors BETWEEN 1 AND 4294967295),
    actor_starts_per_second BIGINT CHECK (actor_starts_per_second BETWEEN 1 AND 4294967295),
    actor_start_burst BIGINT CHECK (actor_start_burst BETWEEN 1 AND 4294967295)
);

-- An organization inherits the most generous override among its projects.
INSERT INTO durable_actors_organization_limits
    (organization_id, max_active_actors, actor_starts_per_second, actor_start_burst)
SELECT d.organization_id, MAX(l.max_active_actors), MAX(l.actor_starts_per_second), MAX(l.actor_start_burst)
FROM durable_actors_project_limits l
JOIN durable_actors_deployment d ON d.project_id = l.project_id
GROUP BY d.organization_id;

DROP TABLE durable_actors_project_limits;
