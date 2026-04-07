CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS organizations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_by_user_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS organization_memberships (
    organization_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role TEXT NOT NULL DEFAULT 'member',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (organization_id, user_id)
);

ALTER TABLE workspaces
    ADD COLUMN IF NOT EXISTS organization_id TEXT;

UPDATE workspaces
SET organization_id = COALESCE(
    organization_id,
    'personal:' || COALESCE(
        NULLIF(owner->>'user_id', ''),
        format('legacy-workspace-%s', id::text)
    )
)
WHERE organization_id IS NULL;

INSERT INTO users (id)
SELECT DISTINCT COALESCE(
    NULLIF(owner->>'user_id', ''),
    format('legacy-workspace-%s', id::text)
)
FROM workspaces
ON CONFLICT (id) DO NOTHING;

INSERT INTO organizations (id, name, created_by_user_id)
SELECT DISTINCT
    w.organization_id,
    CASE
        WHEN w.organization_id LIKE 'personal:%' THEN 'Personal workspace'
        ELSE COALESCE(NULLIF(w.name, ''), w.organization_id)
    END,
    COALESCE(
        NULLIF(w.owner->>'user_id', ''),
        format('legacy-workspace-%s', w.id::text)
    )
FROM workspaces w
ON CONFLICT (id) DO NOTHING;

INSERT INTO organization_memberships (organization_id, user_id, role)
SELECT DISTINCT
    w.organization_id,
    COALESCE(
        NULLIF(w.owner->>'user_id', ''),
        format('legacy-workspace-%s', w.id::text)
    ),
    'owner'
FROM workspaces w
ON CONFLICT (organization_id, user_id) DO NOTHING;

ALTER TABLE workspaces
    ALTER COLUMN organization_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE constraint_name = 'workspaces_organization_id_fkey'
          AND table_name = 'workspaces'
    ) THEN
        ALTER TABLE workspaces
        ADD CONSTRAINT workspaces_organization_id_fkey
        FOREIGN KEY (organization_id) REFERENCES organizations(id) ON DELETE RESTRICT;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_workspaces_organization_id ON workspaces(organization_id);
CREATE INDEX IF NOT EXISTS idx_organization_memberships_user_id ON organization_memberships(user_id);

UPDATE graphs g
SET meta = jsonb_set(
    jsonb_set(
        COALESCE(g.meta, '{}'::jsonb),
        '{organization_id}',
        to_jsonb(COALESCE(NULLIF(g.meta->>'organization_id', ''), w.organization_id)),
        true
    ),
    '{created_by_user_id}',
    to_jsonb(
        COALESCE(
            NULLIF(g.meta->>'created_by_user_id', ''),
            NULLIF(g.meta->'owner'->>'user_id', ''),
            NULLIF(w.owner->>'user_id', ''),
            format('legacy-workspace-%s', w.id::text)
        )
    ),
    true
)
FROM workspaces w
WHERE w.id = g.workspace_id;
