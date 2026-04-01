-- Branch/revision/promotion model + artifact version metadata (Geo-Scry V1 baseline)

CREATE TABLE IF NOT EXISTS graph_revisions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    branch_id UUID,
    parent_revision_id UUID REFERENCES graph_revisions(id) ON DELETE SET NULL,
    created_by TEXT NOT NULL DEFAULT 'system',
    meta JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_graph_revisions_graph ON graph_revisions(graph_id);
CREATE INDEX IF NOT EXISTS idx_graph_revisions_branch ON graph_revisions(branch_id);

CREATE TABLE IF NOT EXISTS graph_branches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    base_revision_id UUID REFERENCES graph_revisions(id) ON DELETE SET NULL,
    head_revision_id UUID REFERENCES graph_revisions(id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'qa', 'approved', 'promoted', 'archived')),
    created_by TEXT NOT NULL DEFAULT 'system',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_graph_branches_graph_name ON graph_branches(graph_id, name);
CREATE INDEX IF NOT EXISTS idx_graph_branches_graph ON graph_branches(graph_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE constraint_name = 'graph_revisions_branch_fk'
          AND table_name = 'graph_revisions'
    ) THEN
        ALTER TABLE graph_revisions
        ADD CONSTRAINT graph_revisions_branch_fk
        FOREIGN KEY (branch_id) REFERENCES graph_branches(id) ON DELETE SET NULL;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS branch_promotions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_branch_id UUID NOT NULL REFERENCES graph_branches(id) ON DELETE CASCADE,
    target_branch_id UUID NOT NULL REFERENCES graph_branches(id) ON DELETE CASCADE,
    source_head_revision_id UUID REFERENCES graph_revisions(id) ON DELETE SET NULL,
    promoted_revision_id UUID REFERENCES graph_revisions(id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'succeeded', 'conflict', 'failed')),
    conflict_report JSONB,
    created_by TEXT NOT NULL DEFAULT 'system',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_branch_promotions_source ON branch_promotions(source_branch_id);
CREATE INDEX IF NOT EXISTS idx_branch_promotions_target ON branch_promotions(target_branch_id);

ALTER TABLE node_artifacts
    ADD COLUMN IF NOT EXISTS variant TEXT NOT NULL DEFAULT 'preview' CHECK (variant IN ('preview', 'final')),
    ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS manifest_version INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS lineage_hash TEXT,
    ADD COLUMN IF NOT EXISTS payload_hash TEXT,
    ADD COLUMN IF NOT EXISTS supersedes_artifact_id UUID REFERENCES node_artifacts(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_node_artifacts_variant ON node_artifacts(variant);

-- Seed a default main branch + initial revision for existing graphs.
WITH initial_revision AS (
    INSERT INTO graph_revisions (graph_id, parent_revision_id, created_by, meta)
    SELECT g.id, NULL, 'migration', jsonb_build_object('seed', 'initial_main_revision')
    FROM graphs g
    WHERE NOT EXISTS (SELECT 1 FROM graph_revisions r WHERE r.graph_id = g.id)
    RETURNING id, graph_id
),
seed_graph AS (
    SELECT
        g.id AS graph_id,
        COALESCE(
            (SELECT ir.id FROM initial_revision ir WHERE ir.graph_id = g.id),
            (SELECT r.id FROM graph_revisions r WHERE r.graph_id = g.id ORDER BY r.created_at ASC, r.id ASC LIMIT 1)
        ) AS revision_id
    FROM graphs g
)
INSERT INTO graph_branches (graph_id, name, base_revision_id, head_revision_id, status, created_by)
SELECT sg.graph_id, 'main', sg.revision_id, sg.revision_id, 'promoted', 'migration'
FROM seed_graph sg
WHERE sg.revision_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM graph_branches b WHERE b.graph_id = sg.graph_id AND b.name = 'main'
  );

UPDATE graph_revisions r
SET branch_id = b.id
FROM graph_branches b
WHERE r.graph_id = b.graph_id
  AND b.name = 'main'
  AND r.branch_id IS NULL;
