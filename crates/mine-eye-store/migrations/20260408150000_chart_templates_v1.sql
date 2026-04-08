CREATE TABLE IF NOT EXISTS chart_templates (
    id UUID PRIMARY KEY,
    organization_id TEXT NOT NULL,
    key TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    template_schema JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (organization_id, key)
);

CREATE INDEX IF NOT EXISTS idx_chart_templates_org ON chart_templates (organization_id);

