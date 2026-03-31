-- Graph + metadata (PostGIS-ready: add geometry columns in a later migration)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE workspaces (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    owner JSONB NOT NULL DEFAULT '{}',
    project_crs JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE graphs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    meta JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE nodes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    category TEXT NOT NULL,
    config JSONB NOT NULL,
    execution_state TEXT NOT NULL DEFAULT 'idle',
    cache_state TEXT NOT NULL DEFAULT 'miss',
    policy JSONB NOT NULL DEFAULT '{}',
    ports JSONB NOT NULL DEFAULT '[]',
    lineage JSONB NOT NULL DEFAULT '{}',
    content_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_nodes_graph ON nodes(graph_id);

CREATE TABLE edges (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    from_node UUID NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    from_port TEXT NOT NULL,
    to_node UUID NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    to_port TEXT NOT NULL,
    semantic_type TEXT NOT NULL
);

CREATE INDEX idx_edges_graph ON edges(graph_id);
CREATE INDEX idx_edges_to ON edges(to_node);

CREATE TABLE job_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_id UUID NOT NULL,
    node_id UUID NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    error_message TEXT,
    result JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE INDEX idx_job_queue_status ON job_queue(status) WHERE status = 'queued';

CREATE TABLE node_artifacts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    node_id UUID NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    artifact_key TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    media_type TEXT,
    is_preview BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_node_artifacts_node ON node_artifacts(node_id);

CREATE TABLE ai_suggestions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    confirmed_by TEXT,
    confirmed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_ai_suggestions_graph ON ai_suggestions(graph_id);
