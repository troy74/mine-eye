ALTER TABLE workspaces
    ADD COLUMN IF NOT EXISTS cache_settings JSONB NOT NULL DEFAULT '{}'::jsonb;

