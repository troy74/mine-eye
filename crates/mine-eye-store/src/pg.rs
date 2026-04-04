use std::collections::HashMap;

use mine_eye_graph::{EdgeRef, GraphSnapshot};
use mine_eye_types::{
    BranchPromotionRecord, BranchPromotionStatus, BranchStatus, CacheState, CrsRecord,
    ExecutionState, GraphBranch, GraphMeta, GraphRevision, LineageMeta, NodeCategory, NodeConfig,
    NodeExecutionPolicy, NodeRecord, OwnerRef, PortBinding, SemanticPortType,
};
use sqlx::PgPool;
use uuid::Uuid;

use crate::StoreError;

fn merge_json_params(base: &mut serde_json::Value, patch: &serde_json::Value) {
    let (serde_json::Value::Object(bm), serde_json::Value::Object(pm)) = (base, patch) else {
        return;
    };
    for (k, v) in pm {
        if let Some(existing) = bm.get(k) {
            if existing.is_object() && v.is_object() {
                let mut inner = existing.clone();
                merge_json_params(&mut inner, v);
                bm.insert(k.clone(), inner);
                continue;
            }
        }
        bm.insert(k.clone(), v.clone());
    }
}

fn cat(s: &str) -> NodeCategory {
    match s {
        "input" => NodeCategory::Input,
        "transform" => NodeCategory::Transform,
        "model" => NodeCategory::Model,
        "qa" => NodeCategory::Qa,
        "visualisation" => NodeCategory::Visualisation,
        "export" => NodeCategory::Export,
        _ => NodeCategory::Transform,
    }
}

fn cat_str(c: NodeCategory) -> &'static str {
    match c {
        NodeCategory::Input => "input",
        NodeCategory::Transform => "transform",
        NodeCategory::Model => "model",
        NodeCategory::Qa => "qa",
        NodeCategory::Visualisation => "visualisation",
        NodeCategory::Export => "export",
    }
}

fn exec(s: &str) -> ExecutionState {
    match s {
        "pending" => ExecutionState::Pending,
        "running" => ExecutionState::Running,
        "failed" => ExecutionState::Failed,
        "succeeded" => ExecutionState::Succeeded,
        _ => ExecutionState::Idle,
    }
}

fn exec_str(e: ExecutionState) -> &'static str {
    match e {
        ExecutionState::Idle => "idle",
        ExecutionState::Pending => "pending",
        ExecutionState::Running => "running",
        ExecutionState::Failed => "failed",
        ExecutionState::Succeeded => "succeeded",
    }
}

fn cache(s: &str) -> CacheState {
    match s {
        "hit" => CacheState::Hit,
        "stale" => CacheState::Stale,
        _ => CacheState::Miss,
    }
}

fn cache_str(c: CacheState) -> &'static str {
    match c {
        CacheState::Miss => "miss",
        CacheState::Hit => "hit",
        CacheState::Stale => "stale",
    }
}

fn sem(s: &str) -> SemanticPortType {
    match s {
        "point_set" | "PointSet" => SemanticPortType::PointSet,
        "interval_set" | "IntervalSet" => SemanticPortType::IntervalSet,
        "trajectory_set" | "TrajectorySet" => SemanticPortType::TrajectorySet,
        "surface" | "Surface" => SemanticPortType::Surface,
        "raster" | "Raster" => SemanticPortType::Raster,
        "mesh" | "Mesh" => SemanticPortType::Mesh,
        "block_model" | "BlockModel" => SemanticPortType::BlockModel,
        "table" | "Table" => SemanticPortType::Table,
        _ => SemanticPortType::Table,
    }
}

fn sem_str(t: SemanticPortType) -> &'static str {
    match t {
        SemanticPortType::PointSet => "point_set",
        SemanticPortType::IntervalSet => "interval_set",
        SemanticPortType::TrajectorySet => "trajectory_set",
        SemanticPortType::Surface => "surface",
        SemanticPortType::Raster => "raster",
        SemanticPortType::Mesh => "mesh",
        SemanticPortType::BlockModel => "block_model",
        SemanticPortType::Table => "table",
    }
}

fn branch_status(s: &str) -> BranchStatus {
    match s {
        "qa" => BranchStatus::Qa,
        "approved" => BranchStatus::Approved,
        "promoted" => BranchStatus::Promoted,
        "archived" => BranchStatus::Archived,
        _ => BranchStatus::Draft,
    }
}

fn branch_status_str(s: BranchStatus) -> &'static str {
    match s {
        BranchStatus::Draft => "draft",
        BranchStatus::Qa => "qa",
        BranchStatus::Approved => "approved",
        BranchStatus::Promoted => "promoted",
        BranchStatus::Archived => "archived",
    }
}

fn promotion_status(s: &str) -> BranchPromotionStatus {
    match s {
        "succeeded" => BranchPromotionStatus::Succeeded,
        "conflict" => BranchPromotionStatus::Conflict,
        "failed" => BranchPromotionStatus::Failed,
        _ => BranchPromotionStatus::Pending,
    }
}

fn promotion_status_str(s: BranchPromotionStatus) -> &'static str {
    match s {
        BranchPromotionStatus::Pending => "pending",
        BranchPromotionStatus::Succeeded => "succeeded",
        BranchPromotionStatus::Conflict => "conflict",
        BranchPromotionStatus::Failed => "failed",
    }
}

pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<(), StoreError> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|e| StoreError::Migrate(e.to_string()))?;
        Ok(())
    }

    pub async fn create_workspace(
        &self,
        name: &str,
        owner: OwnerRef,
        project_crs: Option<serde_json::Value>,
    ) -> Result<Uuid, StoreError> {
        let id = Uuid::new_v4();
        let owner_v = serde_json::to_value(&owner)?;
        sqlx::query(
            r#"INSERT INTO workspaces (id, name, owner, project_crs) VALUES ($1, $2, $3, $4)"#,
        )
        .bind(id)
        .bind(name)
        .bind(owner_v)
        .bind(project_crs)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn update_workspace_project_crs(
        &self,
        workspace_id: Uuid,
        project_crs: serde_json::Value,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE workspaces
            SET project_crs = $2
            WHERE id = $1
            "#,
        )
        .bind(workspace_id)
        .bind(project_crs)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn create_graph(
        &self,
        workspace_id: Uuid,
        name: &str,
        meta: &GraphMeta,
    ) -> Result<Uuid, StoreError> {
        let id = meta.graph_id;
        let meta_v = serde_json::to_value(meta)?;
        sqlx::query(r#"INSERT INTO graphs (id, workspace_id, name, meta) VALUES ($1, $2, $3, $4)"#)
            .bind(id)
            .bind(workspace_id)
            .bind(name)
            .bind(meta_v)
            .execute(&self.pool)
            .await?;
        Ok(id)
    }

    pub async fn create_branch(
        &self,
        graph_id: Uuid,
        name: &str,
        base_revision_id: Option<Uuid>,
        created_by: &str,
        status: BranchStatus,
    ) -> Result<Uuid, StoreError> {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO graph_branches (id, graph_id, name, base_revision_id, head_revision_id, status, created_by)
            VALUES ($1, $2, $3, $4, $4, $5, $6)
            "#,
        )
        .bind(id)
        .bind(graph_id)
        .bind(name)
        .bind(base_revision_id)
        .bind(branch_status_str(status))
        .bind(created_by)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn list_branches(&self, graph_id: Uuid) -> Result<Vec<GraphBranch>, StoreError> {
        let rows: Vec<(
            Uuid,
            Uuid,
            String,
            Option<Uuid>,
            Option<Uuid>,
            String,
            String,
            chrono::DateTime<chrono::Utc>,
            chrono::DateTime<chrono::Utc>,
        )> = sqlx::query_as(
            r#"
            SELECT id, graph_id, name, base_revision_id, head_revision_id, status, created_by, created_at, updated_at
            FROM graph_branches
            WHERE graph_id = $1
            ORDER BY created_at ASC, name ASC
            "#,
        )
        .bind(graph_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    graph_id,
                    name,
                    base_revision_id,
                    head_revision_id,
                    status,
                    created_by,
                    created_at,
                    updated_at,
                )| GraphBranch {
                    id,
                    graph_id,
                    name,
                    base_revision_id,
                    head_revision_id,
                    status: branch_status(&status),
                    created_by,
                    created_at,
                    updated_at,
                },
            )
            .collect())
    }

    pub async fn create_revision(
        &self,
        graph_id: Uuid,
        branch_id: Uuid,
        parent_revision_id: Option<Uuid>,
        created_by: &str,
        meta: serde_json::Value,
    ) -> Result<Uuid, StoreError> {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO graph_revisions (id, graph_id, branch_id, parent_revision_id, created_by, meta)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(id)
        .bind(graph_id)
        .bind(branch_id)
        .bind(parent_revision_id)
        .bind(created_by)
        .bind(meta)
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            UPDATE graph_branches
            SET head_revision_id = $2, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(branch_id)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn list_revisions(
        &self,
        graph_id: Uuid,
        branch_id: Option<Uuid>,
    ) -> Result<Vec<GraphRevision>, StoreError> {
        let rows: Vec<(
            Uuid,
            Uuid,
            Option<Uuid>,
            Option<Uuid>,
            String,
            serde_json::Value,
            chrono::DateTime<chrono::Utc>,
        )> = if let Some(branch_id) = branch_id {
            sqlx::query_as(
                r#"
                SELECT id, graph_id, branch_id, parent_revision_id, created_by, meta, created_at
                FROM graph_revisions
                WHERE graph_id = $1 AND branch_id = $2
                ORDER BY created_at ASC, id ASC
                "#,
            )
            .bind(graph_id)
            .bind(branch_id)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                r#"
                SELECT id, graph_id, branch_id, parent_revision_id, created_by, meta, created_at
                FROM graph_revisions
                WHERE graph_id = $1
                ORDER BY created_at ASC, id ASC
                "#,
            )
            .bind(graph_id)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows
            .into_iter()
            .map(
                |(id, graph_id, branch_id, parent_revision_id, created_by, meta, created_at)| {
                    GraphRevision {
                        id,
                        graph_id,
                        branch_id,
                        parent_revision_id,
                        created_by,
                        meta,
                        created_at,
                    }
                },
            )
            .collect())
    }

    pub async fn record_branch_promotion(
        &self,
        source_branch_id: Uuid,
        target_branch_id: Uuid,
        source_head_revision_id: Option<Uuid>,
        promoted_revision_id: Option<Uuid>,
        status: BranchPromotionStatus,
        conflict_report: Option<serde_json::Value>,
        created_by: &str,
    ) -> Result<Uuid, StoreError> {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO branch_promotions
            (id, source_branch_id, target_branch_id, source_head_revision_id, promoted_revision_id, status, conflict_report, created_by)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(id)
        .bind(source_branch_id)
        .bind(target_branch_id)
        .bind(source_head_revision_id)
        .bind(promoted_revision_id)
        .bind(promotion_status_str(status))
        .bind(conflict_report)
        .bind(created_by)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn list_branch_promotions(
        &self,
        graph_id: Uuid,
    ) -> Result<Vec<BranchPromotionRecord>, StoreError> {
        let rows: Vec<(
            Uuid,
            Uuid,
            Uuid,
            Option<Uuid>,
            Option<Uuid>,
            String,
            Option<serde_json::Value>,
            String,
            chrono::DateTime<chrono::Utc>,
        )> = sqlx::query_as(
            r#"
            SELECT bp.id, bp.source_branch_id, bp.target_branch_id, bp.source_head_revision_id, bp.promoted_revision_id,
                   bp.status, bp.conflict_report, bp.created_by, bp.created_at
            FROM branch_promotions bp
            JOIN graph_branches sb ON sb.id = bp.source_branch_id
            WHERE sb.graph_id = $1
            ORDER BY bp.created_at DESC
            "#,
        )
        .bind(graph_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    source_branch_id,
                    target_branch_id,
                    source_head_revision_id,
                    promoted_revision_id,
                    status,
                    conflict_report,
                    created_by,
                    created_at,
                )| BranchPromotionRecord {
                    id,
                    source_branch_id,
                    target_branch_id,
                    source_head_revision_id,
                    promoted_revision_id,
                    status: promotion_status(&status),
                    conflict_report,
                    created_by,
                    created_at,
                },
            )
            .collect())
    }

    pub async fn upsert_node(&self, node: &NodeRecord) -> Result<(), StoreError> {
        let policy = serde_json::to_value(&node.policy)?;
        let ports = serde_json::to_value(&node.ports)?;
        let lineage = serde_json::to_value(&node.lineage)?;
        let config = serde_json::to_value(&node.config)?;
        sqlx::query(
            r#"
            INSERT INTO nodes (id, graph_id, category, config, execution_state, cache_state, policy, ports, lineage, content_hash, last_error)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (id) DO UPDATE SET
                category = EXCLUDED.category,
                config = EXCLUDED.config,
                execution_state = EXCLUDED.execution_state,
                cache_state = EXCLUDED.cache_state,
                policy = EXCLUDED.policy,
                ports = EXCLUDED.ports,
                lineage = EXCLUDED.lineage,
                content_hash = EXCLUDED.content_hash,
                last_error = EXCLUDED.last_error,
                updated_at = now()
            "#,
        )
        .bind(node.id)
        .bind(node.graph_id)
        .bind(cat_str(node.category))
        .bind(config)
        .bind(exec_str(node.execution))
        .bind(cache_str(node.cache))
        .bind(policy)
        .bind(ports)
        .bind(lineage)
        .bind(&node.content_hash)
        .bind(&node.last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn add_edge(
        &self,
        graph_id: Uuid,
        from_node: Uuid,
        from_port: &str,
        to_node: Uuid,
        to_port: &str,
        semantic_type: SemanticPortType,
    ) -> Result<Uuid, StoreError> {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO edges (id, graph_id, from_node, from_port, to_node, to_port, semantic_type)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(id)
        .bind(graph_id)
        .bind(from_node)
        .bind(from_port)
        .bind(to_node)
        .bind(to_port)
        .bind(sem_str(semantic_type))
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn delete_edge(&self, graph_id: Uuid, edge_id: Uuid) -> Result<(), StoreError> {
        let r = sqlx::query(r#"DELETE FROM edges WHERE id = $1 AND graph_id = $2"#)
            .bind(edge_id)
            .bind(graph_id)
            .execute(&self.pool)
            .await?;
        if r.rows_affected() == 0 {
            return Err(StoreError::NotFound(edge_id.to_string()));
        }
        Ok(())
    }

    pub async fn delete_node(&self, graph_id: Uuid, node_id: Uuid) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;
        // Remove all inbound/outbound edges first so no dangling topology can keep stale
        // upstream artifacts in execution input resolution.
        sqlx::query(
            r#"DELETE FROM edges WHERE graph_id = $1 AND (from_node = $2 OR to_node = $2)"#,
        )
        .bind(graph_id)
        .bind(node_id)
        .execute(&mut *tx)
        .await?;
        // Remove current artifact pointers for the deleted node.
        sqlx::query(r#"DELETE FROM node_artifacts WHERE node_id = $1"#)
            .bind(node_id)
            .execute(&mut *tx)
            .await?;
        let r = sqlx::query(r#"DELETE FROM nodes WHERE id = $1 AND graph_id = $2"#)
            .bind(node_id)
            .bind(graph_id)
            .execute(&mut *tx)
            .await?;
        if r.rows_affected() == 0 {
            return Err(StoreError::NotFound(node_id.to_string()));
        }
        tx.commit().await?;
        Ok(())
    }

    /// Replace the graph's structural definition (nodes + edges) with the provided snapshot.
    /// This keeps graph metadata and artifact history, but rewrites current topology/config.
    pub async fn replace_graph_definition(
        &self,
        graph_id: Uuid,
        nodes: &[NodeRecord],
        edges: &[EdgeRef],
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(r#"DELETE FROM edges WHERE graph_id = $1"#)
            .bind(graph_id)
            .execute(&mut *tx)
            .await?;

        for node in nodes {
            let policy = serde_json::to_value(&node.policy)?;
            let ports = serde_json::to_value(&node.ports)?;
            let lineage = serde_json::to_value(&node.lineage)?;
            let config = serde_json::to_value(&node.config)?;
            sqlx::query(
                r#"
                INSERT INTO nodes (id, graph_id, category, config, execution_state, cache_state, policy, ports, lineage, content_hash, last_error)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (id) DO UPDATE SET
                    graph_id = EXCLUDED.graph_id,
                    category = EXCLUDED.category,
                    config = EXCLUDED.config,
                    execution_state = EXCLUDED.execution_state,
                    cache_state = EXCLUDED.cache_state,
                    policy = EXCLUDED.policy,
                    ports = EXCLUDED.ports,
                    lineage = EXCLUDED.lineage,
                    content_hash = EXCLUDED.content_hash,
                    last_error = EXCLUDED.last_error,
                    updated_at = now()
                "#,
            )
            .bind(node.id)
            .bind(graph_id)
            .bind(cat_str(node.category))
            .bind(config)
            .bind(exec_str(node.execution))
            .bind(cache_str(node.cache))
            .bind(policy)
            .bind(ports)
            .bind(lineage)
            .bind(&node.content_hash)
            .bind(&node.last_error)
            .execute(&mut *tx)
            .await?;
        }

        // Remove nodes no longer present in the replacement snapshot.
        // This preserves artifacts for unchanged node IDs instead of dropping all artifacts on checkout.
        let node_ids: Vec<Uuid> = nodes.iter().map(|n| n.id).collect();
        if node_ids.is_empty() {
            sqlx::query(r#"DELETE FROM nodes WHERE graph_id = $1"#)
                .bind(graph_id)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query(r#"DELETE FROM nodes WHERE graph_id = $1 AND NOT (id = ANY($2))"#)
                .bind(graph_id)
                .bind(&node_ids)
                .execute(&mut *tx)
                .await?;
        }

        for edge in edges {
            sqlx::query(
                r#"
                INSERT INTO edges (id, graph_id, from_node, from_port, to_node, to_port, semantic_type)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
            )
            .bind(edge.id)
            .bind(graph_id)
            .bind(edge.from_node)
            .bind(&edge.from_port)
            .bind(edge.to_node)
            .bind(&edge.to_port)
            .bind(sem_str(edge.semantic_type))
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn load_graph(&self, graph_id: Uuid) -> Result<GraphSnapshot, StoreError> {
        let rows: Vec<(
            Uuid,
            String,
            serde_json::Value,
            String,
            String,
            serde_json::Value,
            serde_json::Value,
            serde_json::Value,
            Option<String>,
            Option<String>,
        )> = sqlx::query_as(
            r#"
            SELECT id, category, config, execution_state, cache_state, policy, ports, lineage, content_hash, last_error
            FROM nodes WHERE graph_id = $1
            "#,
        )
        .bind(graph_id)
        .fetch_all(&self.pool)
        .await?;

        let mut nodes = HashMap::new();
        for (
            id,
            category,
            config,
            exec_s,
            cache_s,
            policy_v,
            ports_v,
            lineage_v,
            content_hash,
            last_error,
        ) in rows
        {
            let config: NodeConfig = serde_json::from_value(config)?;
            let policy: NodeExecutionPolicy = serde_json::from_value(policy_v)?;
            let ports: Vec<PortBinding> = serde_json::from_value(ports_v)?;
            let lineage: LineageMeta = serde_json::from_value(lineage_v)?;
            let record = NodeRecord {
                id,
                graph_id,
                category: cat(&category),
                config,
                execution: exec(&exec_s),
                cache: cache(&cache_s),
                policy,
                ports,
                lineage,
                content_hash,
                last_error,
            };
            nodes.insert(id, record);
        }

        let edge_rows: Vec<(Uuid, Uuid, String, Uuid, String, String)> = sqlx::query_as(
            r#"
            SELECT id, from_node, from_port, to_node, to_port, semantic_type
            FROM edges WHERE graph_id = $1
            "#,
        )
        .bind(graph_id)
        .fetch_all(&self.pool)
        .await?;

        let mut edges = Vec::new();
        for (id, from_node, from_port, to_node, to_port, sem_t) in edge_rows {
            if !nodes.contains_key(&from_node) || !nodes.contains_key(&to_node) {
                continue;
            }
            edges.push(EdgeRef {
                id,
                from_node,
                from_port,
                to_node,
                to_port,
                semantic_type: sem(&sem_t),
            });
        }

        Ok(GraphSnapshot {
            graph_id,
            nodes,
            edges,
        })
    }

    /// Workspace id and optional working CRS for a graph (join `graphs` → `workspaces`).
    pub async fn graph_workspace_meta(
        &self,
        graph_id: Uuid,
    ) -> Result<Option<(Uuid, Option<CrsRecord>)>, StoreError> {
        let row: Option<(Uuid, Option<serde_json::Value>)> = sqlx::query_as(
            r#"
            SELECT g.workspace_id, w.project_crs
            FROM graphs g
            JOIN workspaces w ON w.id = g.workspace_id
            WHERE g.id = $1
            "#,
        )
        .bind(graph_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some((workspace_id, crs_json)) = row else {
            return Ok(None);
        };
        let project_crs = match crs_json {
            Some(v) => Some(serde_json::from_value(v)?),
            None => None,
        };
        Ok(Some((workspace_id, project_crs)))
    }

    /// Deep-merge `params_patch` into `node.config.params`, optionally replace policy.
    /// Marks cache stale.
    pub async fn patch_node_config(
        &self,
        graph_id: Uuid,
        node_id: Uuid,
        params_patch: serde_json::Value,
        policy_patch: Option<NodeExecutionPolicy>,
    ) -> Result<NodeRecord, StoreError> {
        let snap = self.load_graph(graph_id).await?;
        let Some(mut node) = snap.nodes.get(&node_id).cloned() else {
            return Err(StoreError::NotFound(node_id.to_string()));
        };
        if node.graph_id != graph_id {
            return Err(StoreError::NotFound(node_id.to_string()));
        }
        merge_json_params(&mut node.config.params, &params_patch);
        if let Some(policy) = policy_patch {
            node.policy = policy;
        }
        node.cache = CacheState::Stale;
        self.upsert_node(&node).await?;
        Ok(node)
    }

    pub async fn update_node_execution(
        &self,
        node_id: Uuid,
        execution: ExecutionState,
        cache: CacheState,
        content_hash: Option<&str>,
        last_error: Option<&str>,
    ) -> Result<(), StoreError> {
        sqlx::query(
            r#"
            UPDATE nodes SET execution_state = $2, cache_state = $3, content_hash = COALESCE($4, content_hash), last_error = $5, updated_at = now()
            WHERE id = $1
            "#,
        )
        .bind(node_id)
        .bind(exec_str(execution))
        .bind(cache_str(cache))
        .bind(content_hash)
        .bind(last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn replace_node_artifacts(
        &self,
        node_id: Uuid,
        artifacts: &[(String, String, Option<String>)],
    ) -> Result<(), StoreError> {
        for (key, hash, media) in artifacts {
            let prev: Option<(Uuid, String, Option<String>)> = sqlx::query_as(
                r#"
                SELECT id, content_hash, media_type
                FROM node_artifacts
                WHERE node_id = $1 AND artifact_key = $2
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                "#,
            )
            .bind(node_id)
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
            if let Some((_, prev_hash, prev_media)) = &prev {
                if prev_hash == hash && prev_media == media {
                    continue;
                }
            }
            let aid = Uuid::new_v4();
            sqlx::query(
                r#"
                INSERT INTO node_artifacts (
                    id, node_id, artifact_key, content_hash, media_type,
                    variant, payload_hash, supersedes_artifact_id
                )
                VALUES ($1, $2, $3, $4, $5, 'preview', $4, $6)
                "#,
            )
            .bind(aid)
            .bind(node_id)
            .bind(key)
            .bind(hash)
            .bind(media)
            .bind(prev.map(|(id, _, _)| id))
            .execute(&self.pool)
            .await?;

            // Keep latest + penultimate per node/key for rollback.
            sqlx::query(
                r#"
                DELETE FROM node_artifacts
                WHERE id IN (
                    SELECT id FROM (
                        SELECT id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY node_id, artifact_key
                                   ORDER BY created_at DESC, id DESC
                               ) AS rn
                        FROM node_artifacts
                        WHERE node_id = $1 AND artifact_key = $2
                    ) z
                    WHERE z.rn > 2
                )
                "#,
            )
            .bind(node_id)
            .bind(key)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    pub async fn list_artifacts_for_node(
        &self,
        node_id: Uuid,
    ) -> Result<Vec<(String, String, Option<String>)>, StoreError> {
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
            r#"
            SELECT DISTINCT ON (artifact_key) artifact_key, content_hash, media_type
            FROM node_artifacts
            WHERE node_id = $1
            ORDER BY artifact_key, created_at DESC, id DESC
            "#,
        )
        .bind(node_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Resolve upstream artifact refs for `node_id` from the graph edges + `node_artifacts` **now**.
    /// Jobs enqueued before upstream finished often have stale empty `input_artifact_refs`; workers
    /// should call this at execution time.
    pub async fn resolve_input_artifact_refs(
        &self,
        graph_id: Uuid,
        node_id: Uuid,
    ) -> Result<Vec<mine_eye_types::ArtifactRef>, StoreError> {
        let snap = self.load_graph(graph_id).await?;
        let mut refs: Vec<mine_eye_types::ArtifactRef> = Vec::new();
        for edge in &snap.edges {
            if edge.to_node != node_id {
                continue;
            }
            let rows = self.list_artifacts_for_node(edge.from_node).await?;
            for (key, content_hash, media_type) in rows {
                refs.push(mine_eye_types::ArtifactRef {
                    key,
                    content_hash,
                    media_type,
                });
            }
        }
        refs.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(refs)
    }

    pub async fn insert_ai_suggestion(
        &self,
        graph_id: Uuid,
        kind: &str,
        payload: serde_json::Value,
    ) -> Result<Uuid, StoreError> {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO ai_suggestions (id, graph_id, kind, payload, status)
            VALUES ($1, $2, $3, $4, 'pending')
            "#,
        )
        .bind(id)
        .bind(graph_id)
        .bind(kind)
        .bind(payload)
        .execute(&self.pool)
        .await?;
        Ok(id)
    }

    pub async fn confirm_ai_suggestion(
        &self,
        suggestion_id: Uuid,
        user_id: &str,
    ) -> Result<(), StoreError> {
        let r = sqlx::query(
            r#"
            UPDATE ai_suggestions SET status = 'confirmed', confirmed_by = $2, confirmed_at = now()
            WHERE id = $1 AND status = 'pending'
            "#,
        )
        .bind(suggestion_id)
        .bind(user_id)
        .execute(&self.pool)
        .await?;
        if r.rows_affected() == 0 {
            return Err(StoreError::NotFound(suggestion_id.to_string()));
        }
        Ok(())
    }

    pub async fn list_ai_suggestions(
        &self,
        graph_id: Uuid,
    ) -> Result<Vec<(Uuid, String, serde_json::Value, String)>, StoreError> {
        let rows: Vec<(Uuid, String, serde_json::Value, String)> = sqlx::query_as(
            r#"SELECT id, kind, payload, status FROM ai_suggestions WHERE graph_id = $1 ORDER BY created_at DESC"#,
        )
        .bind(graph_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
}
