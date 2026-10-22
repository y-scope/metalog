use metalog_timeutil::epoch_nanos;
use sqlx::MySqlPool;

/// Node registry: manages node liveness and table assignment in the database.
pub struct NodeRegistry {
    db: MySqlPool,
    node_id: String,
}

impl NodeRegistry {
    pub fn new(db: MySqlPool, node_id: &str) -> Self {
        Self {
            db,
            node_id: node_id.to_string(),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Registers this node (UPSERT to _node_registry).
    pub async fn register_node(&self) -> Result<(), sqlx::Error> {
        let now = epoch_nanos();
        sqlx::query(
            "INSERT INTO _node_registry (node_id, last_heartbeat_at, started_at) VALUES (?, ?, ?) \
             ON DUPLICATE KEY UPDATE last_heartbeat_at = VALUES(last_heartbeat_at)",
        )
        .bind(&self.node_id)
        .bind(now)
        .bind(now)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    /// Removes membership after local coordinators have stopped and released ownership.
    /// Heartbeats only update existing rows, so an in-flight heartbeat cannot
    /// recreate membership after graceful shutdown.
    pub async fn unregister_node(&self) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM _node_registry WHERE node_id = ?")
            .bind(&self.node_id)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    /// Sends a heartbeat (UPDATE last_heartbeat_at).
    pub async fn send_heartbeat(&self) -> Result<(), sqlx::Error> {
        let now = epoch_nanos();
        sqlx::query("UPDATE _node_registry SET last_heartbeat_at = ? WHERE node_id = ?")
            .bind(now)
            .bind(&self.node_id)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    /// Renews leases for all tables owned by this node.
    pub async fn renew_leases(&self, ttl_nanos: i64) -> Result<(), sqlx::Error> {
        let now = epoch_nanos();
        let expiry = now.saturating_add(ttl_nanos);
        // An expired lease is no longer ours, even if no peer has reclaimed it yet.
        sqlx::query(
            "UPDATE _table_assignment ta JOIN _table t ON t.table_name = ta.table_name SET \
             ta.lease_expiry = ? WHERE ta.node_id = ? AND ta.lease_expiry > ? AND t.active = TRUE",
        )
        .bind(expiry)
        .bind(&self.node_id)
        .bind(now)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    /// Inactive tables cannot keep ownership or participate in maintenance.
    pub async fn release_inactive_tables(&self) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE _table_assignment ta JOIN _table t ON t.table_name = ta.table_name SET \
             ta.node_id = NULL, ta.node_assigned_at = NULL, ta.lease_expiry = NULL WHERE t.active \
             = FALSE AND ta.node_id IS NOT NULL",
        )
        .execute(&self.db)
        .await?;
        Ok(result.rows_affected())
    }

    /// Claims orphaned tables from dead nodes (heartbeat mode).
    pub async fn claim_orphans_heartbeat(
        &self,
        dead_threshold_nanos: i64,
    ) -> Result<u64, sqlx::Error> {
        let threshold = epoch_nanos() - dead_threshold_nanos;
        let result = sqlx::query(
            "UPDATE _table_assignment ta LEFT JOIN _node_registry nr ON ta.node_id = nr.node_id \
             SET ta.node_id = NULL, ta.node_assigned_at = NULL, ta.lease_expiry = NULL WHERE \
             ta.node_id IS NOT NULL AND (nr.node_id IS NULL OR nr.last_heartbeat_at < ?)",
        )
        .bind(threshold)
        .execute(&self.db)
        .await?;
        Ok(result.rows_affected())
    }

    /// Claims orphaned tables (lease mode — expired leases).
    pub async fn claim_orphans_lease(&self) -> Result<u64, sqlx::Error> {
        let now = epoch_nanos();
        let result = sqlx::query(
            "UPDATE _table_assignment SET node_id = NULL, node_assigned_at = NULL, lease_expiry = \
             NULL WHERE node_id IS NOT NULL AND (lease_expiry IS NULL OR lease_expiry <= ?)",
        )
        .bind(now)
        .execute(&self.db)
        .await?;
        Ok(result.rows_affected())
    }

    /// Gets active, unassigned tables.
    pub async fn get_unassigned_tables(&self) -> Result<Vec<String>, sqlx::Error> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT ta.table_name FROM _table_assignment ta JOIN _table t ON t.table_name = \
             ta.table_name WHERE ta.node_id IS NULL AND t.active = TRUE ORDER BY ta.table_name",
        )
        .fetch_all(&self.db)
        .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    /// Attempts to claim a table (atomic CAS).
    pub async fn claim_table(&self, table_name: &str) -> Result<bool, sqlx::Error> {
        self.claim_table_with_lease(table_name, None).await
    }

    /// Claims an unassigned table, initializing a lease in the same atomic update.
    /// Heartbeat ownership clears any lease left by an earlier owner.
    pub async fn claim_table_with_lease(
        &self,
        table_name: &str,
        lease_ttl_nanos: Option<i64>,
    ) -> Result<bool, sqlx::Error> {
        let now = epoch_nanos();
        let expiry = lease_ttl_nanos.map(|ttl| now.saturating_add(ttl));
        let result = sqlx::query(
            "UPDATE _table_assignment ta JOIN _table t ON t.table_name = ta.table_name SET \
             ta.node_id = ?, ta.node_assigned_at = ?, ta.lease_expiry = ? WHERE ta.table_name = ? \
             AND ta.node_id IS NULL AND t.active = TRUE",
        )
        .bind(&self.node_id)
        .bind(now)
        .bind(expiry)
        .bind(table_name)
        .execute(&self.db)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Gets active tables assigned to this node.
    pub async fn get_my_tables(&self) -> Result<Vec<String>, sqlx::Error> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT ta.table_name FROM _table_assignment ta JOIN _table t ON t.table_name = \
             ta.table_name WHERE ta.node_id = ? AND t.active = TRUE",
        )
        .bind(&self.node_id)
        .fetch_all(&self.db)
        .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    /// Returns only assignments whose leases still authorize this node to work.
    pub async fn get_my_leased_tables(&self) -> Result<Vec<String>, sqlx::Error> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT ta.table_name FROM _table_assignment ta JOIN _table t ON t.table_name = \
             ta.table_name WHERE ta.node_id = ? AND ta.lease_expiry > ? AND t.active = TRUE",
        )
        .bind(&self.node_id)
        .bind(epoch_nanos())
        .fetch_all(&self.db)
        .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    /// Releases a table assignment.
    pub async fn release_table(&self, table_name: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE _table_assignment SET node_id = NULL, node_assigned_at = NULL, lease_expiry = \
             NULL WHERE table_name = ? AND node_id = ?",
        )
        .bind(table_name)
        .bind(&self.node_id)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    /// Counts active nodes (heartbeat mode).
    pub async fn count_active_nodes_heartbeat(
        &self,
        dead_threshold_nanos: i64,
    ) -> Result<i64, sqlx::Error> {
        let threshold = epoch_nanos() - dead_threshold_nanos;
        let (count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM _node_registry WHERE last_heartbeat_at >= ?")
                .bind(threshold)
                .fetch_one(&self.db)
                .await?;
        Ok(count)
    }

    /// Counts active tables assigned to this node.
    pub async fn count_my_tables(&self) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM _table_assignment ta JOIN _table t ON t.table_name = \
             ta.table_name WHERE ta.node_id = ? AND t.active = TRUE",
        )
        .bind(&self.node_id)
        .fetch_one(&self.db)
        .await?;
        Ok(count)
    }

    /// Counts total active, assigned tables.
    pub async fn count_assigned_tables(&self) -> Result<i64, sqlx::Error> {
        let (count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM _table_assignment ta JOIN _table t ON t.table_name = \
             ta.table_name WHERE ta.node_id IS NOT NULL AND t.active = TRUE",
        )
        .fetch_one(&self.db)
        .await?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn node_id() {
        let pool = MySqlPool::connect_lazy("mysql://root@localhost/test").unwrap();
        let reg = NodeRegistry::new(pool, "test-node");
        assert_eq!(reg.node_id(), "test-node");
    }
}
