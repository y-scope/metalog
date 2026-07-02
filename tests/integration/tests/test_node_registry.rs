#[cfg(test)]
mod tests {
    use metalog_ha::NodeRegistry;
    use metalog_it::helpers::setup_db_with_table;
    use metalog_timeutil::epoch_nanos;

    #[tokio::test]
    async fn register_and_heartbeat() {
        let (pool, _container) = setup_db_with_table("test_hb").await;
        let reg = NodeRegistry::new(pool.clone(), "node-a");

        reg.register_node().await.unwrap();
        reg.send_heartbeat().await.unwrap();

        // Verify heartbeat timestamp is recent.
        let (ts,): (i64,) =
            sqlx::query_as("SELECT last_heartbeat_at FROM _node_registry WHERE node_id = 'node-a'")
                .fetch_one(&pool)
                .await
                .unwrap();
        let now = epoch_nanos();
        assert!(now - ts < 5_000_000_000, "heartbeat should be within last 5s");
    }

    #[tokio::test]
    async fn claim_table_lifecycle() {
        let (pool, _container) = setup_db_with_table("test_claim").await;
        let reg = NodeRegistry::new(pool.clone(), "node-a");

        // Claim.
        assert!(reg.claim_table("test_claim").await.unwrap());

        // Verify ownership.
        let my = reg.get_my_tables().await.unwrap();
        assert!(my.contains(&"test_claim".to_string()));
        assert_eq!(reg.count_my_tables().await.unwrap(), 1);
        assert_eq!(reg.count_assigned_tables().await.unwrap(), 1);

        // Release.
        reg.release_table("test_claim").await.unwrap();
        let my = reg.get_my_tables().await.unwrap();
        assert!(my.is_empty());

        // Re-claim by another node.
        let reg_b = NodeRegistry::new(pool.clone(), "node-b");
        reg_b.register_node().await.unwrap();
        assert!(reg_b.claim_table("test_claim").await.unwrap());
        assert_eq!(reg_b.count_my_tables().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn claim_table_cas_prevents_double_claim() {
        let (pool, _container) = setup_db_with_table("test_cas").await;
        let reg_a = NodeRegistry::new(pool.clone(), "node-a");
        let reg_b = NodeRegistry::new(pool.clone(), "node-b");
        reg_a.register_node().await.unwrap();
        reg_b.register_node().await.unwrap();

        // First claim succeeds.
        assert!(reg_a.claim_table("test_cas").await.unwrap());
        // Second claim fails (atomic CAS).
        assert!(!reg_b.claim_table("test_cas").await.unwrap());
    }

    #[tokio::test]
    async fn orphan_claiming_heartbeat_mode() {
        let (pool, _container) = setup_db_with_table("test_orphan_hb").await;
        let reg_a = NodeRegistry::new(pool.clone(), "node-a");
        let reg_b = NodeRegistry::new(pool.clone(), "node-b");

        // Both nodes register and heartbeat.
        reg_a.register_node().await.unwrap();
        reg_b.register_node().await.unwrap();
        reg_a.send_heartbeat().await.unwrap();
        reg_b.send_heartbeat().await.unwrap();

        // Node-a claims the table.
        assert!(reg_a.claim_table("test_orphan_hb").await.unwrap());

        // Stale out node-a by setting heartbeat to 1 hour ago.
        let one_hour_ago = epoch_nanos() - 3_600_000_000_000;
        sqlx::query("UPDATE _node_registry SET last_heartbeat_at = ? WHERE node_id = 'node-a'")
            .bind(one_hour_ago)
            .execute(&pool)
            .await
            .unwrap();

        // Node-b claims orphans (30-minute threshold).
        let threshold = 1_800_000_000_000i64;
        let orphaned = reg_b.claim_orphans_heartbeat(threshold).await.unwrap();
        assert_eq!(orphaned, 1, "one table should be orphaned");

        // Table is now unassigned — node-b can claim it.
        let unassigned = reg_b.get_unassigned_tables().await.unwrap();
        assert!(unassigned.contains(&"test_orphan_hb".to_string()));
        assert!(reg_b.claim_table("test_orphan_hb").await.unwrap());
    }

    #[tokio::test]
    async fn orphan_claiming_lease_mode() {
        let (pool, _container) = setup_db_with_table("test_orphan_lease").await;
        let reg_a = NodeRegistry::new(pool.clone(), "node-a");

        reg_a.register_node().await.unwrap();
        reg_a.claim_table("test_orphan_lease").await.unwrap();

        // Renew lease with a very short TTL (1 second in nanos).
        let short_ttl = 1_000_000_000i64;
        reg_a.renew_leases(short_ttl).await.unwrap();

        // Wait for lease to expire.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Claim orphans by lease.
        let orphaned = reg_a.claim_orphans_lease().await.unwrap();
        assert_eq!(orphaned, 1, "expired lease should release table");

        // Table should now be unassigned.
        let unassigned = reg_a.get_unassigned_tables().await.unwrap();
        assert!(unassigned.contains(&"test_orphan_lease".to_string()));
    }

    #[tokio::test]
    async fn count_active_nodes_and_tables() {
        let (pool, _container) = setup_db_with_table("test_count").await;
        let reg_a = NodeRegistry::new(pool.clone(), "node-a");
        let reg_b = NodeRegistry::new(pool.clone(), "node-b");

        reg_a.register_node().await.unwrap();
        reg_b.register_node().await.unwrap();
        reg_a.send_heartbeat().await.unwrap();
        reg_b.send_heartbeat().await.unwrap();

        let threshold = 1_800_000_000_000i64;
        assert_eq!(
            reg_a.count_active_nodes_heartbeat(threshold).await.unwrap(),
            2
        );

        // Stale one node.
        let one_hour_ago = epoch_nanos() - 3_600_000_000_000;
        sqlx::query("UPDATE _node_registry SET last_heartbeat_at = ? WHERE node_id = 'node-b'")
            .bind(one_hour_ago)
            .execute(&pool)
            .await
            .unwrap();

        assert_eq!(
            reg_a.count_active_nodes_heartbeat(threshold).await.unwrap(),
            1
        );

        // Count assigned tables.
        reg_a.claim_table("test_count").await.unwrap();
        assert_eq!(reg_a.count_assigned_tables().await.unwrap(), 1);
        assert_eq!(reg_a.count_my_tables().await.unwrap(), 1);
        assert_eq!(reg_b.count_my_tables().await.unwrap(), 0);
    }
}
