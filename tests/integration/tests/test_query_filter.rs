#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metalog_it::helpers::setup_db_with_table;
    use metalog_query::SplitQueryEngine;
    use metalog_schema::ColumnRegistry;
    use sqlx::MySqlPool;

    /// Inserts a row directly with the given state and timestamps.
    async fn insert_row(pool: &MySqlPool, table: &str, state: &str, min_ts: i64, max_ts: i64) {
        sqlx::query(&format!(
            "INSERT INTO `{table}` (min_timestamp, max_timestamp, state, record_count, \
             retention_days, expires_at) VALUES ({min_ts}, {max_ts}, '{state}', 1, 30, 0)"
        ))
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_archive(pool: &MySqlPool, table: &str, path: &str, min_ts: i64, max_ts: i64) {
        sqlx::query(&format!(
            "INSERT INTO `{table}` (min_timestamp, max_timestamp, state, archive_path, \
             record_count, retention_days, expires_at) \
             VALUES ({min_ts}, {max_ts}, 'ARCHIVE_CLOSED', '{path}', 10, 30, 0)"
        ))
        .execute(pool)
        .await
        .unwrap();
    }

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn order(specs: &[&str]) -> Vec<String> {
        specs.iter().map(|s| s.to_string()).collect()
    }

    // ── state filter ─────────────────────────────────────────────────────────

    #[tokio::test]
    async fn filter_by_state_returns_subset() {
        let (pool, _c) = setup_db_with_table("test_qf_state").await;

        insert_row(&pool, "test_qf_state", "ARCHIVE_CLOSED", 1000, 2000).await;
        insert_row(&pool, "test_qf_state", "ARCHIVE_CLOSED", 3000, 4000).await;
        insert_row(&pool, "test_qf_state", "IR_BUFFERING", 5000, 6000).await;

        let engine = SplitQueryEngine::new(pool.clone());
        let results = engine
            .execute_page(
                "test_qf_state",
                &cols(&["state", "min_timestamp"]),
                "state = 'ARCHIVE_CLOSED'",
                &order(&["min_timestamp ASC", "id ASC"]),
                "",
                100,
                &[],
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        for row in &results {
            assert_eq!(row["state"].as_str().unwrap(), "ARCHIVE_CLOSED");
        }
    }

    #[tokio::test]
    async fn empty_filter_returns_all_rows() {
        let (pool, _c) = setup_db_with_table("test_qf_empty").await;

        for ts in [1000i64, 2000, 3000] {
            insert_row(&pool, "test_qf_empty", "IR_BUFFERING", ts, ts + 500).await;
        }

        let engine = SplitQueryEngine::new(pool.clone());
        let results = engine
            .execute_page(
                "test_qf_empty",
                &cols(&["min_timestamp"]),
                "",
                &order(&["min_timestamp ASC", "id ASC"]),
                "",
                100,
                &[],
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
    }

    // ── timestamp range filter ────────────────────────────────────────────────

    #[tokio::test]
    async fn filter_timestamp_gte() {
        let (pool, _c) = setup_db_with_table("test_qf_ts").await;

        for ts in [100i64, 200, 300, 400, 500] {
            insert_row(&pool, "test_qf_ts", "IR_BUFFERING", ts, ts + 10).await;
        }

        let engine = SplitQueryEngine::new(pool.clone());
        let results = engine
            .execute_page(
                "test_qf_ts",
                &cols(&["min_timestamp"]),
                "min_timestamp >= 300",
                &order(&["min_timestamp ASC", "id ASC"]),
                "",
                100,
                &[],
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0]["min_timestamp"].as_i64().unwrap(), 300);
    }

    // ── keyset pagination ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn keyset_pagination_no_duplicates_no_gaps() {
        let (pool, _c) = setup_db_with_table("test_qf_page").await;

        // 10 rows at distinct timestamps.
        for i in 0..10i64 {
            insert_row(&pool, "test_qf_page", "IR_BUFFERING", i * 100, i * 100 + 50).await;
        }

        let engine = SplitQueryEngine::new(pool.clone());
        let page_size = 3;
        let mut all_ids: Vec<i64> = Vec::new();
        let mut cursor = String::new();

        loop {
            let results = engine
                .execute_page(
                    "test_qf_page",
                    &cols(&["id", "min_timestamp"]),
                    "",
                    &order(&["min_timestamp ASC", "id ASC"]),
                    &cursor,
                    page_size,
                    &[],
                )
                .await
                .unwrap();

            if results.is_empty() {
                break;
            }

            for row in &results {
                all_ids.push(row["id"].as_i64().unwrap());
            }

            // Build cursor from last row for next page.
            let last = results.last().unwrap();
            let last_ts = last["min_timestamp"].as_i64().unwrap();
            let last_id = last["id"].as_i64().unwrap();
            cursor = format!("(min_timestamp > {last_ts}) OR (min_timestamp = {last_ts} AND id > {last_id})");

            if results.len() < page_size as usize {
                break;
            }
        }

        // All 10 rows found, no duplicates.
        assert_eq!(all_ids.len(), 10);
        let unique: std::collections::HashSet<_> = all_ids.iter().collect();
        assert_eq!(unique.len(), 10, "no duplicate rows across pages");
        // IDs should be in ascending order (we ordered by min_timestamp ASC, id ASC).
        assert!(all_ids.windows(2).all(|w| w[0] < w[1]));
    }

    #[tokio::test]
    async fn limit_respected() {
        let (pool, _c) = setup_db_with_table("test_qf_limit").await;

        for ts in [10i64, 20, 30, 40, 50] {
            insert_row(&pool, "test_qf_limit", "IR_BUFFERING", ts, ts + 1).await;
        }

        let engine = SplitQueryEngine::new(pool.clone());
        let results = engine
            .execute_page(
                "test_qf_limit",
                &cols(&["id"]),
                "",
                &order(&["min_timestamp ASC", "id ASC"]),
                "",
                2,
                &[],
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
    }

    // ── __DIM column resolution ───────────────────────────────────────────────

    #[tokio::test]
    async fn dim_filter_resolves_to_physical_column() {
        let (pool, _c) = setup_db_with_table("test_qf_dim").await;

        // Allocate a dim slot for "env" → should become dim_f01.
        let registry = ColumnRegistry::new(pool.clone(), "test_qf_dim").await.unwrap();
        let col = registry.resolve_or_allocate_dim("env", "str", 64).await.unwrap();
        assert_eq!(col, "dim_f01");

        // Insert rows with different env values via the physical column.
        sqlx::query(
            "INSERT INTO `test_qf_dim` (min_timestamp, max_timestamp, state, record_count, \
             retention_days, expires_at, dim_f01) VALUES (1000, 2000, 'IR_BUFFERING', 1, 30, 0, 'prod')",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO `test_qf_dim` (min_timestamp, max_timestamp, state, record_count, \
             retention_days, expires_at, dim_f01) VALUES (3000, 4000, 'IR_BUFFERING', 1, 30, 0, 'staging')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let engine = SplitQueryEngine::new(pool.clone());

        // Rewrite filter using the registry — __DIM.env → dim_f01.
        let filter_raw = "__DIM.env = 'prod'";
        let filter_resolved =
            metalog_query::rewrite_filter_columns(filter_raw, Some(&registry)).unwrap();

        let results = engine
            .execute_page(
                "test_qf_dim",
                &cols(&["min_timestamp", "dim_f01"]),
                &filter_resolved,
                &order(&["min_timestamp ASC", "id ASC"]),
                "",
                100,
                &[],
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["dim_f01"].as_str().unwrap(), "prod");
    }

    #[tokio::test]
    async fn file_prefix_resolved_in_filter() {
        let (pool, _c) = setup_db_with_table("test_qf_file_prefix").await;

        insert_row(&pool, "test_qf_file_prefix", "IR_BUFFERING", 500, 600).await;
        insert_row(&pool, "test_qf_file_prefix", "IR_BUFFERING", 1500, 1600).await;

        let engine = SplitQueryEngine::new(pool.clone());

        // __FILE.min_timestamp should rewrite to min_timestamp.
        let filter =
            metalog_query::rewrite_filter_columns("__FILE.min_timestamp >= 1000", None).unwrap();

        let results = engine
            .execute_page(
                "test_qf_file_prefix",
                &cols(&["min_timestamp"]),
                &filter,
                &order(&["min_timestamp ASC", "id ASC"]),
                "",
                100,
                &[],
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["min_timestamp"].as_i64().unwrap(), 1500);
    }

    // ── ordering ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn descending_order_returns_newest_first() {
        let (pool, _c) = setup_db_with_table("test_qf_order").await;

        for ts in [100i64, 200, 300] {
            insert_row(&pool, "test_qf_order", "IR_BUFFERING", ts, ts + 10).await;
        }

        let engine = SplitQueryEngine::new(pool.clone());
        let results = engine
            .execute_page(
                "test_qf_order",
                &cols(&["min_timestamp"]),
                "",
                &order(&["min_timestamp DESC", "id DESC"]),
                "",
                100,
                &[],
            )
            .await
            .unwrap();

        let timestamps: Vec<i64> = results
            .iter()
            .map(|r| r["min_timestamp"].as_i64().unwrap())
            .collect();
        assert_eq!(timestamps, vec![300, 200, 100]);
    }

    // ── invalid filter rejected ───────────────────────────────────────────────

    #[tokio::test]
    async fn invalid_filter_returns_error() {
        let (pool, _c) = setup_db_with_table("test_qf_invalid").await;
        let engine = SplitQueryEngine::new(pool.clone());

        let result = engine
            .execute_page(
                "test_qf_invalid",
                &cols(&["id"]),
                "'; DROP TABLE test_qf_invalid; --",
                &order(&["min_timestamp ASC", "id ASC"]),
                "",
                10,
                &[],
            )
            .await;

        assert!(result.is_err(), "SQL injection attempt should be rejected");
    }
}
