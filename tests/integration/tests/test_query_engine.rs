#[cfg(test)]
mod tests {
    use metalog_it::helpers::setup_db_with_table;
    use metalog_query::SplitQueryEngine;

    #[tokio::test]
    async fn query_returns_string_columns_correctly() {
        let (pool, _container) = setup_db_with_table("test_query_str").await;

        // Insert a record with a string archive_path.
        sqlx::query(
            "INSERT INTO `test_query_str` (min_timestamp, max_timestamp, state, record_count, \
             retention_days, expires_at, archive_path, file_path) \
             VALUES (1000, 2000, 'ARCHIVE_CLOSED', 100, 30, 0, 'archive-aaa', 'file1.log')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let engine = SplitQueryEngine::new(pool);
        let columns: Vec<String> = vec![
            "archive_path".into(),
            "file_path".into(),
            "min_timestamp".into(),
            "max_timestamp".into(),
        ];
        let order: Vec<String> = vec!["max_timestamp DESC".into(), "id DESC".into()];
        let empty: Vec<String> = vec![];

        let results = engine
            .execute_page("test_query_str", &columns, "", &order, "", 10, &empty)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);

        let row = &results[0];
        let archive_path = row.get("archive_path").unwrap();
        assert!(
            archive_path.is_string(),
            "archive_path should be string, got: {archive_path:?}"
        );
        assert_eq!(archive_path.as_str().unwrap(), "archive-aaa");

        let file_path = row.get("file_path").unwrap();
        assert!(
            file_path.is_string(),
            "file_path should be string, got: {file_path:?}"
        );
        assert_eq!(file_path.as_str().unwrap(), "file1.log");
    }

    #[tokio::test]
    async fn group_by_returns_distinct_archives() {
        let (pool, _container) = setup_db_with_table("test_query_grp").await;

        // Archive A: 2 files
        for (path, min_ts, max_ts) in [("f1.log", 1000_i64, 2000_i64), ("f2.log", 3000, 4000)] {
            sqlx::query(&format!(
                "INSERT INTO `test_query_grp` (min_timestamp, max_timestamp, state, record_count, \
                 retention_days, expires_at, archive_path, file_path) \
                 VALUES ({min_ts}, {max_ts}, 'ARCHIVE_CLOSED', 100, 30, 0, 'archive-aaa', '{path}')"
            ))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Archive B: 1 file
        sqlx::query(
            "INSERT INTO `test_query_grp` (min_timestamp, max_timestamp, state, record_count, \
             retention_days, expires_at, archive_path, file_path) \
             VALUES (5000, 6000, 'ARCHIVE_CLOSED', 50, 30, 0, 'archive-bbb', 'f3.log')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let engine = SplitQueryEngine::new(pool);
        let columns: Vec<String> = vec![
            "archive_path".into(),
            "MIN(min_timestamp)".into(),
            "MAX(max_timestamp)".into(),
            "SUM(record_count)".into(),
        ];
        let order: Vec<String> = vec!["max_timestamp DESC".into()];
        let group_by: Vec<String> = vec!["archive_path".into()];

        let results = engine
            .execute_page(
                "test_query_grp",
                &columns,
                "state = 'ARCHIVE_CLOSED'",
                &order,
                "",
                10,
                &group_by,
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 2, "should return 2 distinct archives");

        // Archive B first (higher max_timestamp)
        let first = &results[0];
        assert_eq!(first.get("archive_path").unwrap().as_str().unwrap(), "archive-bbb");
        assert_eq!(first.get("max_timestamp").unwrap().as_i64().unwrap(), 6000);
        assert_eq!(first.get("min_timestamp").unwrap().as_i64().unwrap(), 5000);
        assert_eq!(first.get("record_count").unwrap().as_i64().unwrap(), 50);

        // Archive A second
        let second = &results[1];
        assert_eq!(second.get("archive_path").unwrap().as_str().unwrap(), "archive-aaa");
        assert_eq!(second.get("max_timestamp").unwrap().as_i64().unwrap(), 4000);
        assert_eq!(second.get("min_timestamp").unwrap().as_i64().unwrap(), 1000);
        assert_eq!(second.get("record_count").unwrap().as_i64().unwrap(), 200);
    }
}
