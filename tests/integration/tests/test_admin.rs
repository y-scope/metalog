use metalog_coordinator::{RegisterTableOpts, TableRegistration};
use metalog_it::helpers::setup_db;

#[tokio::test]
async fn concurrent_registration_and_coordinator_provisioning_preserve_configuration() {
    let (pool, _db) = setup_db().await;
    let table = "concurrent_registration";
    let registration = TableRegistration::new(pool.clone(), "");
    assert!(registration
        .register_table(
            table,
            "Initial display",
            RegisterTableOpts {
                config_json: Some(
                    r#"{"retention":{"enabled":true,"grace_period_secs":30,
                    "scan_interval_secs":60,"delete_rate":7}}"#
                        .into(),
                ),
            },
        )
        .await
        .unwrap());
    let node = metalog_ha::NodeRegistry::new(pool.clone(), "registration-race-node");
    node.register_node().await.unwrap();
    assert!(node
        .claim_table_with_lease(table, Some(60_000_000_000))
        .await
        .unwrap());

    let barrier = tokio::sync::Barrier::new(3);
    let grace_updates = async {
        barrier.wait().await;
        for i in 0..8 {
            assert!(!registration
                .register_table(
                    table,
                    &format!("Display {i}"),
                    RegisterTableOpts {
                        config_json: Some(format!(
                            r#"{{"retention":{{"grace_period_secs":{}}}}}"#,
                            30 + i
                        )),
                    },
                )
                .await
                .map_err(|e| format!("grace update {i}: {e}"))?);
            tokio::task::yield_now().await;
        }
        Ok::<_, String>(())
    };
    let scan_updates = async {
        barrier.wait().await;
        for i in 0..8 {
            assert!(!registration
                .register_table(
                    table,
                    "",
                    RegisterTableOpts {
                        config_json: Some(format!(
                            r#"{{"retention":{{"scan_interval_secs":{}}}}}"#,
                            60 + i
                        )),
                    },
                )
                .await
                .map_err(|e| format!("scan update {i}: {e}"))?);
            tokio::task::yield_now().await;
        }
        Ok::<_, String>(())
    };
    // Coordinator startup repeats provisioning while the admin request updates
    // config/display. INSERT IGNORE's parent FK lock formerly inverted the admin
    // transaction's child-then-parent order and caused MariaDB error 1213.
    let coordinator = async {
        barrier.wait().await;
        for i in 0..16 {
            assert!(!metalog_schema::ensure_table(&pool, table, None)
                .await
                .map_err(|e| format!("coordinator provision {i}: {e}"))?);
            node.renew_leases(60_000_000_000)
                .await
                .map_err(|e| format!("lease renewal {i}: {e}"))?;
            tokio::task::yield_now().await;
        }
        Ok::<_, String>(())
    };
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        tokio::try_join!(grace_updates, scan_updates, coordinator)
    })
    .await
    .expect("bounded registration/provisioning race timed out")
    .unwrap();

    let json: String =
        sqlx::query_scalar("SELECT CAST(config AS CHAR) FROM _table_config WHERE table_name = ?")
            .bind(table)
            .fetch_one(&pool)
            .await
            .unwrap();
    let config = metalog_metastore::decode_table_config(Some(&json)).unwrap();
    assert!(config.retention.enabled);
    assert_eq!(config.retention.grace_period_secs, 37);
    assert_eq!(config.retention.scan_interval_secs, 67);
    assert_eq!(config.retention.delete_rate, 7);
    let display: String =
        sqlx::query_scalar("SELECT CAST(display_name AS CHAR) FROM _table WHERE table_name = ?")
            .bind(table)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(display, "Display 7");
    assert_eq!(node.get_my_leased_tables().await.unwrap(), vec![table]);
}

#[tokio::test]
async fn registration_validates_and_merges_stored_configuration() {
    let (pool, _db) = setup_db().await;
    let registration = TableRegistration::new(pool.clone(), "");
    for config in [
        r#"{"consolidation":{"enabled":true}}"#,
        r#"{"retention":{"scan_interval_secs":0,"enabled":true}}"#,
        r#"{"retention":{"misspelled":1}}"#,
    ] {
        assert!(registration
            .register_table(
                "bad_config",
                "bad",
                RegisterTableOpts {
                    config_json: Some(config.into())
                }
            )
            .await
            .is_err());
    }
    let tables: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM _table WHERE table_name = 'bad_config'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(tables, 0);
    assert!(registration
        .register_table(
            "configured",
            "Initial display",
            RegisterTableOpts {
                config_json: Some(
                    r#"{"retention":{
            "enabled":true,"grace_period_secs":30,"scan_interval_secs":60
        }}"#
                    .into()
                )
            }
        )
        .await
        .unwrap());
    assert!(!registration
        .register_table(
            "configured",
            "Updated display",
            RegisterTableOpts {
                config_json: Some(r#"{"retention":{"scan_interval_secs":120}}"#.into())
            }
        )
        .await
        .unwrap());
    let json: String = sqlx::query_scalar(
        "SELECT CAST(config AS CHAR) FROM _table_config WHERE table_name = 'configured'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let cfg = metalog_metastore::decode_table_config(Some(&json)).unwrap();
    assert!(cfg.retention.enabled);
    assert_eq!(cfg.retention.scan_interval_secs, 120);
    assert_eq!(cfg.retention.grace_period_secs, 30);
    let name: String = sqlx::query_scalar(
        "SELECT CAST(display_name AS CHAR) FROM _table WHERE table_name = 'configured'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(name, "Updated display");
}
