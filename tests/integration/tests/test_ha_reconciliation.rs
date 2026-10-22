use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
        Mutex,
    },
    time::Duration,
};

use metalog_config::{CoordinatorConfig, HAStrategy, NodeConfig};
use metalog_ha::{CoordinatorCallback, NodeRegistry, ReconciliationLoop};
use metalog_it::helpers::{setup_db, setup_db_with_table};
use metalog_node::{NodeBuilder, Resources};
use metalog_timeutil::epoch_nanos;
use sqlx::MySqlPool;
use tokio_util::sync::CancellationToken;

fn test_node_config() -> NodeConfig {
    serde_json::from_value(serde_json::json!({"database": {"primary": {"database": "test"}}}))
        .unwrap()
}

type Events = Arc<Mutex<Vec<(String, String)>>>;

fn callbacks(events: &Events) -> (CoordinatorCallback, CoordinatorCallback) {
    let started = events.clone();
    let stopped = events.clone();
    (
        Arc::new(move |table| {
            let events = started.clone();
            Box::pin(async move {
                tokio::task::yield_now().await;
                events.lock().unwrap().push(("start".into(), table));
                Ok(())
            })
        }),
        Arc::new(move |table| {
            let events = stopped.clone();
            Box::pin(async move {
                tokio::task::yield_now().await;
                events.lock().unwrap().push(("stop".into(), table));
                Ok(())
            })
        }),
    )
}

fn reconciler(
    registry: Arc<NodeRegistry>,
    events: &Events,
    strategy: HAStrategy,
) -> ReconciliationLoop {
    let (start, stop) = callbacks(events);
    ReconciliationLoop::new(
        registry,
        CoordinatorConfig {
            enabled: true,
            ha_strategy: strategy,
            ..Default::default()
        },
        start,
        stop,
        Arc::new(|_| false),
    )
}

async fn add_assignment(pool: &MySqlPool, table: &str) {
    sqlx::query("INSERT INTO _table (table_name, display_name) VALUES (?, ?)")
        .bind(table)
        .bind(table)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO _table_assignment (table_name) VALUES (?)")
        .bind(table)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn fresh_cluster_claims_every_table_without_exceeding_fair_share() {
    let (pool, _container) = setup_db().await;
    for n in 0..5 {
        add_assignment(&pool, &format!("table_{n}")).await;
    }
    let a = Arc::new(NodeRegistry::new(pool.clone(), "a"));
    let b = Arc::new(NodeRegistry::new(pool.clone(), "b"));
    a.register_node().await.unwrap();
    b.register_node().await.unwrap();
    let events = Events::default();
    let a_loop = reconciler(a.clone(), &events, HAStrategy::Heartbeat);
    let b_loop = reconciler(b.clone(), &events, HAStrategy::Heartbeat);
    let mut a_running = HashSet::new();
    let mut b_running = HashSet::new();
    a_loop.reconcile_once(&mut a_running).await.unwrap();
    assert_eq!(a_running.len(), 3);
    assert_eq!(a.count_my_tables().await.unwrap(), 3);
    b_loop.reconcile_once(&mut b_running).await.unwrap();
    assert_eq!(b_running.len(), 2);
    assert!(a_running.is_disjoint(&b_running));
    assert!(a.get_unassigned_tables().await.unwrap().is_empty());
    a_loop.reconcile_once(&mut a_running).await.unwrap();
    assert_eq!(
        events.lock().unwrap().len(),
        8,
        "owned units are checked for configuration refresh"
    );
}

#[tokio::test]
async fn inactive_tables_are_not_claimed_and_deactivation_stops_the_owner() {
    let (pool, _container) = setup_db().await;
    add_assignment(&pool, "logs").await;
    add_assignment(&pool, "disabled").await;
    sqlx::query("UPDATE _table SET active = FALSE WHERE table_name = 'disabled'")
        .execute(&pool)
        .await
        .unwrap();
    let registry = Arc::new(NodeRegistry::new(pool.clone(), "a"));
    registry.register_node().await.unwrap();
    assert!(!registry.claim_table("disabled").await.unwrap());
    let events = Events::default();
    let reconcile = reconciler(registry.clone(), &events, HAStrategy::Lease);
    let mut running = HashSet::new();
    reconcile.reconcile_once(&mut running).await.unwrap();
    assert_eq!(running, HashSet::from(["logs".to_owned()]));

    sqlx::query("UPDATE _table SET active = FALSE WHERE table_name = 'logs'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(registry.get_my_tables().await.unwrap().is_empty());
    assert!(registry.get_my_leased_tables().await.unwrap().is_empty());
    assert_eq!(registry.count_my_tables().await.unwrap(), 0);
    assert_eq!(registry.count_assigned_tables().await.unwrap(), 0);
    reconcile.reconcile_once(&mut running).await.unwrap();
    assert!(running.is_empty());
    assert!(registry.get_unassigned_tables().await.unwrap().is_empty());
    let assignment: (Option<String>, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT node_id, node_assigned_at, lease_expiry FROM _table_assignment WHERE table_name = \
         'logs'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(assignment, (None, None, None));
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            ("start".into(), "logs".into()),
            ("stop".into(), "logs".into())
        ]
    );

    sqlx::query("UPDATE _table SET active = TRUE WHERE table_name = 'logs'")
        .execute(&pool)
        .await
        .unwrap();
    reconcile.reconcile_once(&mut running).await.unwrap();
    assert_eq!(running, HashSet::from(["logs".to_owned()]));
}

#[tokio::test]
async fn heartbeat_failover_recovers_after_the_last_owner_dies() {
    let (pool, _container) = setup_db().await;
    add_assignment(&pool, "logs").await;
    let a = Arc::new(NodeRegistry::new(pool.clone(), "a"));
    let b = Arc::new(NodeRegistry::new(pool.clone(), "b"));
    a.register_node().await.unwrap();
    a.claim_table("logs").await.unwrap();
    sqlx::query("UPDATE _node_registry SET last_heartbeat_at = 0 WHERE node_id = 'a'")
        .execute(&pool)
        .await
        .unwrap();
    b.register_node().await.unwrap();
    let events = Events::default();
    let mut running = HashSet::new();
    reconciler(b.clone(), &events, HAStrategy::Heartbeat)
        .reconcile_once(&mut running)
        .await
        .unwrap();
    assert_eq!(running, HashSet::from(["logs".to_string()]));
    assert_eq!(b.get_my_tables().await.unwrap(), vec!["logs"]);
    assert!(a.get_my_tables().await.unwrap().is_empty());
}

#[tokio::test]
async fn startup_failure_releases_claim_and_is_retried() {
    let (pool, _container) = setup_db().await;
    add_assignment(&pool, "logs").await;
    let registry = Arc::new(NodeRegistry::new(pool.clone(), "a"));
    registry.register_node().await.unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let starts = attempts.clone();
    let start: CoordinatorCallback = Arc::new(move |_| {
        let starts = starts.clone();
        Box::pin(async move {
            tokio::task::yield_now().await;
            if starts.fetch_add(1, Ordering::SeqCst) == 0 {
                Err("transient startup failure".into())
            } else {
                Ok(())
            }
        })
    });
    let (_, stop) = callbacks(&Events::default());
    let reconcile = ReconciliationLoop::new(
        registry.clone(),
        CoordinatorConfig::default(),
        start,
        stop,
        Arc::new(|_| false),
    );
    let mut running = HashSet::new();
    reconcile.reconcile_once(&mut running).await.unwrap();
    assert!(running.is_empty());
    assert_eq!(
        registry.get_unassigned_tables().await.unwrap(),
        vec!["logs"]
    );
    reconcile.reconcile_once(&mut running).await.unwrap();
    assert!(running.contains("logs"));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn lease_claim_is_initialized_and_expired_owner_cannot_renew_it() {
    let (pool, _container) = setup_db().await;
    add_assignment(&pool, "logs").await;
    let a = Arc::new(NodeRegistry::new(pool.clone(), "a"));
    let b = Arc::new(NodeRegistry::new(pool.clone(), "b"));
    a.register_node().await.unwrap();
    b.register_node().await.unwrap();
    let events = Events::default();
    let mut running = HashSet::new();
    reconciler(a.clone(), &events, HAStrategy::Lease)
        .reconcile_once(&mut running)
        .await
        .unwrap();
    let (expiry,): (i64,) =
        sqlx::query_as("SELECT lease_expiry FROM _table_assignment WHERE table_name = 'logs'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(expiry > epoch_nanos());
    sqlx::query("UPDATE _table_assignment SET lease_expiry = 1 WHERE table_name = 'logs'")
        .execute(&pool)
        .await
        .unwrap();
    a.renew_leases(180_000_000_000).await.unwrap();
    assert!(a.get_my_leased_tables().await.unwrap().is_empty());
    let mut b_running = HashSet::new();
    reconciler(b.clone(), &events, HAStrategy::Lease)
        .reconcile_once(&mut b_running)
        .await
        .unwrap();
    assert!(b_running.contains("logs"));
    let (expiry,): (i64,) =
        sqlx::query_as("SELECT lease_expiry FROM _table_assignment WHERE table_name = 'logs'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        expiry > epoch_nanos(),
        "new owner cannot inherit the expired lease"
    );
    reconciler(a.clone(), &events, HAStrategy::Lease)
        .reconcile_once(&mut running)
        .await
        .unwrap();
    assert!(running.is_empty());
    assert!(events
        .lock()
        .unwrap()
        .contains(&("stop".into(), "logs".into())));
}

#[tokio::test]
async fn graceful_shutdown_deregisters_nodes_and_replacements_claim_every_table() {
    let (pool, _container) = setup_db().await;
    for n in 0..4 {
        add_assignment(&pool, &format!("logs_{n}")).await;
    }
    let a = Arc::new(NodeRegistry::new(pool.clone(), "old-a"));
    let b = Arc::new(NodeRegistry::new(pool.clone(), "old-b"));
    a.register_node().await.unwrap();
    b.register_node().await.unwrap();
    let events = Events::default();
    let token = CancellationToken::new();
    let mut handles = Vec::new();
    for registry in [&a, &b] {
        let reconcile = reconciler(registry.clone(), &events, HAStrategy::Heartbeat);
        let run_token = token.clone();
        handles.push(tokio::spawn(async move {
            reconcile.run(run_token).await;
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if events.lock().unwrap().len() == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    token.cancel();
    for handle in handles {
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .unwrap()
            .unwrap();
    }
    {
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 8);
        for n in 0..4 {
            let table = format!("logs_{n}");
            let start = events
                .iter()
                .position(|event| event == &("start".into(), table.clone()));
            let stop = events
                .iter()
                .position(|event| event == &("stop".into(), table.clone()));
            assert!(start.unwrap() < stop.unwrap());
        }
    }
    assert!(a.get_my_tables().await.unwrap().is_empty());
    assert!(b.get_my_tables().await.unwrap().is_empty());
    // A heartbeat finishing after deregistration must not recreate a stopped node.
    a.send_heartbeat().await.unwrap();
    b.send_heartbeat().await.unwrap();
    assert_eq!(
        a.count_active_nodes_heartbeat(180_000_000_000)
            .await
            .unwrap(),
        0
    );

    let c = Arc::new(NodeRegistry::new(pool.clone(), "replacement-c"));
    let d = Arc::new(NodeRegistry::new(pool.clone(), "replacement-d"));
    c.register_node().await.unwrap();
    d.register_node().await.unwrap();
    let mut c_running = HashSet::new();
    let mut d_running = HashSet::new();
    reconciler(c.clone(), &events, HAStrategy::Heartbeat)
        .reconcile_once(&mut c_running)
        .await
        .unwrap();
    reconciler(d.clone(), &events, HAStrategy::Heartbeat)
        .reconcile_once(&mut d_running)
        .await
        .unwrap();
    assert_eq!(c_running.len(), 2);
    assert_eq!(d_running.len(), 2);
    assert!(c_running.is_disjoint(&d_running));
    assert!(c.get_unassigned_tables().await.unwrap().is_empty());
}

#[tokio::test]
async fn node_loads_stored_config_and_start_stop_is_idempotent() {
    let (pool, _container) = setup_db_with_table("configured").await;
    sqlx::query("UPDATE _table_config SET config = ? WHERE table_name = 'configured'")
        .bind(
            serde_json::json!({
                "retention": {"enabled": false, "grace_period_secs": 42},
                "consolidation": {"enabled": false, "stale_buffering_mins": 77},
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();
    let resources = Arc::new(Resources::new(pool.clone(), true, Duration::from_secs(60)));
    let mut node = NodeBuilder::new(test_node_config(), resources.clone()).build();
    node.start(&[]).await.unwrap();
    let (start, stop, _) = node.ha_callbacks();
    let (a, b) = tokio::join!(start("configured".into()), start("configured".into()));
    a.unwrap();
    b.unwrap();
    let unit = resources.coordinator("configured").unwrap();
    assert_eq!(unit.table_config().retention.grace_period_secs, 42);
    assert_eq!(unit.table_config().consolidation.stale_buffering_mins, 77);
    start("configured".into()).await.unwrap();
    assert!(Arc::ptr_eq(
        &unit,
        &resources.coordinator("configured").unwrap()
    ));
    sqlx::query("UPDATE _table_config SET config = ? WHERE table_name = 'configured'")
        .bind(r#"{"retention":{"enabled":false,"grace_period_secs":99}}"#)
        .execute(&pool)
        .await
        .unwrap();
    start("configured".into()).await.unwrap();
    let refreshed = resources.coordinator("configured").unwrap();
    assert!(!Arc::ptr_eq(&unit, &refreshed));
    assert!(unit.token().is_cancelled());
    assert_eq!(refreshed.table_config().retention.grace_period_secs, 99);
    stop("configured".into()).await.unwrap();
    stop("configured".into()).await.unwrap();
    assert!(unit.token().is_cancelled());
    assert!(resources.coordinator("configured").is_none());
    start("configured".into()).await.unwrap();
    assert!(!Arc::ptr_eq(
        &unit,
        &resources.coordinator("configured").unwrap()
    ));
    node.stop().await;
    assert!(resources.coordinator("configured").is_none());
    assert!(start("configured".into()).await.is_err());
}

#[tokio::test]
async fn node_rejects_enabled_unwired_lifecycle_instead_of_ignoring_it() {
    let (pool, _container) = setup_db_with_table("unsupported").await;
    for feature in ["retention", "consolidation"] {
        sqlx::query("UPDATE _table_config SET config = ? WHERE table_name = 'unsupported'")
            .bind(format!(r#"{{"{feature}":{{"enabled":true}}}}"#))
            .execute(&pool)
            .await
            .unwrap();
        let resources = Arc::new(Resources::new(pool.clone(), true, Duration::from_secs(60)));
        let mut node = NodeBuilder::new(test_node_config(), resources.clone()).build();
        let error = node.start(&["unsupported".into()]).await.unwrap_err();
        assert!(matches!(
            error,
            metalog_node::NodeError::UnsupportedFeature { .. }
        ));
        assert!(error.to_string().contains(feature));
        assert!(resources.coordinator("unsupported").is_none());
    }
    sqlx::query("UPDATE _table_config SET config = ? WHERE table_name = 'unsupported'")
        .bind(
            serde_json::json!({"retention": {
                "enabled": true, "scan_interval_secs": u64::MAX,
            }})
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();
    let resources = Arc::new(Resources::new(pool.clone(), true, Duration::from_secs(60)));
    let mut node = NodeBuilder::new(test_node_config(), resources)
        .with_storage(Arc::new(metalog_storage::Registry::new()))
        .build();
    let error = node.start(&["unsupported".into()]).await.unwrap_err();
    assert!(matches!(error, metalog_node::NodeError::TableConfig { .. }));
}

#[tokio::test]
async fn watchdog_waits_for_stop_before_restart() {
    let (pool, _container) = setup_db().await;
    add_assignment(&pool, "logs").await;
    let registry = Arc::new(NodeRegistry::new(pool, "a"));
    registry.register_node().await.unwrap();
    let events = Events::default();
    let (start, stop) = callbacks(&events);
    let stalled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let checker = stalled.clone();
    let reconcile = ReconciliationLoop::new(
        registry,
        CoordinatorConfig::default(),
        start,
        stop,
        Arc::new(move |_| checker.load(Ordering::SeqCst)),
    );
    let mut running = HashSet::new();
    reconcile.reconcile_once(&mut running).await.unwrap();
    stalled.store(true, Ordering::SeqCst);
    reconcile.reconcile_once(&mut running).await.unwrap();
    assert_eq!(
        *events.lock().unwrap(),
        vec![
            ("start".into(), "logs".into()),
            ("stop".into(), "logs".into()),
            ("start".into(), "logs".into()),
        ]
    );
}

#[tokio::test]
async fn lease_liveness_refreshes_membership_even_without_tables() {
    let (pool, _container) = setup_db().await;
    let registry = Arc::new(NodeRegistry::new(pool.clone(), "a"));
    registry.register_node().await.unwrap();
    sqlx::query("UPDATE _node_registry SET last_heartbeat_at = 0 WHERE node_id = 'a'")
        .execute(&pool)
        .await
        .unwrap();
    let token = CancellationToken::new();
    let child = token.clone();
    let liveness = metalog_ha::LivenessLoop::new(
        registry.clone(),
        HAStrategy::Lease,
        Duration::from_millis(10),
        Duration::from_secs(180),
    );
    let handle = tokio::spawn(async move {
        liveness.run(child).await;
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if registry
                .count_active_nodes_heartbeat(180_000_000_000)
                .await
                .unwrap()
                == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    token.cancel();
    handle.await.unwrap();
}
