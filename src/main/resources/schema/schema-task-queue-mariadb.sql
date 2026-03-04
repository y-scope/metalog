CREATE DATABASE IF NOT EXISTS clp_metastore;
USE clp_metastore;

CREATE TABLE IF NOT EXISTS _task_queue (
    task_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_name   VARCHAR(64) NOT NULL,
    state        ENUM('pending', 'processing', 'completed', 'failed', 'timed_out', 'dead_letter')
                     NOT NULL DEFAULT 'pending',
    worker_id    VARCHAR(64) NULL,
    created_at   INT UNSIGNED NOT NULL DEFAULT (UNIX_TIMESTAMP()),
    claimed_at   INT UNSIGNED NULL,
    completed_at INT UNSIGNED NULL,
    retry_count  TINYINT UNSIGNED NOT NULL DEFAULT 0,
    input        MEDIUMBLOB NOT NULL,
    output       MEDIUMBLOB NULL,
    INDEX idx_claim   (table_name, state),
    INDEX idx_stale   (table_name, state, claimed_at),
    INDEX idx_cleanup (table_name, state, completed_at)
) ENGINE=InnoDB;
