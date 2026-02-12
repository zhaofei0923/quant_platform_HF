-- Quant HFT ops/audit schema tables.
-- Safe to re-run: all statements are idempotent.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.system_config (
    config_key VARCHAR(50) PRIMARY KEY,
    config_value TEXT,
    description VARCHAR(200),
    update_time TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.system_logs (
    log_id BIGINT GENERATED ALWAYS AS IDENTITY,
    log_level VARCHAR(10) NOT NULL,
    module VARCHAR(50),
    account_id VARCHAR(32),
    strategy_id VARCHAR(32),
    order_ref VARCHAR(50),
    message TEXT NOT NULL,
    detail JSONB,
    create_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (log_id, create_time)
) PARTITION BY RANGE (create_time);

CREATE INDEX IF NOT EXISTS idx_ops_system_logs
    ON ops.system_logs (create_time, account_id);

CREATE TABLE IF NOT EXISTS ops.archive_manifest (
    archive_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    file_path VARCHAR(500),
    file_size BIGINT,
    checksum VARCHAR(64),
    archive_time TIMESTAMPTZ DEFAULT NOW(),
    status SMALLINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ops.sim_account_mapping (
    sim_account_id VARCHAR(32) PRIMARY KEY,
    real_account_id VARCHAR(32) NOT NULL,
    broker_id VARCHAR(20),
    user_id VARCHAR(32),
    password VARCHAR(100),
    app_id VARCHAR(50),
    auth_code VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_ops_sim_mapping
    ON ops.sim_account_mapping (real_account_id);

CREATE TABLE IF NOT EXISTS ops.ctp_connection (
    conn_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(50),
    trader_front VARCHAR(200),
    md_front VARCHAR(200),
    broker_id VARCHAR(20),
    user_id VARCHAR(32),
    password VARCHAR(100),
    auth_code VARCHAR(100),
    app_id VARCHAR(50),
    environment VARCHAR(20),
    priority INT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS ops.settlement_runs (
    trading_day DATE PRIMARY KEY,
    status VARCHAR(32) NOT NULL,
    force_run BOOLEAN NOT NULL DEFAULT FALSE,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_code VARCHAR(64) NOT NULL DEFAULT '',
    error_msg TEXT NOT NULL DEFAULT '',
    evidence_path VARCHAR(512) NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_ops_settlement_runs_status CHECK (
        status IN (
            'PENDING_PRICE',
            'CALCULATED',
            'RECONCILING',
            'COMPLETED',
            'BLOCKED',
            'FAILED',
            'RUNNING'
        )
    )
);

CREATE TABLE IF NOT EXISTS ops.settlement_reconcile_diff (
    diff_id BIGINT GENERATED ALWAYS AS IDENTITY,
    trading_day DATE NOT NULL,
    account_id VARCHAR(32) NOT NULL DEFAULT '',
    diff_type VARCHAR(32) NOT NULL,
    key_ref VARCHAR(128) NOT NULL DEFAULT '',
    local_value DECIMAL(22, 8),
    ctp_value DECIMAL(22, 8),
    delta_value DECIMAL(22, 8),
    diagnose_hint TEXT NOT NULL DEFAULT '',
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (diff_id, created_at),
    CONSTRAINT chk_ops_settlement_reconcile_diff_type CHECK (
        diff_type IN ('FUNDS', 'POSITION', 'QUERY_ERROR')
    )
) PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS idx_ops_settlement_reconcile_diff_day
    ON ops.settlement_reconcile_diff (trading_day, diff_type, created_at);
