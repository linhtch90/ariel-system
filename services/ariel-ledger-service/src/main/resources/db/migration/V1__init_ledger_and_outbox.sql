CREATE TABLE IF NOT EXISTS account_balances (
    account_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    balance DECIMAL(19, 4) NOT NULL,
    currency VARCHAR(12) NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    transaction_id VARCHAR(64) PRIMARY KEY,
    correlation_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    account_id VARCHAR(64) NOT NULL,
    counterparty_id VARCHAR(64) NOT NULL,
    amount DECIMAL(19, 4) NOT NULL,
    currency VARCHAR(12) NOT NULL,
    region VARCHAR(32) NOT NULL,
    risk_score DOUBLE PRECISION NOT NULL,
    resulting_balance DECIMAL(19, 4) NOT NULL,
    committed_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ledger_tenant_account
    ON ledger_entries (tenant_id, account_id, committed_at);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id VARCHAR(64) PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    topic VARCHAR(128) NOT NULL,
    payload_json CLOB NOT NULL,
    correlation_id VARCHAR(64) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    published BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events (published, created_at);
