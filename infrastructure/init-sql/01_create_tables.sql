CREATE TABLE IF NOT EXISTS raw_transactions (
    tx_id        VARCHAR(36) PRIMARY KEY,
    user_id      VARCHAR(20) NOT NULL,
    merchant     VARCHAR(50),
    amount       NUMERIC(15, 2),
    currency     VARCHAR(10) DEFAULT 'VND',
    country      VARCHAR(5),
    tx_at        TIMESTAMP NOT NULL,
    device       VARCHAR(20),
    is_fraud     BOOLEAN DEFAULT FALSE,
    fraud_reason VARCHAR(100),
    ingested_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id       SERIAL PRIMARY KEY,
    tx_id          VARCHAR(36),
    user_id        VARCHAR(20),
    rule_triggered VARCHAR(50),
    severity       VARCHAR(10),
    created_at     TIMESTAMP DEFAULT NOW()
);
