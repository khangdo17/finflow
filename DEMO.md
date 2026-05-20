# FinFlow — 5-Minute Demo Script

> **Goal**: Show the full Lambda Architecture pipeline live — from transaction generation to real-time fraud detection to batch analytics to the serving dashboard.

---

## Prerequisites (do before the demo)

```bash
make install       # Install Python dependencies
make up            # Start all Docker services (Kafka, Postgres, Redis, Airflow)
make verify        # Confirm all services are healthy
```

Expected output of `make verify`:
```
 Kafka — connected, topics visible
 Postgres — connected, tables exist
 Redis — PONG
```

---

## Minute 1 — Infrastructure Overview

Open browser tabs before presenting:
- **Kafka UI**: http://localhost:8090 — show `raw-transactions` and `fraud-alerts` topics
- **Airflow**: http://localhost:8080 (admin / admin) — show DAG list
- **Redis Commander**: http://localhost:8091 — show empty keyspace

Point out the architecture:
```
Generator → Kafka → Speed Layer (Spark) → Redis + Kafka fraud-alerts
                 → Batch Layer (Airflow + dbt) → Postgres marts
                                                → Observability (DuckDB)
                                                → Serving (FastAPI + Streamlit)
```

---

## Minute 2 — Start the Data Generator

Open **Terminal 1** — run in normal mode first:

```bash
python -m data_generator.generator --rate 500 --mode normal
```

Switch to Kafka UI → `raw-transactions` topic → show messages arriving.
Point out: Vietnamese merchants (Grab, MoMo, VNPay), lognormal amounts, 85% VN country.

Inject fraud:

```bash
# Stop normal mode (Ctrl+C), then run mixed mode
python -m data_generator.generator --rate 500 --mode mixed
```

Show the fraud simulator injecting velocity spikes, amount spikes, and geo anomalies.

---

## Minute 3 — Speed Layer: Real-Time Fraud Detection

Open **Terminal 2** — start Spark streaming:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  speed_layer/spark_streaming.py
```

Within ~10 seconds:
- Switch to **Redis Commander** → refresh → show keys appearing:
  - `fraud:alerts` — LIST with fraud alert JSON
  - `fraud:count:total` — counter incrementing
  - `fraud:top_users` — sorted set of flagged users
- Switch to **Kafka UI** → `fraud-alerts` topic → show alert messages published

Point out the **circuit breaker**: if fraud rate in a batch exceeds 20%, the Kafka sink pauses automatically (Redis key `fraud:circuit_breaker:triggered` with 5-minute TTL).

Three fraud rules firing:
| Rule | Trigger |
|---|---|
| Velocity | > 5 transactions in 60 seconds |
| Amount Spike | Amount > 3× user's 24h median |
| Geo Anomaly | Same user in 2 countries within 30 min |

---

## Minute 4 — Batch Layer + Observability

**Trigger the ingest DAG** in Airflow UI:
1. Go to http://localhost:8080
2. Enable `ingest_dag` → click  Run
3. Watch: `ingest_dag` → completes → auto-triggers `transform_dag`
4. `transform_dag` runs: `dbt run staging` → `dbt test` → `dbt run marts` → `dbt test`

After completion, open **Terminal 3**:

```bash
# Verify marts populated
PGPASSWORD=finflow123 psql -h localhost -p 5433 -U finflow -d finflow \
  -c "SELECT merchant, total_revenue_vnd, fraud_rate_pct FROM dbt_dev.mart_revenue_daily LIMIT 5;"
```

**Trigger the observability DAG**:
1. Enable `observability_dag` → click  Run
2. `schema_check` and `freshness_check` tasks run in **parallel**
3. Results stored in `datawatch.db` (DuckDB)

---

## Minute 5 — Serving Layer: Dashboard

Open **Terminal 4** — start FastAPI:

```bash
uvicorn serving.api.main:app --host 0.0.0.0 --port 8000
```

Open **Terminal 5** — start Streamlit:

```bash
streamlit run serving/dashboard/app.py --server.port 8501
```

Open http://localhost:8501 and walk through the 3 tabs:

** Fraud Alerts tab**
- Header: Total flagged count, Circuit Breaker status, Kafka health, Postgres health
- Table: Color-coded by fraud_score (red=3, orange=2, yellow=1)
- Sidebar: Top 10 flagged users by cumulative score

** Revenue tab**
- Bar chart: Total revenue by merchant (Grab, Shopee, VinMart, …)
- Line chart: Fraud rate trend by day

** Observability tab**
- Traffic lights: `raw_transactions` freshness vs 1h SLA, `mart_revenue_daily` vs 2h SLA
- Schema drift table: has_changes, is_breaking per table
- Data Debt Score: composite 0–100 score with letter grade (A→F)
- Auto-refreshes every 60 seconds

---

## Key Design Decisions (talking points)

| Decision | Why |
|---|---|
| **Lambda, not Kappa** | Batch layer handles historical reprocessing; speed layer handles sub-second latency. Kappa requires re-streaming all history for corrections. |
| **Kafka, not RabbitMQ** | Kafka retains messages (log retention), enabling the batch consumer to replay. RabbitMQ deletes on ack. |
| **DuckDB for observability** | No infra needed — single file, SQL interface, columnar for time-series aggregation. Perfect for a monitoring sidecar. |
| **Redis circuit breaker** | Prevents alert storms from overwhelming downstream consumers during fraud bursts. TTL of 5 minutes gives auto-recovery. |
| **dbt, not raw SQL** | Lineage, tests, documentation, and incremental materialisation in one tool. |

---

## Cleanup

```bash
make clean    # docker compose down -v (removes volumes + containers)
```
