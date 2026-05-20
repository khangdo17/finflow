# FinFlow — Directory Structure Reference

Use this file to verify Claude Code created all required files.

## Required Files Checklist

### Root
- [ ] `.env.example`
- [ ] `.gitignore`
- [ ] `CLAUDE.md`
- [ ] `DEMO.md`
- [ ] `Makefile`
- [ ] `README.md`
- [ ] `requirements.txt`

### infrastructure/
- [ ] `infrastructure/docker-compose.yml`
- [ ] `infrastructure/init-sql/01_create_tables.sql`

### terraform/
- [ ] `terraform/main.tf`
- [ ] `terraform/variables.tf`
- [ ] `terraform/outputs.tf`

### scripts/
- [ ] `scripts/verify_connections.py`

### data_generator/
- [ ] `data_generator/__init__.py`
- [ ] `data_generator/profiles.py`
- [ ] `data_generator/generator.py`
- [ ] `data_generator/fraud_simulator.py`

### speed_layer/
- [ ] `speed_layer/__init__.py`
- [ ] `speed_layer/spark_streaming.py`
- [ ] `speed_layer/fraud_detector.py`
- [ ] `speed_layer/rules/__init__.py`
- [ ] `speed_layer/rules/velocity_check.py`
- [ ] `speed_layer/rules/amount_spike.py`
- [ ] `speed_layer/rules/geo_anomaly.py`
- [ ] `speed_layer/sink/__init__.py`
- [ ] `speed_layer/sink/redis_sink.py`
- [ ] `speed_layer/sink/kafka_sink.py`

### batch_layer/
- [ ] `batch_layer/kafka_consumer.py`
- [ ] `batch_layer/dags/example_dag.py`
- [ ] `batch_layer/dags/ingest_dag.py`
- [ ] `batch_layer/dags/transform_dag.py`
- [ ] `batch_layer/dags/observability_dag.py`

### batch_layer/dbt/
- [ ] `batch_layer/dbt/finflow/dbt_project.yml`
- [ ] `batch_layer/dbt/finflow/profiles.yml`
- [ ] `batch_layer/dbt/finflow/packages.yml`
- [ ] `batch_layer/dbt/finflow/models/staging/sources.yml`
- [ ] `batch_layer/dbt/finflow/models/staging/stg_transactions.sql`
- [ ] `batch_layer/dbt/finflow/models/intermediate/int_user_daily_summary.sql`
- [ ] `batch_layer/dbt/finflow/models/intermediate/int_merchant_revenue.sql`
- [ ] `batch_layer/dbt/finflow/models/marts/schema.yml`
- [ ] `batch_layer/dbt/finflow/models/marts/mart_revenue_daily.sql`
- [ ] `batch_layer/dbt/finflow/models/marts/mart_fraud_report.sql`
- [ ] `batch_layer/dbt/finflow/tests/assert_no_negative_amount.sql`
- [ ] `batch_layer/dbt/finflow/tests/assert_fraud_rate_reasonable.sql`

### observability/
- [ ] `observability/__init__.py`
- [ ] `observability/schema_check.py`
- [ ] `observability/freshness_check.py`

### serving/
- [ ] `serving/__init__.py`
- [ ] `serving/api/__init__.py`
- [ ] `serving/api/main.py`
- [ ] `serving/api/routes/__init__.py`
- [ ] `serving/api/routes/fraud.py`
- [ ] `serving/api/routes/revenue.py`
- [ ] `serving/api/routes/health.py`
- [ ] `serving/api/routes/observability.py`
- [ ] `serving/dashboard/app.py`

### tests/
- [ ] `tests/test_generator.py`
- [ ] `tests/test_speed_layer.py`

### .github/
- [ ] `.github/workflows/ci.yml`

---

## Verification Commands

After Claude Code finishes building, run these in order:

```bash
# 1. Check all files exist
find . -type f -name "*.py" | sort
find . -type f -name "*.yml" | sort
find . -type f -name "*.sql" | sort

# 2. Check docker-compose has no duplicate keys
grep -n "^services:\|^volumes:" infrastructure/docker-compose.yml
# Must show exactly 2 lines

# 3. Check Makefile has tabs
cat -A Makefile | grep "^\^I" | wc -l
# Must be > 0

# 4. Start infrastructure
make up

# 5. Verify connections
make verify
# Must show: Kafka OK, Postgres OK, Redis OK

# 6. Run unit tests
pytest tests/ -v

# 7. Test generator
cd data_generator && python generator.py --mode mixed --rate 50
# Must see: "Sent 100 tx | Rate: ~50 tx/min"

# 8. Test API
uvicorn serving.api.main:app --port 8000 &
curl http://localhost:8000/health
# Must return: {"status": "healthy", ...}
```

---

## Port Reference

| Port | Service |
|---|---|
| 8080 | Airflow UI |
| 8090 | Kafka UI |
| 8091 | Redis Commander |
| 9092 | Kafka broker (external) |
| 5433 | Postgres (host → container 5432) |
| 6379 | Redis |
| 8000 | FastAPI |
| 8501 | Streamlit |
| 4040 | Spark UI |
| 8093 | dbt docs (optional) |
