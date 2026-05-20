"""
Observability layer for FinFlow.
schema_check: detects schema drift in Postgres tables against stored snapshots.
freshness_check: monitors data freshness against SLA thresholds.
Results are stored in DuckDB (datawatch.db) for historical analysis.
"""
