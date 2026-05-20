"""
Data freshness monitor for FinFlow observability layer.
Checks when each watched table was last updated and compares against SLA thresholds.
Sends Slack alerts for stale tables (hours_since_update > sla_hours).
Results are persisted in DuckDB freshness_history table.
"""
import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import duckdb
import psycopg2
import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "datawatch.db")

# SLA config: table_name → max hours before stale
SLA_CONFIG: Dict[str, float] = {
    "raw_transactions": 1.0,
    "mart_revenue_daily": 2.0,
}

# Status thresholds based on hours_since_update / sla_hours ratio
# ratio ≤ 0.75 → fresh, ≤ 1.0 → warning, > 1.0 → stale
_FRESH_RATIO = 0.75
_WARNING_RATIO = 1.0


@dataclass
class FreshnessResult:
    """Result of a freshness check for a single table."""
    source_name: str
    checked_at: datetime
    last_updated: Optional[datetime]
    hours_since_update: float
    sla_hours: float

    @property
    def status(self) -> str:
        """
        Derive status from the ratio of hours elapsed to SLA budget.
        Returns 'fresh', 'warning', or 'stale'.
        """
        if self.last_updated is None:
            return "stale"
        ratio = self.hours_since_update / self.sla_hours if self.sla_hours > 0 else float("inf")
        if ratio <= _FRESH_RATIO:
            return "fresh"
        if ratio <= _WARNING_RATIO:
            return "warning"
        return "stale"


def _get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "finflow_db"),
        user=os.getenv("POSTGRES_USER", "finflow"),
        password=os.getenv("POSTGRES_PASSWORD", "finflow_pass"),
    )


def _init_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    """Ensure freshness_history table exists (schema_check may have already created it)."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS freshness_history (
            source_name        VARCHAR,
            checked_at         TIMESTAMP,
            last_updated       TIMESTAMP,
            hours_since_update DOUBLE,
            sla_hours          DOUBLE,
            status             VARCHAR
        )
    """)


def check_postgres_freshness(table_name: str, sla_hours: float) -> FreshnessResult:
    """
    Query the table for its most recent record timestamp.
    Uses MAX(ingested_at) when available, falls back to MAX(created_at),
    then MAX(tx_at) for tables without an ingestion timestamp.
    """
    now = datetime.utcnow()

    timestamp_columns = {
        "raw_transactions": "ingested_at",
        "mart_revenue_daily": "updated_at",
    }
    ts_col = timestamp_columns.get(table_name, "created_at")

    conn = _get_pg_conn()
    last_updated: Optional[datetime] = None
    try:
        with conn.cursor() as cur:
            # Try the expected timestamp column; fall back gracefully
            try:
                cur.execute(f"SELECT MAX({ts_col}) FROM {table_name}")  # noqa: S608
                result = cur.fetchone()
                last_updated = result[0] if result and result[0] else None
            except psycopg2.Error:
                conn.rollback()
                logger.warning(f"Column {ts_col} not found on {table_name}, using NOW() as fallback")
                last_updated = None
    finally:
        conn.close()

    hours_since = (
        (now - last_updated).total_seconds() / 3600.0
        if last_updated is not None
        else float("inf")
    )

    return FreshnessResult(
        source_name=table_name,
        checked_at=now,
        last_updated=last_updated,
        hours_since_update=hours_since,
        sla_hours=sla_hours,
    )


def send_slack_alert(result: FreshnessResult) -> None:
    """
    POST a Slack message via SLACK_WEBHOOK_URL when a table is stale.
    Skips gracefully if SLACK_WEBHOOK_URL is not set — no exception raised.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.debug("SLACK_WEBHOOK_URL not set — skipping Slack alert")
        return

    message = {
        "text": (
            f":red_circle: *FinFlow Data Freshness Alert*\n"
            f"Table `{result.source_name}` is *{result.status.upper()}*\n"
            f"Last updated: {result.last_updated or 'never'}\n"
            f"Hours since update: {result.hours_since_update:.1f}h (SLA: {result.sla_hours}h)"
        )
    }
    try:
        resp = requests.post(webhook_url, json=message, timeout=10)
        resp.raise_for_status()
        logger.info(f"Slack alert sent for {result.source_name}")
    except requests.RequestException as exc:
        logger.error(f"Failed to send Slack alert: {exc}")


def run_freshness_checks() -> List[FreshnessResult]:
    """
    Main entry point for freshness monitoring.
    Checks every table in SLA_CONFIG, persists results in DuckDB,
    and sends Slack alerts for stale tables.
    Returns list of FreshnessResult objects.
    """
    con = duckdb.connect(DUCKDB_PATH)
    _init_duckdb(con)

    results: List[FreshnessResult] = []

    for table_name, sla_hours in SLA_CONFIG.items():
        logger.info(f"Freshness check: {table_name} (SLA={sla_hours}h)")
        try:
            result = check_postgres_freshness(table_name, sla_hours)
        except Exception as exc:
            logger.error(f"Freshness check failed for {table_name}: {exc}")
            continue

        logger.info(
            f"{table_name}: status={result.status}, "
            f"hours_since_update={result.hours_since_update:.2f}h"
        )

        con.execute(
            "INSERT INTO freshness_history VALUES (?, ?, ?, ?, ?, ?)",
            [
                result.source_name,
                result.checked_at,
                result.last_updated,
                result.hours_since_update,
                result.sla_hours,
                result.status,
            ],
        )

        if result.status == "stale":
            send_slack_alert(result)

        results.append(result)

    con.close()
    return results
