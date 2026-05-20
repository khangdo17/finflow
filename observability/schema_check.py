"""
Schema drift detector for FinFlow observability layer.
Compares the current Postgres schema against the last stored snapshot.
Classifies changes as breaking (column removed/type changed) or non-breaking (column added).
Results are persisted in DuckDB datawatch.db for historical trending.
"""
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import duckdb
import psycopg2
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "datawatch.db")

WATCHED_TABLES = ["raw_transactions", "fraud_alerts"]

BREAKING_CHANGE_TYPES = {"removed", "type_changed"}


@dataclass
class ColumnInfo:
    """Metadata for a single Postgres column."""
    name: str
    data_type: str
    is_nullable: bool
    column_default: Optional[str]


@dataclass
class SchemaDiff:
    """Result of comparing two schema snapshots."""
    source_name: str
    checked_at: datetime
    has_changes: bool
    is_breaking: bool
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    type_changed: List[Dict] = field(default_factory=list)


def _get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "finflow_db"),
        user=os.getenv("POSTGRES_USER", "finflow"),
        password=os.getenv("POSTGRES_PASSWORD", "finflow_pass"),
    )


def _init_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    """Create DuckDB observability tables if they don't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS schema_snapshots (
            source_name  VARCHAR,
            snapshot_at  TIMESTAMP,
            schema_json  VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS schema_diffs (
            source_name  VARCHAR,
            checked_at   TIMESTAMP,
            has_changes  BOOLEAN,
            is_breaking  BOOLEAN,
            diff_json    VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS freshness_history (
            source_name       VARCHAR,
            checked_at        TIMESTAMP,
            last_updated      TIMESTAMP,
            hours_since_update DOUBLE,
            sla_hours         DOUBLE,
            status            VARCHAR
        )
    """)


def get_postgres_schema(table_name: str) -> Dict[str, ColumnInfo]:
    """
    Query information_schema.columns for the given table.
    Returns a dict keyed by column_name for O(1) lookups during diff.
    """
    conn = _get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
                """,
                (table_name,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    return {
        row[0]: ColumnInfo(
            name=row[0],
            data_type=row[1],
            is_nullable=(row[2] == "YES"),
            column_default=row[3],
        )
        for row in rows
    }


def diff_schemas(
    table_name: str,
    current: Dict[str, ColumnInfo],
    previous: Dict[str, ColumnInfo],
) -> SchemaDiff:
    """
    Compare current schema against previous snapshot.
    Breaking changes: column removed, or data_type changed.
    Non-breaking: column added.
    """
    now = datetime.utcnow()
    added = [col for col in current if col not in previous]
    removed = [col for col in previous if col not in current]
    type_changed = [
        {"column": col, "from": previous[col].data_type, "to": current[col].data_type}
        for col in current
        if col in previous and current[col].data_type != previous[col].data_type
    ]

    has_changes = bool(added or removed or type_changed)
    is_breaking = bool(removed or type_changed)

    return SchemaDiff(
        source_name=table_name,
        checked_at=now,
        has_changes=has_changes,
        is_breaking=is_breaking,
        added=added,
        removed=removed,
        type_changed=type_changed,
    )


def run_schema_check() -> List[SchemaDiff]:
    """
    Main entry point for schema drift detection.
    For each watched table:
      1. Fetch current schema from Postgres.
      2. Load last snapshot from DuckDB.
      3. Diff and persist both snapshot and diff result.
    Returns list of SchemaDiff objects for all tables.
    """
    con = duckdb.connect(DUCKDB_PATH)
    _init_duckdb(con)

    results: List[SchemaDiff] = []

    for table_name in WATCHED_TABLES:
        logger.info(f"Schema check: {table_name}")
        try:
            current_schema = get_postgres_schema(table_name)
        except Exception as exc:
            logger.error(f"Could not fetch schema for {table_name}: {exc}")
            continue

        current_json = json.dumps(
            {col: vars(info) for col, info in current_schema.items()}
        )

        # Load last snapshot
        row = con.execute(
            """
            SELECT schema_json FROM schema_snapshots
            WHERE source_name = ?
            ORDER BY snapshot_at DESC LIMIT 1
            """,
            [table_name],
        ).fetchone()

        if row is None:
            logger.info(f"No prior snapshot for {table_name} — storing baseline.")
            diff = SchemaDiff(
                source_name=table_name,
                checked_at=datetime.utcnow(),
                has_changes=False,
                is_breaking=False,
            )
        else:
            previous_raw = json.loads(row[0])
            previous_schema = {
                col: ColumnInfo(**info) for col, info in previous_raw.items()
            }
            diff = diff_schemas(table_name, current_schema, previous_schema)

        # Persist snapshot
        con.execute(
            "INSERT INTO schema_snapshots VALUES (?, ?, ?)",
            [table_name, datetime.utcnow(), current_json],
        )

        # Persist diff
        diff_payload = {
            "added": diff.added,
            "removed": diff.removed,
            "type_changed": diff.type_changed,
        }
        con.execute(
            "INSERT INTO schema_diffs VALUES (?, ?, ?, ?, ?)",
            [
                diff.source_name,
                diff.checked_at,
                diff.has_changes,
                diff.is_breaking,
                json.dumps(diff_payload),
            ],
        )

        if diff.is_breaking:
            logger.warning(
                f"BREAKING schema change detected on {table_name}: "
                f"removed={diff.removed}, type_changed={diff.type_changed}"
            )
        elif diff.has_changes:
            logger.info(f"Non-breaking schema change on {table_name}: added={diff.added}")
        else:
            logger.info(f"No schema changes on {table_name}")

        results.append(diff)

    con.close()
    return results
