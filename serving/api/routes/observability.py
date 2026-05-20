"""
Observability routes — reads schema drift, freshness, and data debt from DuckDB.
GET /observability/freshness    — latest freshness result per table
GET /observability/schema-drift — latest schema diff per table
GET /observability/data-debt    — composite data debt score (0-100)
"""
import os
from typing import Any, Dict, List

import duckdb
from dotenv import load_dotenv
from fastapi import APIRouter
from loguru import logger

load_dotenv()

router = APIRouter()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "datawatch.db")


def _get_duckdb() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DUCKDB_PATH, read_only=True)


@router.get("/freshness")
def get_freshness() -> List[Dict[str, Any]]:
    """
    Return the most recent freshness check result for each watched table.
    Uses a window function to pick the latest row per source_name.
    Returns empty list if freshness_history table doesn't exist yet.
    """
    try:
        con = _get_duckdb()
        rows = con.execute(
            """
            SELECT source_name, checked_at, last_updated,
                   hours_since_update, sla_hours, status
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY source_name ORDER BY checked_at DESC
                       ) AS rn
                FROM freshness_history
            ) t
            WHERE rn = 1
            ORDER BY source_name
            """
        ).fetchall()
        con.close()
    except Exception as exc:
        logger.warning(f"DuckDB freshness query failed: {exc}")
        return []

    cols = ["source_name", "checked_at", "last_updated", "hours_since_update", "sla_hours", "status"]
    return [dict(zip(cols, row)) for row in rows]


@router.get("/schema-drift")
def get_schema_drift() -> List[Dict[str, Any]]:
    """
    Return the most recent schema diff for each watched table.
    Includes has_changes, is_breaking, and the full diff_json payload.
    """
    try:
        con = _get_duckdb()
        rows = con.execute(
            """
            SELECT source_name, checked_at, has_changes, is_breaking, diff_json
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY source_name ORDER BY checked_at DESC
                       ) AS rn
                FROM schema_diffs
            ) t
            WHERE rn = 1
            ORDER BY source_name
            """
        ).fetchall()
        con.close()
    except Exception as exc:
        logger.warning(f"DuckDB schema drift query failed: {exc}")
        return []

    cols = ["source_name", "checked_at", "has_changes", "is_breaking", "diff_json"]
    return [dict(zip(cols, row)) for row in rows]


@router.get("/data-debt")
def get_data_debt() -> Dict[str, Any]:
    """
    Compute a composite data debt score (0–100, higher = worse).
    Scoring:
      - Each stale table contributes 20 points.
      - Each warning table contributes 10 points.
      - Each breaking schema change contributes 25 points.
      - Each non-breaking schema change contributes 5 points.
    Capped at 100.
    """
    score = 0
    details: List[str] = []

    try:
        con = _get_duckdb()

        # Freshness debt
        fresh_rows = con.execute(
            """
            SELECT status FROM (
                SELECT status,
                       ROW_NUMBER() OVER (
                           PARTITION BY source_name ORDER BY checked_at DESC
                       ) AS rn
                FROM freshness_history
            ) t WHERE rn = 1
            """
        ).fetchall()
        for (status,) in fresh_rows:
            if status == "stale":
                score += 20
                details.append("stale table (+20)")
            elif status == "warning":
                score += 10
                details.append("warning table (+10)")

        # Schema drift debt
        drift_rows = con.execute(
            """
            SELECT has_changes, is_breaking FROM (
                SELECT has_changes, is_breaking,
                       ROW_NUMBER() OVER (
                           PARTITION BY source_name ORDER BY checked_at DESC
                       ) AS rn
                FROM schema_diffs
            ) t WHERE rn = 1
            """
        ).fetchall()
        for (has_changes, is_breaking) in drift_rows:
            if is_breaking:
                score += 25
                details.append("breaking schema change (+25)")
            elif has_changes:
                score += 5
                details.append("non-breaking schema change (+5)")

        con.close()
    except Exception as exc:
        logger.warning(f"DuckDB data-debt query failed: {exc}")

    score = min(score, 100)
    grade = "A" if score <= 10 else "B" if score <= 30 else "C" if score <= 50 else "D" if score <= 70 else "F"

    return {
        "data_debt_score": score,
        "grade": grade,
        "details": details,
    }
