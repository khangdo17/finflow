"""
Revenue analytics routes — reads from Postgres dbt mart tables.
GET /revenue/daily         — mart_revenue_daily rows (optional date filter)
GET /revenue/by-merchant   — aggregated totals grouped by merchant
"""
import os
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Query
from loguru import logger

load_dotenv()

router = APIRouter()


def _get_pg_conn():
    """Connect to Postgres using host-machine port 5433."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "finflow_db"),
        user=os.getenv("POSTGRES_USER", "finflow"),
        password=os.getenv("POSTGRES_PASSWORD", "finflow_pass"),
    )


@router.get("/daily")
def get_revenue_daily(
    limit: int = Query(default=30, ge=1, le=365, description="Number of recent days"),
    merchant: Optional[str] = Query(default=None, description="Filter by merchant name"),
) -> List[Dict[str, Any]]:
    """
    Return rows from mart_revenue_daily ordered by date descending.
    Optionally filter by merchant name.
    Falls back gracefully if the mart table doesn't exist yet (dbt not yet run).
    """
    try:
        conn = _get_pg_conn()
    except psycopg2.OperationalError as exc:
        logger.error(f"Postgres connection failed: {exc}")
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if merchant:
                cur.execute(
                    """
                    SELECT * FROM dbt_dev.mart_revenue_daily
                    WHERE merchant = %s
                    ORDER BY date DESC
                    LIMIT %s
                    """,
                    (merchant, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM dbt_dev.mart_revenue_daily
                    ORDER BY date DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            rows = cur.fetchall()
    except psycopg2.Error as exc:
        logger.warning(f"mart_revenue_daily query failed (table may not exist yet): {exc}")
        return []
    finally:
        conn.close()

    return [dict(row) for row in rows]


@router.get("/geo")
def get_revenue_geo() -> List[Dict[str, Any]]:
    """
    Return transaction count and total revenue grouped by country_code.
    Includes approximate lat/lon centroid for pydeck map rendering.
    """
    COUNTRY_COORDS = {
        "VN": (14.0583, 108.2772),
        "US": (37.0902, -95.7129),
        "SG": (1.3521, 103.8198),
        "JP": (36.2048, 138.2529),
        "KR": (35.9078, 127.7669),
        "CN": (35.8617, 104.1954),
        "TH": (15.8700, 100.9925),
        "MY": (4.2105, 101.9758),
        "AU": (-25.2744, 133.7751),
        "GB": (55.3781, -3.4360),
    }
    try:
        conn = _get_pg_conn()
    except psycopg2.OperationalError as exc:
        logger.error(f"Postgres connection failed: {exc}")
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    country_code,
                    SUM(tx_count)          AS total_tx,
                    SUM(total_revenue_vnd) AS total_revenue_vnd,
                    AVG(fraud_rate_pct)    AS avg_fraud_rate_pct
                FROM dbt_dev.mart_revenue_daily
                WHERE country_code IS NOT NULL
                GROUP BY country_code
                ORDER BY total_tx DESC
                """
            )
            rows = cur.fetchall()
    except psycopg2.Error as exc:
        logger.warning(f"geo query failed: {exc}")
        return []
    finally:
        conn.close()

    result = []
    for row in rows:
        d = dict(row)
        cc = d.get("country_code", "")
        lat, lon = COUNTRY_COORDS.get(cc, (0.0, 0.0))
        d["lat"] = lat
        d["lon"] = lon
        result.append(d)
    return result


@router.get("/by-merchant")
def get_revenue_by_merchant() -> List[Dict[str, Any]]:
    """
    Return total revenue aggregated by merchant across all time.
    Reads from mart_revenue_daily, grouping by merchant.
    """
    try:
        conn = _get_pg_conn()
    except psycopg2.OperationalError as exc:
        logger.error(f"Postgres connection failed: {exc}")
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    merchant,
                    SUM(tx_count)          AS total_tx,
                    SUM(total_revenue_vnd) AS total_revenue_vnd,
                    AVG(fraud_rate_pct)    AS avg_fraud_rate_pct
                FROM dbt_dev.mart_revenue_daily
                GROUP BY merchant
                ORDER BY total_revenue_vnd DESC
                """
            )
            rows = cur.fetchall()
    except psycopg2.Error as exc:
        logger.warning(f"by-merchant query failed: {exc}")
        return []
    finally:
        conn.close()

    return [dict(row) for row in rows]
