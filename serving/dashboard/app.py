"""
FinFlow Streamlit dashboard.
3 tabs: Fraud Alerts, Revenue, Observability.
Reads all data from the FastAPI backend (localhost:8000).
The Observability tab auto-refreshes every 60 seconds via st.rerun().
Run with: streamlit run serving/dashboard/app.py --server.port 8501
"""
import time
from typing import Any, Dict, List

import requests
import streamlit as st

API_BASE = "http://localhost:8000"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(path: str, params: Dict = None) -> Any:
    """Call the FastAPI backend. Returns parsed JSON or None on error."""
    try:
        resp = requests.get(f"{API_BASE}{path}", params=params, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        st.error(f"API error ({path}): {exc}")
        return None


def _status_label(status: str) -> str:
    return {"ok": "[OK]", "fresh": "[OK]", "warning": "[WARN]", "stale": "[STALE]", "degraded": "[DEGRADED]", "error": "[ERROR]"}.get(
        status, "[?]"
    )


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="FinFlow — Fraud & Revenue Dashboard",
    layout="wide",
)

st.title("FinFlow — Real-Time Fraud & Revenue Dashboard")

# ---------------------------------------------------------------------------
# Header metrics row — always visible regardless of tab
# ---------------------------------------------------------------------------

health = _get("/health") or {}
fraud_stats = _get("/fraud/stats") or {}

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        label="Total Fraud Alerts",
        value=fraud_stats.get("total_fraud_alerts", "—"),
    )
with col2:
    cb_active = fraud_stats.get("circuit_breaker_active", False)
    st.metric(
        label="Circuit Breaker",
        value="ACTIVE" if cb_active else "OK",
    )
with col3:
    kafka_ok = health.get("kafka", {}).get("status") == "ok"
    st.metric(
        label="Kafka",
        value="OK" if kafka_ok else "DOWN",
    )
with col4:
    pg_ok = health.get("postgres", {}).get("status") == "ok"
    st.metric(
        label="Postgres",
        value="OK" if pg_ok else "DOWN",
    )

st.divider()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_fraud, tab_revenue, tab_obs = st.tabs(["Fraud Alerts", "Revenue", "Observability"])


# ── Tab 1: Fraud Alerts ────────────────────────────────────────────────────

with tab_fraud:
    st.subheader("Recent Fraud Alerts")

    limit = st.slider("Number of alerts to display", min_value=10, max_value=500, value=50, step=10)
    alerts: List[Dict] = _get("/fraud/alerts", params={"limit": limit}) or []

    if alerts:
        import pandas as pd

        df = pd.DataFrame(alerts)

        # Colour rows by fraud_score: 3=red, 2=orange, 1=yellow
        def _row_color(score: int) -> str:
            return {3: "background-color: #ff4b4b", 2: "background-color: #ffa500",
                    1: "background-color: #ffd700"}.get(score, "")

        styled = df.style.apply(
            lambda row: [_row_color(row.get("fraud_score", 0))] * len(row), axis=1
        )
        st.dataframe(styled, use_container_width=True)
    else:
        st.info("No fraud alerts found. Is the speed layer running?")

    # ── Fraud Trend (last 24h) ─────────────────────────────────────────────
    st.subheader("Fraud Trend — Last 24 Hours")
    trend: List[Dict] = _get("/fraud/trend") or []
    if trend:
        import pandas as pd

        df_trend = pd.DataFrame(trend)
        df_trend["hour"] = pd.to_datetime(df_trend["hour"])
        df_trend = df_trend.sort_values("hour")
        st.line_chart(
            df_trend.set_index("hour")[["count"]],
            use_container_width=True,
        )
    else:
        st.info("No trend data yet — alerts appear here once the speed layer emits fraud events.")

    # ── Transaction Volume Heatmap ─────────────────────────────────────────
    st.subheader("Fraud Alert Heatmap (Day x Hour)")
    heatmap_data: List[Dict] = _get("/fraud/heatmap") or []
    if heatmap_data:
        import pandas as pd
        import plotly.graph_objects as go

        df_hm = pd.DataFrame(heatmap_data)
        DAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        df_pivot = df_hm.pivot(index="day", columns="hour", values="count").reindex(DAY_ORDER)

        fig_hm = go.Figure(
            data=go.Heatmap(
                z=df_pivot.values.tolist(),
                x=[f"{h:02d}:00" for h in range(24)],
                y=DAY_ORDER,
                colorscale="Reds",
                showscale=True,
                hovertemplate="Day: %{y}<br>Hour: %{x}<br>Alerts: %{z}<extra></extra>",
            )
        )
        fig_hm.update_layout(
            xaxis_title="Hour of Day",
            yaxis_title="Day of Week",
            height=300,
            margin=dict(l=60, r=20, t=20, b=40),
        )
        st.plotly_chart(fig_hm, use_container_width=True)
    else:
        st.info("No heatmap data yet.")

    # ── Top Flagged Users ──────────────────────────────────────────────────
    st.subheader("Top Flagged Users")
    top_users: List[Dict] = fraud_stats.get("top_flagged_users", [])
    if top_users:
        import pandas as pd
        st.dataframe(pd.DataFrame(top_users), use_container_width=True)
    else:
        st.info("No top users data yet.")


# ── Tab 2: Revenue ─────────────────────────────────────────────────────────

with tab_revenue:
    st.subheader("Revenue by Merchant")

    by_merchant: List[Dict] = _get("/revenue/by-merchant") or []
    if by_merchant:
        import pandas as pd

        df_merchant = pd.DataFrame(by_merchant)
        df_merchant["total_revenue_vnd"] = df_merchant["total_revenue_vnd"].astype(float)
        df_merchant["avg_fraud_rate_pct"] = df_merchant["avg_fraud_rate_pct"].astype(float)

        # ── Grouped bar: Revenue vs Fraud Rate ────────────────────────────
        import plotly.graph_objects as go

        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(
            name="Revenue (VND)",
            x=df_merchant["merchant"],
            y=df_merchant["total_revenue_vnd"],
            yaxis="y1",
            marker_color="#4C78A8",
        ))
        fig_bar.add_trace(go.Bar(
            name="Avg Fraud Rate (%)",
            x=df_merchant["merchant"],
            y=df_merchant["avg_fraud_rate_pct"],
            yaxis="y2",
            marker_color="#E45756",
        ))
        fig_bar.update_layout(
            barmode="group",
            yaxis=dict(title="Total Revenue (VND)", side="left"),
            yaxis2=dict(title="Avg Fraud Rate (%)", side="right", overlaying="y"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=400,
            margin=dict(l=60, r=60, t=40, b=60),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        st.dataframe(df_merchant, use_container_width=True)
    else:
        st.info("No revenue data yet. Has the batch layer run dbt?")

    st.subheader("Fraud Rate Trend by Day")
    daily: List[Dict] = _get("/revenue/daily", params={"limit": 30}) or []
    if daily:
        import pandas as pd

        df_daily = pd.DataFrame(daily)
        if "date" in df_daily.columns and "fraud_rate_pct" in df_daily.columns:
            df_daily["date"] = pd.to_datetime(df_daily["date"])
            df_daily = df_daily.sort_values("date")
            st.line_chart(
                df_daily.set_index("date")[["fraud_rate_pct"]],
                use_container_width=True,
            )
    else:
        st.info("No daily revenue data yet.")

    # ── Geo Map ───────────────────────────────────────────────────────────
    st.subheader("Transaction Distribution by Country")
    geo_data: List[Dict] = _get("/revenue/geo") or []
    if geo_data:
        import pandas as pd
        import plotly.express as px

        df_geo = pd.DataFrame(geo_data)
        df_geo["total_tx"] = df_geo["total_tx"].astype(float)
        df_geo["total_revenue_vnd"] = df_geo["total_revenue_vnd"].astype(float)
        df_geo["avg_fraud_rate_pct"] = df_geo["avg_fraud_rate_pct"].astype(float).round(2)

        ALPHA2_TO_ALPHA3 = {
            "VN": "VNM", "US": "USA", "SG": "SGP", "JP": "JPN",
            "KR": "KOR", "CN": "CHN", "TH": "THA", "MY": "MYS",
            "AU": "AUS", "GB": "GBR",
        }
        df_geo["iso_alpha3"] = df_geo["country_code"].map(ALPHA2_TO_ALPHA3)

        fig_geo = px.choropleth(
            df_geo,
            locations="iso_alpha3",
            color="total_tx",
            hover_name="country_code",
            hover_data={
                "total_tx": True,
                "total_revenue_vnd": True,
                "avg_fraud_rate_pct": True,
                "country_code": False,
            },
            color_continuous_scale="Reds",
            labels={
                "total_tx": "Transactions",
                "total_revenue_vnd": "Revenue (VND)",
                "avg_fraud_rate_pct": "Fraud Rate (%)",
            },
            title="",
        )
        fig_geo.update_layout(
            geo=dict(showframe=False, showcoastlines=True, projection_type="natural earth"),
            height=420,
            margin=dict(l=0, r=0, t=10, b=0),
            coloraxis_colorbar=dict(title="Transactions"),
        )
        st.plotly_chart(fig_geo, use_container_width=True)
    else:
        st.info("No geo data yet.")


# ── Tab 3: Observability ───────────────────────────────────────────────────

with tab_obs:
    st.subheader("Data Freshness")

    freshness: List[Dict] = _get("/observability/freshness") or []
    if freshness:
        for item in freshness:
            status = item.get("status", "unknown")
            label_prefix = _status_label(status)
            hours = item.get("hours_since_update")
            sla = item.get("sla_hours")
            hours_str = f"{hours:.2f}h" if hours is not None else "N/A"
            st.metric(
                label=f"{label_prefix} {item['source_name']}",
                value=status.upper(),
                delta=f"{hours_str} / {sla}h SLA",
                delta_color="inverse" if status == "stale" else "normal",
            )
    else:
        st.info("No freshness data yet.")

    st.subheader("Schema Drift")
    drift: List[Dict] = _get("/observability/schema-drift") or []
    if drift:
        import pandas as pd
        st.dataframe(pd.DataFrame(drift), use_container_width=True)
    else:
        st.info("No schema drift data yet.")

    st.subheader("Data Debt Score")
    debt: Dict = _get("/observability/data-debt") or {}
    if debt:
        score = debt.get("data_debt_score", 0)
        grade = debt.get("grade", "—")
        details = debt.get("details", [])

        col_score, col_grade = st.columns(2)
        with col_score:
            st.metric("Data Debt Score", f"{score} / 100")
        with col_grade:
            st.metric("Grade", grade)

        if details:
            st.caption("Contributing factors: " + ", ".join(details))

    # Auto-refresh every 60 seconds
    time.sleep(60)
    st.rerun()
