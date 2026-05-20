"""
Fraud detector: combines all three rules and produces a unified fraud_score (0-3).
Each rule contributes 1 point; a transaction triggering all three scores 3 (critical).
Only transactions with fraud_score >= 1 are forwarded to the sinks.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from speed_layer.rules.velocity_check import apply_velocity_check
from speed_layer.rules.amount_spike import apply_amount_spike
from speed_layer.rules.geo_anomaly import apply_geo_anomaly


def apply_all_rules(df: DataFrame) -> DataFrame:
    """
    Apply velocity, amount-spike, and geo-anomaly rules sequentially,
    then combine into a single fraud_score column (0–3).

    Pipeline:
      1. velocity_check  → adds tx_count_60s, velocity_score (0/1/2)
      2. amount_spike    → adds user_median_amount, amount_ratio, amount_spike_flag
      3. geo_anomaly     → adds geo_anomaly_flag

    Final columns added:
      - fraud_score      : int, 0–3 (one point per triggered rule)
      - is_flagged       : bool, True when fraud_score >= 1
      - triggered_rules  : comma-separated string of rule names that fired

    Only returns rows where is_flagged=True to keep sinks lean.
    """
    # Step 1 — velocity
    df = apply_velocity_check(df)

    # Step 2 — amount spike
    df = apply_amount_spike(df)

    # Step 3 — geo anomaly
    df = apply_geo_anomaly(df)

    # Combine scores: velocity contributes its score (0/1/2 capped to 1 for counting),
    # each of amount_spike and geo_anomaly contribute 1 point each.
    df = df.withColumn(
        "fraud_score",
        (F.when(F.col("velocity_score") > 0, F.lit(1)).otherwise(F.lit(0)))
        + F.when(F.col("amount_spike_flag") == True, F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("geo_anomaly_flag") == True, F.lit(1)).otherwise(F.lit(0)),
    )

    # Build a human-readable list of which rules fired
    df = df.withColumn(
        "triggered_rules",
        F.concat_ws(
            ",",
            F.when(F.col("velocity_score") > 0,      F.lit("velocity")).otherwise(F.lit(None).cast("string")),
            F.when(F.col("amount_spike_flag") == True, F.lit("amount_spike")).otherwise(F.lit(None).cast("string")),
            F.when(F.col("geo_anomaly_flag") == True,  F.lit("geo_anomaly")).otherwise(F.lit(None).cast("string")),
        ),
    )

    df = df.withColumn("is_flagged", F.col("fraud_score") >= 1)

    # Filter: only pass flagged transactions to the sinks
    return df.filter(F.col("is_flagged"))
