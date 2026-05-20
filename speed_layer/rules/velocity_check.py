"""
Velocity check rule: flag users with more than 5 transactions in any 60-second sliding window.
Sliding every 10 seconds gives fine-grained detection without waiting a full minute.
Scoring: count > 10 → velocity_score=2 (critical), count > 5 → velocity_score=1 (warning).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_velocity_check(df: DataFrame) -> DataFrame:
    """
    Join a per-user sliding-window count back onto the original transaction stream.
    Returns df with two new columns:
      - tx_count_60s   : number of transactions by this user in the preceding 60s window
      - velocity_score : 0 (clean), 1 (warning: >5), or 2 (critical: >10)

    Uses a 60-second window sliding every 10 seconds.
    Watermark on tx_at must already be applied upstream (spark_streaming.py).
    """
    # Aggregate: count transactions per user per 60s window, slide every 10s
    window_counts = (
        df
        .groupBy(
            F.col("user_id"),
            F.window(F.col("tx_at"), "60 seconds", "10 seconds").alias("vel_window"),
        )
        .agg(F.count("*").alias("tx_count_60s"))
    )

    # Join counts back to the original stream on user_id and tx_at inside the window
    enriched = (
        df.alias("tx")
        .join(
            window_counts.alias("wc"),
            (F.col("tx.user_id") == F.col("wc.user_id")) &
            (F.col("tx.tx_at") >= F.col("wc.vel_window.start")) &
            (F.col("tx.tx_at") < F.col("wc.vel_window.end")),
            how="left",
        )
        .select("tx.*", F.coalesce(F.col("wc.tx_count_60s"), F.lit(1)).alias("tx_count_60s"))
    )

    # Score: 2=critical (>10), 1=warning (>5), 0=clean
    scored = enriched.withColumn(
        "velocity_score",
        F.when(F.col("tx_count_60s") > 10, F.lit(2))
         .when(F.col("tx_count_60s") > 5,  F.lit(1))
         .otherwise(F.lit(0))
    )

    return scored
