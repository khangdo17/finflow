"""
Amount spike rule: flag transactions where the amount exceeds 3x the user's
median transaction value over a rolling 24-hour window.
Uses percentile_approx for streaming-friendly approximate median computation.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_amount_spike(df: DataFrame) -> DataFrame:
    """
    Compute per-user median amount over a 24-hour sliding window,
    then flag transactions where amount > 3x that median.
    Returns df with two new columns:
      - user_median_amount : approximate median for this user in the 24h window
      - amount_ratio       : amount / median (null-safe, 1.0 if no baseline yet)
      - amount_spike_flag  : True if amount_ratio > 3.0

    Watermark on tx_at must already be applied upstream.
    """
    # Compute approximate median per user over 24h window
    user_median = (
        df
        .groupBy(
            F.col("user_id"),
            F.window(F.col("tx_at"), "24 hours", "1 hour").alias("amt_window"),
        )
        .agg(
            F.percentile_approx("amount", 0.5).alias("user_median_amount")
        )
    )

    # Join back to get the median for each transaction
    enriched = (
        df.alias("tx")
        .join(
            user_median.alias("um"),
            (F.col("tx.user_id") == F.col("um.user_id")) &
            (F.col("tx.tx_at") >= F.col("um.amt_window.start")) &
            (F.col("tx.tx_at") < F.col("um.amt_window.end")),
            how="left",
        )
        .select(
            "tx.*",
            F.coalesce(F.col("um.user_median_amount"), F.col("tx.amount")).alias("user_median_amount"),
        )
    )

    # amount_ratio: how many times larger than median; default 1.0 when no baseline
    scored = (
        enriched
        .withColumn(
            "amount_ratio",
            F.round(
                F.col("amount") / F.nullif(F.col("user_median_amount"), F.lit(0.0)),
                4,
            ),
        )
        .withColumn(
            "amount_spike_flag",
            F.col("amount_ratio") > F.lit(3.0),
        )
    )

    return scored
