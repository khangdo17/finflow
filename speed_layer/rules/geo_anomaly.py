"""
Geo anomaly rule: flag transactions where the same user appears in two different
countries within a 30-minute window (physically impossible travel scenario).
Uses a self-join on user_id with a time-range condition.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_geo_anomaly(df: DataFrame) -> DataFrame:
    """
    Self-join the stream on user_id to find pairs of transactions from
    different countries within 30 minutes of each other.

    Returns df with one new column:
      - geo_anomaly_flag : True if a cross-country pair was detected for this tx

    Join condition:
      tx2.tx_at BETWEEN tx1.tx_at AND tx1.tx_at + INTERVAL 30 MINUTES
      AND tx1.country != tx2.country

    Watermark must already be applied upstream; both sides use the same watermark
    so Spark can safely drop old state.
    """
    # Left side: candidate transactions
    left = df.alias("tx1")

    # Right side: the same stream re-aliased for self-join
    # We only keep the columns we need from the right side to reduce shuffle size
    right = df.select(
        F.col("user_id").alias("r_user_id"),
        F.col("tx_at").alias("r_tx_at"),
        F.col("country").alias("r_country"),
        F.col("tx_id").alias("r_tx_id"),
    ).alias("tx2")

    geo_pairs = (
        left.join(
            right,
            (F.col("tx1.user_id") == F.col("tx2.r_user_id")) &
            (F.col("tx1.tx_id") != F.col("tx2.r_tx_id")) &
            (F.col("tx1.country") != F.col("tx2.r_country")) &
            (F.col("tx2.r_tx_at") >= F.col("tx1.tx_at")) &
            (F.col("tx2.r_tx_at") <= F.col("tx1.tx_at") + F.expr("INTERVAL 30 MINUTES")),
            how="left",
        )
        .select(
            "tx1.*",
            F.col("tx2.r_tx_id").alias("_geo_pair_tx_id"),
        )
        .withColumn(
            "geo_anomaly_flag",
            F.col("_geo_pair_tx_id").isNotNull(),
        )
        .drop("_geo_pair_tx_id")
    )

    return geo_pairs
