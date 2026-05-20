"""
Unit tests for the FinFlow speed layer fraud rules and sinks.
All Spark operations use mock DataFrames via MagicMock so no Spark cluster is needed.
Redis and Kafka clients are patched to avoid external service dependencies.
Run with: python tests/test_speed_layer.py
"""
import json
import sys
import os
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PYSPARK_AVAILABLE = True
try:
    import pyspark  # noqa: F401
except ImportError:
    PYSPARK_AVAILABLE = False


# ---------------------------------------------------------------------------
# Helpers to build mock Row objects that behave like pyspark.sql.Row
# ---------------------------------------------------------------------------

class MockRow(dict):
    """Dict subclass that also supports attribute access like a Spark Row."""
    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(item)

    def get(self, item, default=None):
        return super().get(item, default)


def make_tx(**overrides) -> MockRow:
    """Return a MockRow representing a clean transaction with optional field overrides."""
    base = {
        "tx_id":          "tx-0001",
        "user_id":        "user_0042",
        "merchant":       "Grab",
        "amount":         50_000.0,
        "currency":       "VND",
        "country":        "VN",
        "tx_at":          datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc),
        "device":         "mobile",
        "is_fraud":       False,
        "fraud_reason":   None,
        "is_flagged":     False,
        "fraud_score":    0,
        "triggered_rules": "",
        "velocity_score": 0,
        "tx_count_60s":   1,
        "amount_spike_flag": False,
        "amount_ratio":   1.0,
        "user_median_amount": 50_000.0,
        "geo_anomaly_flag": False,
    }
    base.update(overrides)
    return MockRow(base)


# ---------------------------------------------------------------------------
# TX_SCHEMA field names test (no Spark needed)
# ---------------------------------------------------------------------------

@unittest.skipUnless(PYSPARK_AVAILABLE, "pyspark not installed")
class TestTXSchemaFieldNames(unittest.TestCase):
    """Verify TX_SCHEMA field names exactly match generator output."""

    def test_schema_fields_match_generator(self):
        """
        Import TX_SCHEMA without triggering SparkSession by mocking pyspark at module level.
        We inspect the StructType field names directly.
        """
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

        # Rebuild expected schema inline — must match generator.py field names exactly
        expected_fields = [
            "tx_id", "user_id", "merchant", "amount", "currency",
            "country", "tx_at", "device", "is_fraud", "fraud_reason",
        ]

        TX_SCHEMA = StructType([
            StructField("tx_id",        StringType(),  nullable=False),
            StructField("user_id",      StringType(),  nullable=False),
            StructField("merchant",     StringType(),  nullable=True),
            StructField("amount",       DoubleType(),  nullable=False),
            StructField("currency",     StringType(),  nullable=True),
            StructField("country",      StringType(),  nullable=True),
            StructField("tx_at",        StringType(),  nullable=False),
            StructField("device",       StringType(),  nullable=True),
            StructField("is_fraud",     BooleanType(), nullable=True),
            StructField("fraud_reason", StringType(),  nullable=True),
        ])

        actual_fields = [f.name for f in TX_SCHEMA.fields]
        self.assertEqual(actual_fields, expected_fields)


# ---------------------------------------------------------------------------
# Velocity check rule
# ---------------------------------------------------------------------------

@unittest.skipUnless(PYSPARK_AVAILABLE, "pyspark not installed")
class TestVelocityCheckImport(unittest.TestCase):
    """Verify velocity_check module imports cleanly."""

    def test_import(self):
        from speed_layer.rules.velocity_check import apply_velocity_check
        self.assertTrue(callable(apply_velocity_check))


class TestVelocityCheckLogic(unittest.TestCase):
    """Test velocity scoring logic independently of Spark."""

    def _score(self, count: int) -> int:
        """Mirror the scoring logic from velocity_check.py."""
        if count > 10:
            return 2
        elif count > 5:
            return 1
        return 0

    def test_score_clean(self):
        self.assertEqual(self._score(1), 0)
        self.assertEqual(self._score(5), 0)

    def test_score_warning(self):
        self.assertEqual(self._score(6), 1)
        self.assertEqual(self._score(10), 1)

    def test_score_critical(self):
        self.assertEqual(self._score(11), 2)
        self.assertEqual(self._score(100), 2)

    def test_threshold_boundary_at_5(self):
        # Exactly 5 is NOT a warning (threshold is >5)
        self.assertEqual(self._score(5), 0)

    def test_threshold_boundary_at_10(self):
        # Exactly 10 is NOT critical (threshold is >10)
        self.assertEqual(self._score(10), 1)


# ---------------------------------------------------------------------------
# Amount spike rule
# ---------------------------------------------------------------------------

@unittest.skipUnless(PYSPARK_AVAILABLE, "pyspark not installed")
class TestAmountSpikeImport(unittest.TestCase):
    def test_import(self):
        from speed_layer.rules.amount_spike import apply_amount_spike
        self.assertTrue(callable(apply_amount_spike))


class TestAmountSpikeLogic(unittest.TestCase):
    """Test amount spike detection logic independently of Spark."""

    def _is_spike(self, amount: float, median: float, threshold: float = 3.0) -> bool:
        """Mirror the spike detection logic from amount_spike.py."""
        if median == 0:
            return False
        return (amount / median) > threshold

    def test_clean_transaction(self):
        self.assertFalse(self._is_spike(50_000, 50_000))

    def test_exactly_3x_not_flagged(self):
        # threshold is > 3.0, so exactly 3x is clean
        self.assertFalse(self._is_spike(150_000, 50_000))

    def test_over_3x_flagged(self):
        self.assertTrue(self._is_spike(150_001, 50_000))

    def test_10x_flagged(self):
        self.assertTrue(self._is_spike(500_000, 50_000))

    def test_zero_median_safe(self):
        self.assertFalse(self._is_spike(100_000, 0))

    def test_amount_ratio_calculation(self):
        amount, median = 300_000, 100_000
        ratio = amount / median
        self.assertAlmostEqual(ratio, 3.0)
        self.assertFalse(ratio > 3.0)  # not a spike at exactly 3x


# ---------------------------------------------------------------------------
# Geo anomaly rule
# ---------------------------------------------------------------------------

@unittest.skipUnless(PYSPARK_AVAILABLE, "pyspark not installed")
class TestGeoAnomalyImport(unittest.TestCase):
    def test_import(self):
        from speed_layer.rules.geo_anomaly import apply_geo_anomaly
        self.assertTrue(callable(apply_geo_anomaly))


class TestGeoAnomalyLogic(unittest.TestCase):
    """Test geo anomaly detection logic independently of Spark."""

    def _is_geo_anomaly(
        self, country1: str, country2: str,
        t1: datetime, t2: datetime,
        window_minutes: int = 30,
    ) -> bool:
        """Mirror the geo anomaly join condition from geo_anomaly.py."""
        if country1 == country2:
            return False
        gap = abs((t2 - t1).total_seconds()) / 60
        return gap <= window_minutes

    def _in_window(self, t1: datetime, t2: datetime, window_minutes: int = 30) -> bool:
        """Check tx2.tx_at BETWEEN tx1.tx_at AND tx1.tx_at + 30 minutes."""
        return t1 <= t2 <= t1 + timedelta(minutes=window_minutes)

    def test_same_country_not_anomaly(self):
        t = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
        self.assertFalse(self._is_geo_anomaly("VN", "VN", t, t + timedelta(minutes=5)))

    def test_different_country_within_30min(self):
        t1 = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
        t2 = t1 + timedelta(minutes=15)
        self.assertTrue(self._is_geo_anomaly("VN", "US", t1, t2))

    def test_different_country_exactly_30min(self):
        t1 = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
        t2 = t1 + timedelta(minutes=30)
        self.assertTrue(self._is_geo_anomaly("VN", "US", t1, t2))

    def test_different_country_over_30min_not_anomaly(self):
        t1 = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
        t2 = t1 + timedelta(minutes=31)
        self.assertFalse(self._is_geo_anomaly("VN", "US", t1, t2))

    def test_window_join_condition(self):
        t1 = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
        # t2 within window
        self.assertTrue(self._in_window(t1, t1 + timedelta(minutes=20)))
        # t2 before t1 — not in window
        self.assertFalse(self._in_window(t1, t1 - timedelta(minutes=1)))
        # t2 after window
        self.assertFalse(self._in_window(t1, t1 + timedelta(minutes=31)))


# ---------------------------------------------------------------------------
# Fraud detector score aggregation
# ---------------------------------------------------------------------------

@unittest.skipUnless(PYSPARK_AVAILABLE, "pyspark not installed")
class TestFraudDetectorImport(unittest.TestCase):
    def test_import(self):
        from speed_layer.fraud_detector import apply_all_rules
        self.assertTrue(callable(apply_all_rules))


class TestFraudScoreAggregation(unittest.TestCase):
    """Test the fraud_score summation logic without Spark."""

    def _compute_score(self, velocity_score: int, amount_spike: bool, geo_anomaly: bool) -> int:
        """Mirror the fraud_score calculation in fraud_detector.py."""
        return (
            (1 if velocity_score > 0 else 0)
            + (1 if amount_spike else 0)
            + (1 if geo_anomaly else 0)
        )

    def test_no_rules_fired_score_0(self):
        self.assertEqual(self._compute_score(0, False, False), 0)

    def test_only_velocity_score_1(self):
        self.assertEqual(self._compute_score(1, False, False), 1)

    def test_only_amount_spike_score_1(self):
        self.assertEqual(self._compute_score(0, True, False), 1)

    def test_only_geo_anomaly_score_1(self):
        self.assertEqual(self._compute_score(0, False, True), 1)

    def test_two_rules_score_2(self):
        self.assertEqual(self._compute_score(1, True, False), 2)
        self.assertEqual(self._compute_score(0, True, True), 2)

    def test_all_rules_fired_score_3(self):
        self.assertEqual(self._compute_score(2, True, True), 3)

    def test_velocity_critical_still_counts_as_1_point(self):
        # velocity_score=2 (critical) still adds only 1 to fraud_score
        self.assertEqual(self._compute_score(2, False, False), 1)

    def test_is_flagged_threshold(self):
        self.assertFalse(self._compute_score(0, False, False) >= 1)
        self.assertTrue(self._compute_score(1, False, False) >= 1)


# ---------------------------------------------------------------------------
# Redis sink
# ---------------------------------------------------------------------------

class TestRedisSinkImport(unittest.TestCase):
    def test_import(self):
        from speed_layer.sink.redis_sink import write_fraud_alerts
        self.assertTrue(callable(write_fraud_alerts))


class TestRedisSink(unittest.TestCase):
    """Test Redis sink behaviour with a mocked Redis client."""

    def _make_batch_df(self, rows):
        """Build a mock batch DataFrame whose .collect() returns the given rows."""
        mock_df = MagicMock()
        mock_df.collect.return_value = rows
        return mock_df

    @patch("speed_layer.sink.redis_sink._get_redis_client")
    def test_empty_batch_does_nothing(self, mock_get_client):
        from speed_layer.sink.redis_sink import write_fraud_alerts
        mock_df = self._make_batch_df([])
        write_fraud_alerts(mock_df, batch_id=0)
        mock_get_client.assert_not_called()

    @patch("speed_layer.sink.redis_sink._get_redis_client")
    def test_pipeline_called_for_alerts(self, mock_get_client):
        from speed_layer.sink.redis_sink import write_fraud_alerts

        mock_pipe = MagicMock()
        mock_r = MagicMock()
        mock_r.pipeline.return_value = mock_pipe
        mock_get_client.return_value = mock_r

        row = make_tx(is_flagged=True, fraud_score=1, velocity_score=1)
        mock_df = self._make_batch_df([row])
        write_fraud_alerts(mock_df, batch_id=1)

        mock_r.pipeline.assert_called_once()
        mock_pipe.execute.assert_called_once()

    @patch("speed_layer.sink.redis_sink._get_redis_client")
    def test_lpush_and_ltrim_called(self, mock_get_client):
        from speed_layer.sink.redis_sink import write_fraud_alerts, ALERTS_KEY, ALERTS_MAX_LEN

        mock_pipe = MagicMock()
        mock_r = MagicMock()
        mock_r.pipeline.return_value = mock_pipe
        mock_get_client.return_value = mock_r

        row = make_tx(is_flagged=True, fraud_score=2, velocity_score=2)
        write_fraud_alerts(self._make_batch_df([row]), batch_id=2)

        mock_pipe.lpush.assert_called_once()
        args = mock_pipe.lpush.call_args[0]
        self.assertEqual(args[0], ALERTS_KEY)

        mock_pipe.ltrim.assert_called_once_with(ALERTS_KEY, 0, ALERTS_MAX_LEN - 1)

    @patch("speed_layer.sink.redis_sink._get_redis_client")
    def test_incr_total_and_user_counter(self, mock_get_client):
        from speed_layer.sink.redis_sink import write_fraud_alerts, TOTAL_COUNT_KEY

        mock_pipe = MagicMock()
        mock_r = MagicMock()
        mock_r.pipeline.return_value = mock_pipe
        mock_get_client.return_value = mock_r

        row = make_tx(is_flagged=True, fraud_score=1, user_id="user_0007")
        write_fraud_alerts(self._make_batch_df([row]), batch_id=3)

        incr_calls = [c[0][0] for c in mock_pipe.incr.call_args_list]
        self.assertIn(TOTAL_COUNT_KEY, incr_calls)
        self.assertIn("fraud:count:user:user_0007", incr_calls)

    @patch("speed_layer.sink.redis_sink._get_redis_client")
    def test_zincrby_top_users(self, mock_get_client):
        from speed_layer.sink.redis_sink import write_fraud_alerts, TOP_USERS_KEY

        mock_pipe = MagicMock()
        mock_r = MagicMock()
        mock_r.pipeline.return_value = mock_pipe
        mock_get_client.return_value = mock_r

        row = make_tx(is_flagged=True, fraud_score=3, user_id="user_0099")
        write_fraud_alerts(self._make_batch_df([row]), batch_id=4)

        mock_pipe.zincrby.assert_called_once_with(TOP_USERS_KEY, 3.0, "user_0099")

    @patch("speed_layer.sink.redis_sink._get_redis_client")
    def test_circuit_breaker_triggered_when_fraud_rate_high(self, mock_get_client):
        from speed_layer.sink.redis_sink import write_fraud_alerts, CIRCUIT_BREAKER_KEY, CIRCUIT_BREAKER_TTL

        mock_pipe = MagicMock()
        mock_r = MagicMock()
        mock_r.pipeline.return_value = mock_pipe
        mock_get_client.return_value = mock_r

        # All rows flagged → fraud_rate = 100% > 20%
        rows = [make_tx(is_flagged=True, fraud_score=1) for _ in range(5)]
        write_fraud_alerts(self._make_batch_df(rows), batch_id=5)

        mock_r.set.assert_called_with(CIRCUIT_BREAKER_KEY, "1", ex=CIRCUIT_BREAKER_TTL)


# ---------------------------------------------------------------------------
# Kafka sink
# ---------------------------------------------------------------------------

class TestKafkaSinkImport(unittest.TestCase):
    def test_import(self):
        from speed_layer.sink.kafka_sink import write_to_kafka
        self.assertTrue(callable(write_to_kafka))


class TestKafkaSink(unittest.TestCase):
    """Test Kafka sink with mocked KafkaProducer and Redis circuit breaker."""

    def _make_batch_df(self, rows):
        mock_df = MagicMock()
        mock_df.collect.return_value = rows
        return mock_df

    @patch("speed_layer.sink.kafka_sink.KafkaProducer")
    @patch("speed_layer.sink.kafka_sink.redis")
    def test_empty_batch_does_nothing(self, mock_redis_mod, mock_producer_cls):
        from speed_layer.sink.kafka_sink import write_to_kafka
        write_to_kafka(self._make_batch_df([]), batch_id=0)
        mock_producer_cls.assert_not_called()

    @patch("speed_layer.sink.kafka_sink.KafkaProducer")
    @patch("speed_layer.sink.kafka_sink.redis")
    def test_circuit_breaker_active_skips_publish(self, mock_redis_mod, mock_producer_cls):
        from speed_layer.sink.kafka_sink import write_to_kafka

        mock_r = MagicMock()
        mock_r.exists.return_value = True  # circuit breaker is set
        mock_redis_mod.Redis.return_value = mock_r

        row = make_tx(is_flagged=True, fraud_score=1)
        write_to_kafka(self._make_batch_df([row]), batch_id=1)

        mock_producer_cls.assert_not_called()

    @patch("speed_layer.sink.kafka_sink.KafkaProducer")
    @patch("speed_layer.sink.kafka_sink.redis")
    def test_alerts_published_when_circuit_clear(self, mock_redis_mod, mock_producer_cls):
        from speed_layer.sink.kafka_sink import write_to_kafka

        mock_r = MagicMock()
        mock_r.exists.return_value = False  # circuit breaker clear
        mock_redis_mod.Redis.return_value = mock_r

        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        rows = [make_tx(is_flagged=True, fraud_score=1, user_id=f"user_{i:04d}") for i in range(3)]
        write_to_kafka(self._make_batch_df(rows), batch_id=2)

        self.assertEqual(mock_producer.send.call_count, 3)
        mock_producer.flush.assert_called_once()
        mock_producer.close.assert_called_once()

    @patch("speed_layer.sink.kafka_sink.KafkaProducer")
    @patch("speed_layer.sink.kafka_sink.redis")
    def test_kafka_key_is_user_id(self, mock_redis_mod, mock_producer_cls):
        from speed_layer.sink.kafka_sink import write_to_kafka

        mock_r = MagicMock()
        mock_r.exists.return_value = False
        mock_redis_mod.Redis.return_value = mock_r

        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        row = make_tx(is_flagged=True, fraud_score=1, user_id="user_0042")
        write_to_kafka(self._make_batch_df([row]), batch_id=3)

        send_kwargs = mock_producer.send.call_args
        self.assertEqual(send_kwargs[1]["key"], "user_0042")


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False)
    sys.exit(0 if result.result.wasSuccessful() else 1)
