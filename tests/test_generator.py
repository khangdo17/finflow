"""
Unit tests for the data generator and fraud simulator.
Does NOT require a running Kafka — all Kafka calls are mocked.
Run with: python tests/test_generator.py
"""
import math
import sys
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

# Ensure project root is on path so imports work from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_generator.profiles import (
    MERCHANTS, COUNTRIES, DEVICES, AMOUNT_MIN_VND, AMOUNT_MAX_VND, USER_COUNT,
)
from data_generator.generator import generate_amount, generate_transaction
from data_generator.fraud_simulator import FraudSimulator


class TestProfiles(unittest.TestCase):
    """Verify static profile data matches spec."""

    def test_merchant_count(self):
        self.assertEqual(len(MERCHANTS), 10)

    def test_merchant_names(self):
        names = [m["name"] for m in MERCHANTS]
        expected = {"Grab", "Shopee", "VinMart", "MoMo", "Tiki",
                    "Highlands", "Circle K", "ZaloPay", "VNPay", "Lazada"}
        self.assertEqual(set(names), expected)

    def test_merchant_has_avg_and_std(self):
        for m in MERCHANTS:
            self.assertIn("avg_amount", m, f"{m['name']} missing avg_amount")
            self.assertIn("std", m, f"{m['name']} missing std")
            self.assertGreater(m["avg_amount"], 0)
            self.assertGreater(m["std"], 0)

    def test_countries_85pct_vn(self):
        vn_count = COUNTRIES.count("VN")
        self.assertEqual(vn_count, 85, f"Expected 85 VN entries, got {vn_count}")
        self.assertEqual(len(COUNTRIES), 100)

    def test_devices_list(self):
        self.assertIn("mobile", DEVICES)
        self.assertIn("web", DEVICES)
        self.assertIn("pos", DEVICES)

    def test_amount_bounds(self):
        self.assertEqual(AMOUNT_MIN_VND, 1_000.0)
        self.assertEqual(AMOUNT_MAX_VND, 50_000_000.0)

    def test_user_count(self):
        self.assertEqual(USER_COUNT, 500)


class TestGenerateAmount(unittest.TestCase):
    """Verify lognormal amount generation stays within bounds."""

    def test_amount_within_bounds(self):
        for _ in range(1000):
            amount = generate_amount(avg_amount=200_000, std=150_000)
            self.assertGreaterEqual(amount, AMOUNT_MIN_VND)
            self.assertLessEqual(amount, AMOUNT_MAX_VND)

    def test_amount_is_float(self):
        amount = generate_amount(avg_amount=100_000, std=50_000)
        self.assertIsInstance(amount, float)

    def test_amount_rounded_to_2_decimals(self):
        for _ in range(100):
            amount = generate_amount(avg_amount=300_000, std=200_000)
            # Check at most 2 decimal places
            self.assertEqual(amount, round(amount, 2))

    def test_high_avg_clamped(self):
        # avg=100M, std=50M should be clamped to AMOUNT_MAX_VND
        for _ in range(50):
            amount = generate_amount(avg_amount=100_000_000, std=50_000_000)
            self.assertLessEqual(amount, AMOUNT_MAX_VND)


class TestGenerateTransaction(unittest.TestCase):
    """Verify transaction structure and field validity."""

    def test_transaction_fields_present(self):
        tx = generate_transaction()
        required_fields = ["tx_id", "user_id", "merchant", "amount", "currency",
                           "country", "tx_at", "device", "is_fraud", "fraud_reason"]
        for field in required_fields:
            self.assertIn(field, tx, f"Missing field: {field}")

    def test_user_id_format(self):
        for _ in range(50):
            tx = generate_transaction()
            uid = tx["user_id"]
            self.assertTrue(uid.startswith("user_"), f"Bad user_id format: {uid}")
            number_part = uid[5:]
            self.assertEqual(len(number_part), 4, f"Expected 4-digit suffix, got: {number_part}")
            n = int(number_part)
            self.assertGreaterEqual(n, 1)
            self.assertLessEqual(n, USER_COUNT)

    def test_currency_is_vnd(self):
        tx = generate_transaction()
        self.assertEqual(tx["currency"], "VND")

    def test_merchant_is_known(self):
        merchant_names = {m["name"] for m in MERCHANTS}
        for _ in range(20):
            tx = generate_transaction()
            self.assertIn(tx["merchant"], merchant_names)

    def test_device_is_known(self):
        for _ in range(20):
            tx = generate_transaction()
            self.assertIn(tx["device"], DEVICES)

    def test_normal_transaction_not_fraud(self):
        tx = generate_transaction(is_fraud=False)
        self.assertFalse(tx["is_fraud"])

    def test_fraud_transaction_flagged(self):
        tx = generate_transaction(is_fraud=True, fraud_reason="test_reason")
        self.assertTrue(tx["is_fraud"])
        self.assertEqual(tx["fraud_reason"], "test_reason")

    def test_tx_id_is_uuid(self):
        import uuid
        tx = generate_transaction()
        # Should not raise
        uuid.UUID(tx["tx_id"])

    def test_tx_at_is_iso_format(self):
        tx = generate_transaction()
        # Should not raise
        datetime.fromisoformat(tx["tx_at"])


class TestFraudSimulator(unittest.TestCase):
    """Verify each fraud injection method produces correct output."""

    def setUp(self):
        self.sim = FraudSimulator()

    # --- velocity_spike ---

    def test_velocity_spike_returns_8_transactions(self):
        txs = self.sim.inject_velocity_spike()
        self.assertEqual(len(txs), 8)

    def test_velocity_spike_same_user(self):
        txs = self.sim.inject_velocity_spike()
        user_ids = {tx["user_id"] for tx in txs}
        self.assertEqual(len(user_ids), 1, "All velocity-spike txs must share the same user_id")

    def test_velocity_spike_fraud_flagged(self):
        txs = self.sim.inject_velocity_spike()
        for tx in txs:
            self.assertTrue(tx["is_fraud"])
            self.assertEqual(tx["fraud_reason"], "velocity_spike")

    def test_velocity_spike_within_60s(self):
        txs = self.sim.inject_velocity_spike()
        times = [datetime.fromisoformat(tx["tx_at"]) for tx in txs]
        spread = (max(times) - min(times)).total_seconds()
        self.assertLessEqual(spread, 60, f"Velocity spike spread {spread}s exceeds 60s")

    # --- amount_spike ---

    def test_amount_spike_returns_one_transaction(self):
        txs = self.sim.inject_amount_spike()
        self.assertEqual(len(txs), 1)

    def test_amount_spike_fraud_flagged(self):
        tx = self.sim.inject_amount_spike()[0]
        self.assertTrue(tx["is_fraud"])
        self.assertEqual(tx["fraud_reason"], "amount_spike")

    def test_amount_spike_within_bounds(self):
        for _ in range(20):
            tx = self.sim.inject_amount_spike()[0]
            self.assertGreaterEqual(tx["amount"], AMOUNT_MIN_VND)
            self.assertLessEqual(tx["amount"], AMOUNT_MAX_VND)

    # --- geo_anomaly ---

    def test_geo_anomaly_returns_two_transactions(self):
        txs = self.sim.inject_geo_anomaly()
        self.assertEqual(len(txs), 2)

    def test_geo_anomaly_same_user(self):
        txs = self.sim.inject_geo_anomaly()
        user_ids = {tx["user_id"] for tx in txs}
        self.assertEqual(len(user_ids), 1)

    def test_geo_anomaly_different_countries(self):
        txs = self.sim.inject_geo_anomaly()
        countries = [tx["country"] for tx in txs]
        self.assertIn("VN", countries)
        self.assertIn("US", countries)

    def test_geo_anomaly_within_20_minutes(self):
        txs = self.sim.inject_geo_anomaly()
        times = sorted([datetime.fromisoformat(tx["tx_at"]) for tx in txs])
        gap_minutes = (times[1] - times[0]).total_seconds() / 60
        self.assertLessEqual(gap_minutes, 20, f"Geo gap {gap_minutes:.1f}min exceeds 20min")
        self.assertGreaterEqual(gap_minutes, 5, f"Geo gap {gap_minutes:.1f}min is under 5min")

    def test_geo_anomaly_fraud_flagged(self):
        txs = self.sim.inject_geo_anomaly()
        for tx in txs:
            self.assertTrue(tx["is_fraud"])
            self.assertEqual(tx["fraud_reason"], "geo_anomaly")


class TestGeneratorMode(unittest.TestCase):
    """Verify generator respects mode flag without needing Kafka."""

    @patch("data_generator.generator.build_producer")
    def test_normal_mode_no_fraud(self, mock_build):
        """In normal mode, generated transactions should not be fraud."""
        # We just test generate_transaction directly for normal mode behaviour
        for _ in range(50):
            tx = generate_transaction(is_fraud=False)
            self.assertFalse(tx["is_fraud"])

    def test_generate_amount_lognormal_distribution(self):
        """Verify lognormal sampling: mean of large sample should be close to avg_amount."""
        avg = 200_000
        std = 100_000
        samples = [generate_amount(avg, std) for _ in range(5000)]
        sample_mean = sum(samples) / len(samples)
        # Allow 30% deviation given clamping
        self.assertAlmostEqual(sample_mean, avg, delta=avg * 0.30,
                                msg=f"Sample mean {sample_mean:.0f} too far from {avg}")


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False)
    sys.exit(0 if result.result.wasSuccessful() else 1)
