"""
Fraud pattern injector for FinFlow.
Provides three injection methods that create realistic fraud signals:
  - velocity_spike: burst of transactions from one user in 60s
  - amount_spike: single transaction with 10x inflated amount
  - geo_anomaly: same user appearing in VN then US within 20 minutes
"""
import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

from loguru import logger

from data_generator.profiles import MERCHANTS, DEVICES, DEVICE_WEIGHTS, AMOUNT_MIN_VND, AMOUNT_MAX_VND, USER_COUNT


class FraudSimulator:
    """Encapsulates fraud injection strategies used by the generator."""

    def _base_tx(
        self,
        user_id: str,
        country: str,
        tx_at: datetime,
        is_fraud: bool,
        fraud_reason: str,
        amount_override: float = None,
    ) -> Dict[str, Any]:
        """Build a transaction dict with optional field overrides."""
        merchant = random.choice(MERCHANTS)
        amount = amount_override if amount_override is not None else round(
            random.uniform(10_000, 500_000), 2
        )
        amount = round(max(AMOUNT_MIN_VND, min(AMOUNT_MAX_VND, amount)), 2)

        return {
            "tx_id": str(uuid.uuid4()),
            "user_id": user_id,
            "merchant": merchant["name"],
            "amount": amount,
            "currency": "VND",
            "country": country,
            "tx_at": tx_at.isoformat(),
            "device": random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0],
            "is_fraud": is_fraud,
            "fraud_reason": fraud_reason,
        }

    def inject_velocity_spike(self) -> List[Dict[str, Any]]:
        """
        Generate 8 transactions from the same user within 60 seconds.
        Simulates a credential-stuffing or card-testing velocity attack.
        """
        user_id = f"user_{random.randint(1, USER_COUNT):04d}"
        now = datetime.now(timezone.utc)
        txs = []

        for i in range(8):
            tx_at = now + timedelta(seconds=random.uniform(0, 59))
            txs.append(self._base_tx(
                user_id=user_id,
                country="VN",
                tx_at=tx_at,
                is_fraud=True,
                fraud_reason="velocity_spike",
            ))

        logger.debug(f"[FraudSimulator] velocity_spike — user={user_id}, count={len(txs)}")
        return txs

    def inject_amount_spike(self) -> List[Dict[str, Any]]:
        """
        Generate a single transaction with amount 10x larger than the merchant average.
        Simulates account takeover where attacker drains balance in one shot.
        """
        user_id = f"user_{random.randint(1, USER_COUNT):04d}"
        merchant = random.choice(MERCHANTS)
        spike_amount = round(min(merchant["avg_amount"] * 10, AMOUNT_MAX_VND), 2)
        now = datetime.now(timezone.utc)

        tx = self._base_tx(
            user_id=user_id,
            country="VN",
            tx_at=now,
            is_fraud=True,
            fraud_reason="amount_spike",
            amount_override=spike_amount,
        )

        logger.debug(f"[FraudSimulator] amount_spike — user={user_id}, amount={spike_amount:,.0f} VND")
        return [tx]

    def inject_geo_anomaly(self) -> List[Dict[str, Any]]:
        """
        Generate two transactions from the same user in VN then US within 20 minutes.
        Simulates a physically impossible travel scenario (card present in two countries).
        """
        user_id = f"user_{random.randint(1, USER_COUNT):04d}"
        now = datetime.now(timezone.utc)
        # First transaction in Vietnam
        tx_vn = self._base_tx(
            user_id=user_id,
            country="VN",
            tx_at=now,
            is_fraud=True,
            fraud_reason="geo_anomaly",
        )
        # Second transaction in US, 5–19 minutes later (within 20 min window)
        offset_minutes = random.randint(5, 19)
        tx_us = self._base_tx(
            user_id=user_id,
            country="US",
            tx_at=now + timedelta(minutes=offset_minutes),
            is_fraud=True,
            fraud_reason="geo_anomaly",
        )

        logger.debug(f"[FraudSimulator] geo_anomaly — user={user_id}, gap={offset_minutes}min")
        return [tx_vn, tx_us]
