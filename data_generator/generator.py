"""
Main transaction generator for FinFlow.
Produces synthetic fintech transactions to Kafka using lognormal amount distribution.
Supports --rate (tx/s) and --mode (normal/fraud/mixed) CLI flags.
"""
import argparse
import json
import math
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any

from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger

from data_generator.profiles import (
    MERCHANTS, COUNTRIES, DEVICES, DEVICE_WEIGHTS,
    AMOUNT_MIN_VND, AMOUNT_MAX_VND, USER_COUNT,
)
from data_generator.fraud_simulator import FraudSimulator

load_dotenv()


def generate_amount(avg_amount: float, std: float) -> float:
    """
    Sample amount from lognormal distribution parameterised by mean and std.
    Clamps result to [AMOUNT_MIN_VND, AMOUNT_MAX_VND] to stay realistic.
    """
    # Convert normal mean/std to lognormal mu/sigma
    variance = std ** 2
    mu = math.log(avg_amount ** 2 / math.sqrt(variance + avg_amount ** 2))
    sigma = math.sqrt(math.log(1 + variance / avg_amount ** 2))
    amount = random.lognormvariate(mu, sigma)
    return round(max(AMOUNT_MIN_VND, min(AMOUNT_MAX_VND, amount)), 2)


def generate_transaction(is_fraud: bool = False, fraud_reason: str = None) -> Dict[str, Any]:
    """
    Build a single transaction dict with randomised fields.
    Kafka key is set to user_id to ensure partition affinity per user.
    """
    merchant = random.choice(MERCHANTS)
    user_id = f"user_{random.randint(1, USER_COUNT):04d}"
    amount = generate_amount(merchant["avg_amount"], merchant["std"])

    tx: Dict[str, Any] = {
        "tx_id": str(uuid.uuid4()),
        "user_id": user_id,
        "merchant": merchant["name"],
        "amount": amount,
        "currency": "VND",
        "country": random.choice(COUNTRIES),
        "tx_at": datetime.now(timezone.utc).isoformat(),
        "device": random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0],
        "is_fraud": is_fraud,
        "fraud_reason": fraud_reason,
    }
    return tx


def build_producer() -> KafkaProducer:
    """Create a KafkaProducer that serialises values as JSON and uses user_id as key."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def run(rate: int, mode: str) -> None:
    """
    Main loop that produces transactions at the given rate (tx/s).
    mode=normal: clean transactions only
    mode=fraud:  all transactions flagged as fraud
    mode=mixed:  ~5% fraud injection via FraudSimulator
    """
    topic = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "raw-transactions")
    producer = build_producer()
    simulator = FraudSimulator()
    interval = 1.0 / rate
    sent = 0

    logger.info(f"Starting generator — mode={mode}, rate={rate} tx/s, topic={topic}")

    try:
        while True:
            start = time.monotonic()

            if mode == "fraud":
                txs = simulator.inject_velocity_spike()  # all fraud
            elif mode == "mixed" and random.random() < 0.05:
                # Pick a random fraud type for ~5% of batches
                injector = random.choice([
                    simulator.inject_velocity_spike,
                    simulator.inject_amount_spike,
                    simulator.inject_geo_anomaly,
                ])
                txs = injector()
            else:
                txs = [generate_transaction()]

            for tx in txs:
                producer.send(topic, key=tx["user_id"], value=tx)
                sent += 1
                if sent % 1000 == 0:
                    logger.info(f"Sent {sent} transactions")

            elapsed = time.monotonic() - start
            sleep_time = interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info(f"Generator stopped. Total sent: {sent}")
    except KafkaError as exc:
        logger.error(f"Kafka error: {exc}")
        raise
    finally:
        producer.flush()
        producer.close()


def main() -> None:
    """Parse CLI args and start the generator."""
    parser = argparse.ArgumentParser(description="FinFlow transaction generator")
    parser.add_argument("--rate", type=int, default=1000, help="Transactions per second (default: 1000)")
    parser.add_argument(
        "--mode",
        choices=["normal", "fraud", "mixed"],
        default="mixed",
        help="Generation mode: normal | fraud | mixed (default: mixed)",
    )
    args = parser.parse_args()
    run(rate=args.rate, mode=args.mode)


if __name__ == "__main__":
    main()
