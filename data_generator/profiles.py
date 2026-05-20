"""
Static profiles for merchants, countries, and devices used by the transaction generator.
Merchant amounts use lognormal distribution parameters (avg_amount, std in VND).
Country weights reflect real Vietnamese e-commerce demographics (85% VN traffic).
"""
from typing import List, Dict, Any

MERCHANTS: List[Dict[str, Any]] = [
    {"name": "Grab",      "category": "transport",  "avg_amount": 50_000,    "std": 30_000},
    {"name": "Shopee",    "category": "ecommerce",  "avg_amount": 250_000,   "std": 200_000},
    {"name": "VinMart",   "category": "grocery",    "avg_amount": 150_000,   "std": 100_000},
    {"name": "MoMo",      "category": "fintech",    "avg_amount": 500_000,   "std": 400_000},
    {"name": "Tiki",      "category": "ecommerce",  "avg_amount": 300_000,   "std": 250_000},
    {"name": "Highlands", "category": "food",       "avg_amount": 80_000,    "std": 40_000},
    {"name": "Circle K",  "category": "grocery",    "avg_amount": 60_000,    "std": 30_000},
    {"name": "ZaloPay",   "category": "fintech",    "avg_amount": 200_000,   "std": 150_000},
    {"name": "VNPay",     "category": "fintech",    "avg_amount": 1_000_000, "std": 800_000},
    {"name": "Lazada",    "category": "ecommerce",  "avg_amount": 350_000,   "std": 300_000},
]

# Country weights: 85% Vietnam, 15% distributed among other countries
COUNTRIES: List[str] = ["VN"] * 85 + ["US"] * 5 + ["SG"] * 4 + ["JP"] * 3 + ["KR"] * 2 + ["GB"] * 1

DEVICES: List[str] = ["mobile", "web", "pos"]

# Device weights: mobile-heavy as typical in Southeast Asia
DEVICE_WEIGHTS: List[float] = [0.70, 0.20, 0.10]

AMOUNT_MIN_VND: float = 1_000.0
AMOUNT_MAX_VND: float = 50_000_000.0

USER_COUNT: int = 500
