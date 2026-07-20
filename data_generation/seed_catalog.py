"""Deterministic customer/product/carrier catalogs shared by both generators.

generate_clickstream.py and generate_orders_domain.py each call build_customers()/
build_products() with the same --seed so customer_id and sku spaces line up across the two
independently-generated datasets, without one script depending on the other's output file.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from faker import Faker

CATEGORIES = [
    ("apparel", 18, 120),
    ("footwear", 35, 180),
    ("electronics", 25, 900),
    ("home", 12, 350),
    ("beauty", 8, 65),
    ("outdoor", 20, 400),
]

CARRIERS = [
    # (carrier, avg_transit_days, historical_late_rate_pct)
    ("ParcelSwift", 3, 6.5),
    ("NorthLine Freight", 5, 14.0),
    ("QuickHop", 2, 4.0),
    ("EverRoute", 4, 9.5),
]

US_TZ_OFFSETS = [-5, -6, -7, -8]  # eastern..pacific, kept simple for synthetic data


@dataclass
class Customer:
    customer_id: str
    first_seen_date: str
    is_returning: bool
    home_tz_offset: int


@dataclass
class Product:
    sku: str
    name: str
    category: str
    price: float


def build_customers(n: int, seed: int) -> list[Customer]:
    fake = Faker()
    Faker.seed(seed)
    rng = random.Random(seed)
    customers = []
    for i in range(1, n + 1):
        customers.append(
            Customer(
                customer_id=f"CUST{i:06d}",
                first_seen_date=fake.date_between(
                    start_date="-2y", end_date="-30d"
                ).isoformat(),
                is_returning=rng.random() < 0.35,
                home_tz_offset=rng.choice(US_TZ_OFFSETS),
            )
        )
    return customers


def build_products(n: int, seed: int) -> list[Product]:
    fake = Faker()
    Faker.seed(seed + 1)
    rng = random.Random(seed + 1)
    products = []
    for i in range(1, n + 1):
        category, lo, hi = rng.choice(CATEGORIES)
        price = round(rng.uniform(lo, hi), 2)
        products.append(
            Product(
                sku=f"SKU{i:05d}",
                name=f"{fake.word().capitalize()} {category.capitalize()} {fake.word().capitalize()}",
                category=category,
                price=price,
            )
        )
    return products
