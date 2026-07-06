#!/usr/bin/env python3
"""
Load realistic marketplace test data into CefasDB using cefasctl.

Default shape:
  - 100K physical rows per marketplace table by default.
  - DynamoDB-JSON batches sent through `cefas batch-write-item`.
  - Deterministic randomness from --seed.

The generator is intentionally field-rich: ASIN catalog records, seller offers,
customers, order headers/line items, customer order projections, and inventory
records all vary independently at field level.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable


ROOT = Path(__file__).resolve().parents[2]
U64_MASK = (1 << 64) - 1
XXH64_PRIME1 = 11400714785074694791
XXH64_PRIME2 = 14029467366897019727
XXH64_PRIME3 = 1609587929392839161
XXH64_PRIME4 = 9650029242287828579
XXH64_PRIME5 = 2870177450012600261
DEFAULT_TABLES = [
    "MarketplaceCatalog",
    "MarketplaceSellers",
    "MarketplaceCustomers",
    "MarketplaceOrders",
    "CustomerOrders",
    "FulfillmentInventory",
]

TABLE_SEEDS = {
    "MarketplaceCatalog": 101,
    "MarketplaceSellers": 211,
    "MarketplaceCustomers": 307,
    "MarketplaceOrders": 401,
    "CustomerOrders": 503,
    "FulfillmentInventory": 601,
}

PARTITION_KEY_BY_TABLE = {
    "MarketplaceCatalog": "asin",
    "MarketplaceSellers": "seller_id",
    "MarketplaceCustomers": "customer_id",
    "MarketplaceOrders": "order_id",
    "CustomerOrders": "customer_id",
    "FulfillmentInventory": "fulfillment_node_id",
}

BASE36 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
COUNTRIES = ["BR", "US", "MX", "CA", "GB", "DE", "JP"]
BR_STATES = ["SP", "RJ", "MG", "PR", "SC", "RS", "BA", "PE", "CE", "DF"]
BR_CITIES = [
    "Sao Paulo",
    "Rio de Janeiro",
    "Belo Horizonte",
    "Curitiba",
    "Porto Alegre",
    "Campinas",
    "Recife",
    "Salvador",
    "Fortaleza",
    "Brasilia",
]
FIRST_NAMES = [
    "Ana",
    "Bruno",
    "Carla",
    "Diego",
    "Elisa",
    "Felipe",
    "Giovana",
    "Henrique",
    "Isabela",
    "Joao",
    "Luiza",
    "Marcos",
    "Natalia",
    "Otavio",
    "Paula",
    "Rafael",
]
LAST_NAMES = [
    "Silva",
    "Costa",
    "Nunes",
    "Martins",
    "Pereira",
    "Oliveira",
    "Santos",
    "Rocha",
    "Almeida",
    "Lima",
    "Gomes",
    "Barbosa",
]
SELLER_PREFIXES = [
    "Prime",
    "Nova",
    "Urban",
    "Digital",
    "Brasil",
    "Atlas",
    "Smart",
    "Rapid",
    "Central",
    "Direct",
]
SELLER_VERTICALS = [
    "Electronics",
    "Home",
    "Books",
    "Beauty",
    "Sports",
    "Kitchen",
    "Tools",
    "Fashion",
    "Gaming",
    "Office",
]
FULFILLMENT_NODES = [
    "FC-GRU1",
    "FC-GRU2",
    "FC-VCP1",
    "FC-GIG1",
    "FC-CWB1",
    "FC-POA1",
    "FC-REC1",
    "FC-BSB1",
    "DS-SP01",
    "DS-RJ01",
]

PRODUCT_FAMILIES = [
    {
        "category": "electronics.smart-home.speakers",
        "product_type": "SMART_SPEAKER",
        "brands": ["CefasBasics", "Echo", "Nimbus"],
        "nouns": ["Smart Speaker", "Voice Speaker", "Home Hub"],
        "adjectives": ["Compact", "Premium", "Room-Filling", "Mini"],
        "base_price": (24990, 79990),
        "keywords": ["alexa", "wireless", "audio", "smart home"],
    },
    {
        "category": "electronics.ereaders",
        "product_type": "EREADER",
        "brands": ["Kindle", "Readly", "Paperbook"],
        "nouns": ["E-Reader", "Paperwhite Reader", "Digital Reader"],
        "adjectives": ["Waterproof", "Glare-Free", "Lightweight"],
        "base_price": (49990, 139990),
        "keywords": ["ebook", "reader", "waterproof", "display"],
    },
    {
        "category": "electronics.accessories.chargers",
        "product_type": "CHARGER",
        "brands": ["AmazonBasics", "Voltis", "CefasBasics"],
        "nouns": ["USB-C Charger", "Fast Charger", "Power Adapter"],
        "adjectives": ["GaN", "Compact", "Travel", "Dual-Port"],
        "base_price": (7990, 29990),
        "keywords": ["usb c", "charger", "power delivery", "phone"],
    },
    {
        "category": "home.kitchen.coffee",
        "product_type": "COFFEE_MAKER",
        "brands": ["CasaNova", "BrewLab", "AmazonBasics"],
        "nouns": ["Coffee Maker", "Espresso Machine", "Capsule Brewer"],
        "adjectives": ["Automatic", "Barista", "Programmable"],
        "base_price": (19990, 199990),
        "keywords": ["coffee", "espresso", "kitchen", "capsule"],
    },
    {
        "category": "books.fiction",
        "product_type": "BOOK",
        "brands": ["BlueRiver", "Northstar", "Sabi Books"],
        "nouns": ["Paperback Novel", "Hardcover Novel", "Box Set"],
        "adjectives": ["Award-Winning", "Collector", "Illustrated"],
        "base_price": (2990, 19990),
        "keywords": ["book", "fiction", "paperback", "author"],
    },
    {
        "category": "sports.outdoors.fitness",
        "product_type": "FITNESS",
        "brands": ["Athletix", "PulseFit", "TrailPro"],
        "nouns": ["Fitness Tracker", "Running Belt", "Yoga Mat"],
        "adjectives": ["Waterproof", "Lightweight", "Pro", "Foldable"],
        "base_price": (3990, 49990),
        "keywords": ["fitness", "sports", "running", "training"],
    },
]


def s(value: str) -> dict[str, str]:
    return {"S": value}


def n(value: int | float | str) -> dict[str, str]:
    if isinstance(value, float):
        text = f"{value:.4f}".rstrip("0").rstrip(".")
    else:
        text = str(value)
    return {"N": text}


def b(value: bool) -> dict[str, bool]:
    return {"BOOL": value}


def ss(values: Iterable[str]) -> dict[str, list[str]]:
    return {"SS": sorted(set(values))}


def l(values: list[dict]) -> dict[str, list[dict]]:
    return {"L": values}


def m(values: dict[str, dict]) -> dict[str, dict[str, dict]]:
    return {"M": values}


def vec(values: list[float]) -> dict:
    return {"V": [round(v, 6) for v in values], "D": len(values)}


def put(item: dict[str, dict]) -> dict[str, dict]:
    return {"PutRequest": {"Item": item}}


def _rotl64(value: int, bits: int) -> int:
    return ((value << bits) | (value >> (64 - bits))) & U64_MASK


def _xxh64_round(acc: int, lane: int) -> int:
    acc = (acc + (lane * XXH64_PRIME2)) & U64_MASK
    acc = _rotl64(acc, 31)
    acc = (acc * XXH64_PRIME1) & U64_MASK
    return acc


def _xxh64_merge_round(acc: int, val: int) -> int:
    acc ^= _xxh64_round(0, val)
    acc = ((acc * XXH64_PRIME1) + XXH64_PRIME4) & U64_MASK
    return acc


def xxh64(data: bytes, seed: int = 0) -> int:
    length = len(data)
    pos = 0

    if length >= 32:
        v1 = (seed + XXH64_PRIME1 + XXH64_PRIME2) & U64_MASK
        v2 = (seed + XXH64_PRIME2) & U64_MASK
        v3 = seed & U64_MASK
        v4 = (seed - XXH64_PRIME1) & U64_MASK
        limit = length - 32
        while pos <= limit:
            v1 = _xxh64_round(v1, int.from_bytes(data[pos : pos + 8], "little"))
            pos += 8
            v2 = _xxh64_round(v2, int.from_bytes(data[pos : pos + 8], "little"))
            pos += 8
            v3 = _xxh64_round(v3, int.from_bytes(data[pos : pos + 8], "little"))
            pos += 8
            v4 = _xxh64_round(v4, int.from_bytes(data[pos : pos + 8], "little"))
            pos += 8
        h = (_rotl64(v1, 1) + _rotl64(v2, 7) + _rotl64(v3, 12) + _rotl64(v4, 18)) & U64_MASK
        h = _xxh64_merge_round(h, v1)
        h = _xxh64_merge_round(h, v2)
        h = _xxh64_merge_round(h, v3)
        h = _xxh64_merge_round(h, v4)
    else:
        h = (seed + XXH64_PRIME5) & U64_MASK

    h = (h + length) & U64_MASK

    while pos + 8 <= length:
        k1 = _xxh64_round(0, int.from_bytes(data[pos : pos + 8], "little"))
        h ^= k1
        h = ((_rotl64(h, 27) * XXH64_PRIME1) + XXH64_PRIME4) & U64_MASK
        pos += 8

    if pos + 4 <= length:
        h ^= (int.from_bytes(data[pos : pos + 4], "little") * XXH64_PRIME1) & U64_MASK
        h = ((_rotl64(h, 23) * XXH64_PRIME2) + XXH64_PRIME3) & U64_MASK
        pos += 4

    while pos < length:
        h ^= (data[pos] * XXH64_PRIME5) & U64_MASK
        h = (_rotl64(h, 11) * XXH64_PRIME1) & U64_MASK
        pos += 1

    h ^= h >> 33
    h = (h * XXH64_PRIME2) & U64_MASK
    h ^= h >> 29
    h = (h * XXH64_PRIME3) & U64_MASK
    h ^= h >> 32
    return h & U64_MASK


def base36(value: int, width: int) -> str:
    if value < 0:
        raise ValueError("base36 only supports non-negative values")
    chars: list[str] = []
    while value:
        value, rem = divmod(value, 36)
        chars.append(BASE36[rem])
    encoded = "".join(reversed(chars or ["0"]))
    return encoded.zfill(width)[-width:]


def asin_for(index: int) -> str:
    return "B0" + base36(index + 10_000, 8)


def seller_id(index: int) -> str:
    return f"SLR-{index:06d}"


def customer_id(index: int) -> str:
    return f"CUST-{index:06d}"


def order_id(index: int) -> str:
    suffix = (index * 7_919 + 123_4567) % 10_000_000
    return f"BR-113-{9_000_000 + index:07d}-{suffix:07d}"


def sku_for(product_index: int, seller_index: int, variant: int) -> str:
    return f"SKU-{asin_for(product_index)}-{seller_index % 10000:04d}-{variant:02d}"


def item_rng(seed: int, table: str, index: int) -> random.Random:
    return random.Random((seed * 1_000_003) + (TABLE_SEEDS[table] * 65_537) + index)


def weighted_choice(rng: random.Random, choices: list[tuple[str, int]]) -> str:
    total = sum(weight for _, weight in choices)
    draw = rng.randint(1, total)
    running = 0
    for value, weight in choices:
        running += weight
        if draw <= running:
            return value
    return choices[-1][0]


def iso_at(index: int, rng: random.Random, start_days_ago: int = 365) -> str:
    base = datetime.now(timezone.utc) - timedelta(days=start_days_ago)
    delta = timedelta(
        days=index % start_days_ago,
        seconds=rng.randint(0, 86_399),
        milliseconds=rng.randint(0, 999),
    )
    return (base + delta).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def product_profile(product_index: int, seed: int) -> dict:
    rng = item_rng(seed, "MarketplaceCatalog", product_index * 2)
    family = rng.choice(PRODUCT_FAMILIES)
    brand = rng.choice(family["brands"])
    adjective = rng.choice(family["adjectives"])
    noun = rng.choice(family["nouns"])
    model = f"{base36(product_index + 1, 4)}-{rng.choice(['BR', 'PRO', 'MAX', 'LITE'])}"
    color = rng.choice(["black", "white", "blue", "graphite", "silver", "green"])
    title = f"{brand} {adjective} {noun} {model} {color.title()}"
    low, high = family["base_price"]
    list_price = int(round(rng.randint(low, high) / 10) * 10)
    rating = min(5.0, max(2.5, rng.gauss(4.25, 0.45)))
    keywords = [brand.lower(), adjective.lower(), noun.lower(), color, *family["keywords"]]
    return {
        "family": family,
        "brand": brand,
        "title": title,
        "search_title": " ".join(keywords),
        "list_price": list_price,
        "rating": rating,
        "color": color,
        "model": model,
    }


def catalog_item(index: int, seed: int, include_vectors: bool, rows_per_table: int) -> dict:
    product_index = index // 2
    profile = product_profile(product_index, seed)
    rng = item_rng(seed, "MarketplaceCatalog", index)
    asin = asin_for(product_index)

    if index % 2 == 0:
        family = profile["family"]
        tags = [
            family["product_type"].lower(),
            profile["brand"].lower(),
            profile["color"],
            rng.choice(["prime", "deal", "new-arrival", "bestseller", "gift"]),
        ]
        item = {
            "asin": s(asin),
            "entity": s("PRODUCT"),
            "title": s(profile["title"]),
            "search_title": s(profile["search_title"]),
            "brand": s(profile["brand"]),
            "category": s(family["category"]),
            "product_type": s(family["product_type"]),
            "status": s(weighted_choice(rng, [("ACTIVE", 94), ("SUPPRESSED", 3), ("DRAFT", 3)])),
            "primary_image_url": s(f"https://cdn.example.com/products/{asin}/main.jpg"),
            "list_price_cents": n(profile["list_price"]),
            "currency": s("BRL"),
            "rating": n(profile["rating"]),
            "review_count": n(rng.randint(0, 25_000)),
            "browse_nodes": ss(tags),
            "attributes": m(
                {
                    "color": s(profile["color"]),
                    "model": s(profile["model"]),
                    "package_weight_grams": n(rng.randint(120, 7_500)),
                    "hazmat": b(rng.random() < 0.015),
                }
            ),
            "created_at": s(iso_at(product_index, rng, 900)),
            "updated_at": s(iso_at(product_index + 17, rng, 90)),
        }
        if include_vectors:
            item["embedding"] = vec([rng.uniform(-1, 1) for _ in range(8)])
        return item

    seller_index = rng.randint(0, max(0, rows_per_table - 1))
    discount = rng.uniform(0.78, 1.08)
    price = max(100, int(profile["list_price"] * discount))
    condition = weighted_choice(rng, [("NEW", 92), ("USED_LIKE_NEW", 4), ("USED_GOOD", 3), ("REFURBISHED", 1)])
    fulfillment = weighted_choice(rng, [("FBA", 72), ("MFN", 23), ("SFP", 5)])
    return {
        "asin": s(asin),
        "entity": s(f"OFFER#{seller_id(seller_index)}"),
        "seller_id": s(seller_id(seller_index)),
        "sku": s(sku_for(product_index, seller_index, rng.randint(1, 9))),
        "condition": s(condition),
        "fulfillment_channel": s(fulfillment),
        "shipping_template": s("PRIME_1D" if fulfillment == "FBA" else rng.choice(["STANDARD_3D", "EXPRESS_2D"])),
        "price_cents": n(price),
        "currency": s("BRL"),
        "buybox_eligible": b(condition == "NEW" and rng.random() < 0.82),
        "offer_status": s(weighted_choice(rng, [("ACTIVE", 94), ("PAUSED", 4), ("OUT_OF_STOCK", 2)])),
        "handling_time_days": n(0 if fulfillment == "FBA" else rng.randint(1, 5)),
        "created_at": s(iso_at(product_index + seller_index, rng, 365)),
        "updated_at": s(iso_at(product_index + seller_index + 5, rng, 30)),
    }


def seller_item(index: int, seed: int, *_: object) -> dict:
    rng = item_rng(seed, "MarketplaceSellers", index)
    prefix = rng.choice(SELLER_PREFIXES)
    vertical = rng.choice(SELLER_VERTICALS)
    country = weighted_choice(rng, [("BR", 78), ("US", 8), ("MX", 4), ("CA", 3), ("GB", 3), ("DE", 2), ("JP", 2)])
    rating = min(5.0, max(2.0, rng.gauss(4.35, 0.4)))
    status = weighted_choice(rng, [("ACTIVE", 94), ("VACATION", 3), ("SUSPENDED", 2), ("ONBOARDING", 1)])
    return {
        "seller_id": s(seller_id(index)),
        "display_name": s(f"{prefix} {vertical} Store {base36(index, 3)}"),
        "legal_name": s(f"{prefix} {vertical} Comercio {base36(index, 5)} Ltda"),
        "status": s(status),
        "kyc_status": s("VERIFIED" if status != "ONBOARDING" else "PENDING"),
        "seller_tier": s(weighted_choice(rng, [("standard", 60), ("pro", 30), ("enterprise", 10)])),
        "rating": n(rating),
        "rating_count": n(rng.randint(0, 120_000)),
        "ship_from_country": s(country),
        "default_fulfillment_channel": s(weighted_choice(rng, [("FBA", 55), ("MFN", 38), ("SFP", 7)])),
        "return_policy": s(weighted_choice(rng, [("30_DAY", 85), ("14_DAY", 10), ("NO_RETURN", 5)])),
        "created_at": s(iso_at(index, rng, 1_500)),
        "updated_at": s(iso_at(index + 13, rng, 120)),
    }


def customer_item(index: int, seed: int, *_: object) -> dict:
    rng = item_rng(seed, "MarketplaceCustomers", index)
    first = rng.choice(FIRST_NAMES)
    last = rng.choice(LAST_NAMES)
    state = rng.choice(BR_STATES)
    city = rng.choice(BR_CITIES)
    cid = customer_id(index)
    return {
        "customer_id": s(cid),
        "email": s(f"{first.lower()}.{last.lower()}.{index:06d}@example.com"),
        "name": s(f"{first} {last}"),
        "marketplace_id": s("ATVPDKIKX0DER-BR"),
        "prime_member": b(rng.random() < 0.42),
        "customer_segment": s(weighted_choice(rng, [("retail", 76), ("business", 16), ("student", 5), ("family", 3)])),
        "risk_tier": s(weighted_choice(rng, [("LOW", 92), ("MEDIUM", 7), ("HIGH", 1)])),
        "default_address_id": s(f"ADDR-{index:06d}-01"),
        "default_address": m(
            {
                "city": s(city),
                "state": s(state),
                "country": s("BR"),
                "postal_code": s(f"{rng.randint(10000, 99999)}-{rng.randint(0, 999):03d}"),
            }
        ),
        "created_at": s(iso_at(index, rng, 2_000)),
        "last_seen_at": s(iso_at(index + 29, rng, 14)),
    }


def order_group(index: int, line_items_per_order: int) -> tuple[int, int]:
    group_size = line_items_per_order + 1
    return index // group_size, index % group_size


def order_item(index: int, seed: int, line_items_per_order: int, rows_per_table: int) -> dict:
    rng = item_rng(seed, "MarketplaceOrders", index)
    group, pos = order_group(index, line_items_per_order)
    oid = order_id(group)
    customer_index = (group * 37 + rng.randint(0, max(1, rows_per_table - 1))) % rows_per_table
    placed_at = iso_at(group, rng, 180)

    if pos == 0:
        status = weighted_choice(rng, [("CREATED", 8), ("PAID", 45), ("SHIPPED", 22), ("DELIVERED", 20), ("CANCELLED", 5)])
        subtotal = rng.randint(2_000, 220_000)
        shipping = 0 if rng.random() < 0.68 else rng.choice([990, 1490, 1990, 2990])
        return {
            "order_id": s(oid),
            "line_id": s("HEADER"),
            "customer_id": s(customer_id(customer_index)),
            "order_status": s(status),
            "marketplace_id": s("ATVPDKIKX0DER-BR"),
            "payment_status": s("CAPTURED" if status in {"PAID", "SHIPPED", "DELIVERED"} else "PENDING"),
            "fulfillment_status": s(
                weighted_choice(rng, [("NOT_ALLOCATED", 15), ("PARTIALLY_ALLOCATED", 20), ("ALLOCATED", 35), ("SHIPPED", 30)])
            ),
            "currency": s("BRL"),
            "subtotal_cents": n(subtotal),
            "shipping_cents": n(shipping),
            "total_cents": n(subtotal + shipping),
            "channel": s(weighted_choice(rng, [("web", 48), ("mobile", 42), ("voice", 4), ("api", 6)])),
            "placed_at": s(placed_at),
            "updated_at": s(iso_at(group + 3, rng, 30)),
        }

    product_index = (group * 11 + pos * 7 + rng.randint(0, rows_per_table)) % max(1, rows_per_table // 2)
    seller_index = (group * 13 + pos * 31) % rows_per_table
    quantity = weighted_choice(rng, [("1", 78), ("2", 16), ("3", 4), ("4", 2)])
    unit_price = product_profile(product_index, seed)["list_price"]
    discount = rng.uniform(0.80, 1.05)
    price = max(100, int(unit_price * discount))
    qty = int(quantity)
    return {
        "order_id": s(oid),
        "line_id": s(f"ITEM#{pos:04d}"),
        "asin": s(asin_for(product_index)),
        "sku": s(sku_for(product_index, seller_index, rng.randint(1, 9))),
        "seller_id": s(seller_id(seller_index)),
        "quantity": n(qty),
        "unit_price_cents": n(price),
        "line_total_cents": n(price * qty),
        "fulfillment_node_id": s(rng.choice(FULFILLMENT_NODES)),
        "line_status": s(weighted_choice(rng, [("ALLOCATED", 42), ("PICKING", 18), ("SHIPPED", 25), ("PENDING_PAYMENT", 10), ("CANCELLED", 5)])),
        "gift_wrap": b(rng.random() < 0.07),
    }


def customer_order_item(index: int, seed: int, rows_per_table: int, *_: object) -> dict:
    rng = item_rng(seed, "CustomerOrders", index)
    customer_index = (index * 17 + rng.randint(0, max(0, rows_per_table - 1))) % max(1, rows_per_table)
    oid = order_id(index)
    placed_at = iso_at(index, rng, 180)
    status = weighted_choice(rng, [("CREATED", 8), ("PAID", 34), ("SHIPPED", 24), ("DELIVERED", 29), ("CANCELLED", 5)])
    return {
        "customer_id": s(customer_id(customer_index)),
        "order_id": s(f"{placed_at}#{oid}"),
        "canonical_order_id": s(oid),
        "order_status": s(status),
        "total_cents": n(rng.randint(2_000, 240_000)),
        "currency": s("BRL"),
        "item_count": n(rng.randint(1, 6)),
        "placed_at": s(placed_at),
        "last_event": s(weighted_choice(rng, [("ORDER_PLACED", 20), ("PAYMENT_CAPTURED", 25), ("FULFILLMENT_UPDATE", 35), ("DELIVERY_UPDATE", 20)])),
    }


def inventory_item(index: int, seed: int, rows_per_table: int, *_: object) -> dict:
    rng = item_rng(seed, "FulfillmentInventory", index)
    product_index = (index * 19 + rng.randint(0, rows_per_table)) % max(1, rows_per_table // 2)
    seller_index = (index * 23 + rng.randint(0, rows_per_table - 1)) % rows_per_table
    node = FULFILLMENT_NODES[index % len(FULFILLMENT_NODES)]
    available = max(0, int(rng.gauss(120, 55)))
    reserved = max(0, int(rng.gauss(12, 8)))
    safety = rng.randint(5, 60)
    return {
        "fulfillment_node_id": s(node),
        "sku": s(sku_for(product_index, seller_index, index % 10)),
        "asin": s(asin_for(product_index)),
        "seller_id": s(seller_id(seller_index)),
        "available_qty": n(available),
        "reserved_qty": n(reserved),
        "safety_stock": n(safety),
        "inventory_condition": s(weighted_choice(rng, [("SELLABLE", 93), ("DAMAGED", 2), ("UNSELLABLE", 2), ("INBOUND", 3)])),
        "region": s(node_region(node)),
        "reorder_recommended": b(available < safety),
        "last_reconciled_at": s(iso_at(index, rng, 7)),
    }


def node_region(node: str) -> str:
    if "GRU" in node or "VCP" in node or "SP" in node:
        return "BR-SP"
    if "GIG" in node or "RJ" in node:
        return "BR-RJ"
    if "CWB" in node:
        return "BR-PR"
    if "POA" in node:
        return "BR-RS"
    if "REC" in node:
        return "BR-PE"
    if "BSB" in node:
        return "BR-DF"
    return "BR"


GENERATOR_BY_TABLE: dict[str, Callable[..., dict]] = {
    "MarketplaceCatalog": catalog_item,
    "MarketplaceSellers": seller_item,
    "MarketplaceCustomers": customer_item,
    "MarketplaceOrders": order_item,
    "CustomerOrders": customer_order_item,
    "FulfillmentInventory": inventory_item,
}


CREATE_COMMANDS = {
    "MarketplaceCatalog": [
        "create-table",
        "--table-name",
        "MarketplaceCatalog",
        "--attribute-definitions",
        "AttributeName=asin,AttributeType=S",
        "--attribute-definitions",
        "AttributeName=entity,AttributeType=S",
        "--key-schema",
        "AttributeName=asin,KeyType=HASH",
        "--key-schema",
        "AttributeName=entity,KeyType=RANGE",
        "--billing-mode",
        "PAY_PER_REQUEST",
        "--storage-class",
        "disk",
        "--stream-specification",
        "StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES",
    ],
    "MarketplaceSellers": [
        "create-table",
        "--table-name",
        "MarketplaceSellers",
        "--attribute-definitions",
        "AttributeName=seller_id,AttributeType=S",
        "--key-schema",
        "AttributeName=seller_id,KeyType=HASH",
        "--billing-mode",
        "PAY_PER_REQUEST",
        "--storage-class",
        "disk",
    ],
    "MarketplaceCustomers": [
        "create-table",
        "--table-name",
        "MarketplaceCustomers",
        "--attribute-definitions",
        "AttributeName=customer_id,AttributeType=S",
        "--key-schema",
        "AttributeName=customer_id,KeyType=HASH",
        "--billing-mode",
        "PAY_PER_REQUEST",
        "--storage-class",
        "disk",
    ],
    "MarketplaceOrders": [
        "create-table",
        "--table-name",
        "MarketplaceOrders",
        "--attribute-definitions",
        "AttributeName=order_id,AttributeType=S",
        "--attribute-definitions",
        "AttributeName=line_id,AttributeType=S",
        "--key-schema",
        "AttributeName=order_id,KeyType=HASH",
        "--key-schema",
        "AttributeName=line_id,KeyType=RANGE",
        "--billing-mode",
        "PAY_PER_REQUEST",
        "--storage-class",
        "disk",
        "--stream-specification",
        "StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES",
    ],
    "CustomerOrders": [
        "create-table",
        "--table-name",
        "CustomerOrders",
        "--attribute-definitions",
        "AttributeName=customer_id,AttributeType=S",
        "--attribute-definitions",
        "AttributeName=order_id,AttributeType=S",
        "--key-schema",
        "AttributeName=customer_id,KeyType=HASH",
        "--key-schema",
        "AttributeName=order_id,KeyType=RANGE",
        "--billing-mode",
        "PAY_PER_REQUEST",
        "--storage-class",
        "disk",
    ],
    "FulfillmentInventory": [
        "create-table",
        "--table-name",
        "FulfillmentInventory",
        "--attribute-definitions",
        "AttributeName=fulfillment_node_id,AttributeType=S",
        "--attribute-definitions",
        "AttributeName=sku,AttributeType=S",
        "--key-schema",
        "AttributeName=fulfillment_node_id,KeyType=HASH",
        "--key-schema",
        "AttributeName=sku,KeyType=RANGE",
        "--billing-mode",
        "PAY_PER_REQUEST",
        "--storage-class",
        "disk",
        "--stream-specification",
        "StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES",
    ],
}

INDEX_COMMANDS = [
    [
        "create-index",
        "--table",
        "MarketplaceCatalog",
        "--name",
        "product_title_trigram",
        "--type",
        "trigram",
        "--field",
        "search_title",
        "--pk",
        "asin",
        "--sk",
        "entity",
    ],
    [
        "create-index",
        "--table",
        "MarketplaceCatalog",
        "--name",
        "product_brand_radix",
        "--type",
        "radix",
        "--field",
        "brand",
        "--pk",
        "asin",
        "--sk",
        "entity",
    ],
]


class CefasCLI:
    def __init__(self, args: argparse.Namespace, endpoint: str | None = None):
        self.bin = resolve_cefas_bin(args.cefas_bin)
        self.endpoint = endpoint or args.endpoint
        self.common = ["--endpoint", self.endpoint, "--output", "json", "--timeout", args.timeout]
        if args.insecure:
            self.common.append("--insecure")
        elif args.ca:
            self.common.extend(["--ca", args.ca])
        if args.token:
            self.common.extend(["--token", args.token])
        elif args.token_file:
            self.common.extend(["--token-file", args.token_file])
        self.verbose = args.verbose
        self.retry_initial_sleep = args.retry_initial_sleep
        self.retry_max_sleep = args.retry_max_sleep

    def run(self, command: list[str], retries: int = 3) -> subprocess.CompletedProcess[str]:
        full = [self.bin, *self.common, *command]
        delay = self.retry_initial_sleep
        last: subprocess.CompletedProcess[str] | None = None
        for attempt in range(1, retries + 1):
            if self.verbose:
                print("+ " + " ".join(full), file=sys.stderr)
            last = subprocess.run(full, text=True, capture_output=True)
            if last.returncode == 0:
                return last
            if attempt < retries:
                time.sleep(delay)
                delay = min(delay * 2, self.retry_max_sleep)
        assert last is not None
        raise RuntimeError(
            "command failed after "
            f"{retries} attempts: {' '.join(full)}\nstdout={last.stdout}\nstderr={last.stderr}"
        )


def status_field(obj: dict, *names: str):
    for name in names:
        if name in obj:
            return obj[name]
    return None


def parse_route_aware_endpoints(value: str) -> dict[str, str]:
    endpoints: dict[str, str] = {}
    for raw in value.split(","):
        raw = raw.strip()
        if not raw:
            continue
        if "=" not in raw:
            raise ValueError(f"route-aware endpoint must be node=host:port, got {raw!r}")
        node, endpoint = raw.split("=", 1)
        node = node.strip()
        endpoint = endpoint.strip()
        if not node or not endpoint:
            raise ValueError(f"route-aware endpoint must be node=host:port, got {raw!r}")
        endpoints[node] = endpoint
    return endpoints


def attr_canonical_bytes(attr: dict) -> bytes:
    if "S" in attr:
        return attr["S"].encode("utf-8")
    if "N" in attr:
        return attr["N"].encode("utf-8")
    if "B" in attr:
        value = attr["B"]
        if isinstance(value, str):
            return value.encode("utf-8")
        return bytes(value)
    raise ValueError(f"unsupported partition-key attribute: {attr}")


def token_range_contains(range_info: dict, token: int) -> bool:
    start = int(status_field(range_info, "Start", "start"))
    end = int(status_field(range_info, "End", "end"))
    if start == end:
        return True
    if start < end:
        return start <= token < end
    return token >= start or token < end


class ClusterWriteRouter:
    def __init__(self, status: dict, endpoint_by_node: dict[str, str]):
        self.endpoint_by_node = endpoint_by_node
        self.lock = threading.Lock()
        self.http_node_ids: dict[str, str] = {}
        self.learned_leaders: dict[int, str] = {}
        self.shards: list[dict] = []
        self.update(status)

    def update(self, status: dict) -> None:
        nodes = status_field(status, "Nodes", "nodes") or []
        http_node_ids: dict[str, str] = {}
        for node in nodes:
            node_id = status_field(node, "ID", "id")
            http_addr = status_field(node, "HTTPAddr", "httpAddr")
            if node_id and http_addr:
                http_node_ids[http_addr] = node_id

        shards = status_field(status, "Shards", "shards") or []
        routed: list[dict] = []
        for shard in shards:
            shard_id = int(status_field(shard, "ID", "id"))
            leader = status_field(shard, "LeaderHint", "leaderHint") or ""
            voters = status_field(shard, "Voters", "voters") or []
            leader = self.learned_leaders.get(shard_id, leader)
            if not leader and voters:
                leader = voters[0]
            if not leader:
                raise ValueError(f"shard {shard_id} has no leader hint")
            if leader not in self.endpoint_by_node:
                raise ValueError(f"missing route-aware endpoint for leader {leader!r}")
            routed.append(
                {
                    "id": shard_id,
                    "leader": leader,
                    "ranges": status_field(shard, "Ranges", "ranges") or [],
                }
            )
        if not routed:
            raise ValueError("cluster status has no shard placement")
        routed.sort(key=lambda item: item["id"])
        with self.lock:
            self.http_node_ids = http_node_ids
            self.shards = routed

    def target_for_pk(self, pk_bytes: bytes) -> tuple[int, str]:
        token = xxh64(pk_bytes)
        with self.lock:
            shards = list(self.shards)
        for shard in shards:
            for range_info in shard["ranges"]:
                if token_range_contains(range_info, token):
                    return shard["id"], shard["leader"]
        raise ValueError(f"no shard found for token {token}")

    def target_for_op(self, table: str, op: dict) -> tuple[int, str]:
        pk_name = PARTITION_KEY_BY_TABLE[table]
        if "PutRequest" in op:
            item = op["PutRequest"]["Item"]
        else:
            item = op["DeleteRequest"]["Key"]
        if pk_name not in item:
            raise ValueError(f"{table} op missing partition key {pk_name!r}")
        return self.target_for_pk(attr_canonical_bytes(item[pk_name]))

    def group_batch(self, table: str, batch: list[dict]) -> dict[tuple[int, str], list[dict]]:
        groups: dict[tuple[int, str], list[dict]] = {}
        for op in batch:
            target = self.target_for_op(table, op)
            groups.setdefault(target, []).append(op)
        return groups

    def learn_leader_from_error(self, shard_id: int, exc: Exception) -> str | None:
        match = re.search(r"retry at (https?://[^\s]+)", str(exc))
        if not match:
            return None
        retry_http = match.group(1).rstrip(".,")
        with self.lock:
            node_id = self.http_node_ids.get(retry_http)
            if not node_id:
                return None
            self.learned_leaders[shard_id] = node_id
            for shard in self.shards:
                if shard["id"] == shard_id:
                    shard["leader"] = node_id
                    break
            return node_id


def resolve_cefas_bin(value: str | None) -> str:
    if value:
        path = Path(value).expanduser()
        if path.exists():
            return str(path)
        found = shutil.which(value)
        if found:
            return found
        raise SystemExit(f"cefas binary not found: {value}")

    env_bin = os.environ.get("CEFAS_BIN")
    if env_bin:
        return resolve_cefas_bin(env_bin)

    for candidate in ("cefas", "cefasctl"):
        found = shutil.which(candidate)
        if found:
            return found

    target = ROOT / "tmp" / "cefasctl"
    target.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.setdefault("GOCACHE", "/private/tmp/cefas-gocache")
    print(f"+ go build -o {target} ./cmd/cefasctl", file=sys.stderr)
    subprocess.run(["go", "build", "-o", str(target), "./cmd/cefasctl"], cwd=ROOT, env=env, check=True)
    return str(target)


def existing_tables(cli: CefasCLI) -> set[str]:
    try:
        out = cli.run(["list-tables"], retries=1).stdout
        payload = json.loads(out or "{}")
        return set(payload.get("TableNames", []))
    except Exception:
        return set()


def drop_and_create(cli: CefasCLI, tables: list[str], drop_existing: bool) -> None:
    current = existing_tables(cli)
    if drop_existing:
        for table in tables:
            if table in current:
                print(f"drop {table}", file=sys.stderr)
                cli.run(["delete-table", "--table-name", table])
        current = existing_tables(cli)

    for table in tables:
        if table in current:
            print(f"create {table}: already exists", file=sys.stderr)
            continue
        print(f"create {table}", file=sys.stderr)
        cli.run(CREATE_COMMANDS[table])


def create_indexes(cli: CefasCLI) -> None:
    for command in INDEX_COMMANDS:
        name = command[command.index("--name") + 1]
        try:
            print(f"create index {name}", file=sys.stderr)
            cli.run(command)
        except RuntimeError as exc:
            text = str(exc)
            if "already exists" in text or "AlreadyExists" in text:
                print(f"create index {name}: already exists", file=sys.stderr)
                continue
            raise


def generate_batch(
    table: str,
    start: int,
    count: int,
    args: argparse.Namespace,
) -> list[dict]:
    gen = GENERATOR_BY_TABLE[table]
    reference_rows = args.reference_rows_per_table
    rows = []
    for index in range(start, start + count):
        if table == "MarketplaceCatalog":
            item = gen(index, args.seed, args.include_product_vectors, reference_rows)
        elif table == "MarketplaceOrders":
            item = gen(index, args.seed, args.line_items_per_order, reference_rows)
        elif table == "FulfillmentInventory":
            item = gen(index, args.seed, reference_rows)
        elif table == "CustomerOrders":
            item = gen(index, args.seed, reference_rows)
        else:
            item = gen(index, args.seed)
        rows.append(put(item))
    return rows


def write_batch_file(work_dir: Path, table: str, batch_no: int, batch: list[dict], suffix: str = "") -> Path:
    path = work_dir / f"{table}.{batch_no:06d}{suffix}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(batch, f, separators=(",", ":"))
    return path


def cluster_status(cli: CefasCLI) -> dict:
    return json.loads(cli.run(["cluster", "status"], retries=1).stdout)


def is_not_leader_error(exc: Exception) -> bool:
    return "not leader" in str(exc).lower()


def write_plain_batch(cli: CefasCLI, table_dir: Path, table: str, batch_no: int, batch: list[dict], args: argparse.Namespace) -> None:
    path = write_batch_file(table_dir, table, batch_no, batch)
    try:
        cli.run(["batch-write-item", "--table-name", table, "--request-items", f"file://{path}"], retries=args.retries)
    finally:
        if not args.keep_batch_files:
            path.unlink(missing_ok=True)


def write_routed_batch(
    table_dir: Path,
    table: str,
    batch_no: int,
    batch: list[dict],
    args: argparse.Namespace,
    print_lock: threading.Lock,
) -> None:
    router: ClusterWriteRouter = args.route_router
    route_clients: dict[str, CefasCLI] = args.route_clients
    status_cli: CefasCLI = args.route_status_cli
    delay = args.retry_initial_sleep
    last_error: Exception | None = None

    for attempt in range(1, args.retries + 1):
        groups = router.group_batch(table, batch)
        try:
            for (shard_id, leader), group in groups.items():
                path = write_batch_file(table_dir, table, batch_no, group, suffix=f".s{shard_id}.{leader}.try{attempt}")
                try:
                    route_clients[leader].run(
                        ["batch-write-item", "--table-name", table, "--request-items", f"file://{path}"],
                        retries=1,
                    )
                finally:
                    if not args.keep_batch_files:
                        path.unlink(missing_ok=True)
            return
        except RuntimeError as exc:
            last_error = exc
            if not is_not_leader_error(exc) or attempt >= args.retries:
                raise
            learned = None
            if "shard_id" in locals():
                learned = router.learn_leader_from_error(shard_id, exc)
                if learned:
                    route_clients.setdefault(learned, CefasCLI(args, endpoint=args.route_endpoint_map[learned]))
            with print_lock:
                print(
                    f"{table}: not-leader on shard {shard_id}; "
                    f"{'learned leader ' + learned if learned else 'refreshing shard leaders'} "
                    f"(batch {batch_no}, attempt {attempt}/{args.retries})",
                    file=sys.stderr,
                )
            if not learned:
                router.update(cluster_status(status_cli))
            if delay:
                time.sleep(delay)
                delay = min(delay * 2, args.retry_max_sleep)

    if last_error is not None:
        raise last_error


def load_table(cli: CefasCLI, table: str, args: argparse.Namespace, print_lock: threading.Lock) -> tuple[str, int, float]:
    start_time = time.monotonic()
    total = args.rows_per_table
    start_index = args.start_index
    stop_index = start_index + total
    batches = math.ceil(total / args.batch_size)
    table_dir = args.work_dir / table
    table_dir.mkdir(parents=True, exist_ok=True)
    inserted = 0

    for batch_no, start in enumerate(range(start_index, stop_index, args.batch_size), start=1):
        count = min(args.batch_size, stop_index - start)
        batch = generate_batch(table, start, count, args)
        if args.dry_run:
            if batch_no == 1:
                with print_lock:
                    print(json.dumps(batch[: min(2, len(batch))], indent=2))
            inserted += count
            continue

        if args.route_router is None:
            write_plain_batch(cli, table_dir, table, batch_no, batch, args)
        else:
            write_routed_batch(table_dir, table, batch_no, batch, args, print_lock)

        inserted += count
        if args.progress_every_batches and batch_no % args.progress_every_batches == 0:
            elapsed = max(0.001, time.monotonic() - start_time)
            with print_lock:
                print(
                    f"{table}: {inserted}/{total} rows "
                    f"({inserted / elapsed:,.0f} rows/s, batch {batch_no}/{batches})",
                    file=sys.stderr,
                )
        if args.batch_delay_ms:
            time.sleep(args.batch_delay_ms / 1000)

    elapsed = time.monotonic() - start_time
    return table, inserted, elapsed


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate and load 100K realistic marketplace rows per table into CefasDB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--endpoint", default=os.environ.get("CEFAS_ENDPOINT", "127.0.0.1:9090"))
    parser.add_argument("--cefas-bin", default=os.environ.get("CEFAS_BIN"))
    parser.add_argument("--timeout", default=os.environ.get("CEFAS_TIMEOUT", "300s"))
    parser.add_argument("--insecure", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--ca", default=os.environ.get("CEFAS_CA", ""))
    parser.add_argument("--token", default=os.environ.get("CEFAS_TOKEN", ""))
    parser.add_argument("--token-file", default=os.environ.get("CEFAS_TOKEN_FILE", ""))
    parser.add_argument(
        "--route-aware-endpoints",
        default=os.environ.get("CEFAS_ROUTE_AWARE_ENDPOINTS", ""),
        help="Comma-separated node=grpc-host:port map for leader-aware multi-shard writes.",
    )
    parser.add_argument("--rows-per-table", type=int, default=100_000)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--reference-rows-per-table", type=int)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--line-items-per-order", type=int, default=3)
    parser.add_argument("--tables", nargs="+", choices=DEFAULT_TABLES, default=DEFAULT_TABLES)
    parser.add_argument("--create-tables", action="store_true")
    parser.add_argument("--drop-existing", action="store_true")
    parser.add_argument("--create-indexes", action="store_true")
    parser.add_argument("--create-only", action="store_true")
    parser.add_argument("--include-product-vectors", action="store_true")
    parser.add_argument("--work-dir", type=Path, default=Path("/private/tmp/cefas-marketplace-seed"))
    parser.add_argument("--keep-batch-files", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-initial-sleep", type=float, default=0.5)
    parser.add_argument("--retry-max-sleep", type=float, default=30.0)
    parser.add_argument("--batch-delay-ms", type=float, default=0.0)
    parser.add_argument("--progress-every-batches", type=int, default=20)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    if args.rows_per_table <= 0:
        parser.error("--rows-per-table must be > 0")
    if args.start_index < 0:
        parser.error("--start-index must be >= 0")
    if args.reference_rows_per_table is None:
        args.reference_rows_per_table = args.rows_per_table
    if args.reference_rows_per_table <= 0:
        parser.error("--reference-rows-per-table must be > 0")
    if args.batch_size <= 0:
        parser.error("--batch-size must be > 0")
    if args.workers <= 0:
        parser.error("--workers must be > 0")
    if args.line_items_per_order <= 0:
        parser.error("--line-items-per-order must be > 0")
    if args.retry_initial_sleep < 0:
        parser.error("--retry-initial-sleep must be >= 0")
    if args.retry_max_sleep < args.retry_initial_sleep:
        parser.error("--retry-max-sleep must be >= --retry-initial-sleep")
    if args.batch_delay_ms < 0:
        parser.error("--batch-delay-ms must be >= 0")
    if args.drop_existing and not args.create_tables:
        parser.error("--drop-existing requires --create-tables")
    if args.create_only and not args.create_tables:
        parser.error("--create-only requires --create-tables")
    try:
        args.route_endpoint_map = parse_route_aware_endpoints(args.route_aware_endpoints)
    except ValueError as exc:
        parser.error(str(exc))
    args.route_router = None
    args.route_clients = {}
    args.route_status_cli = None

    args.work_dir.mkdir(parents=True, exist_ok=True)
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    cli = CefasCLI(args)

    if args.create_tables and not args.dry_run:
        drop_and_create(cli, args.tables, args.drop_existing)
        if args.create_indexes:
            create_indexes(cli)
        if args.create_only:
            print(
                json.dumps(
                    {
                        "rows": 0,
                        "tables": args.tables,
                        "create_only": True,
                        "dry_run": False,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0

    if args.route_endpoint_map and not args.dry_run:
        args.route_status_cli = cli
        args.route_router = ClusterWriteRouter(cluster_status(cli), args.route_endpoint_map)
        args.route_clients = {
            node: CefasCLI(args, endpoint=endpoint)
            for node, endpoint in sorted(args.route_endpoint_map.items())
        }
        print(
            "route-aware writes enabled: "
            + ",".join(f"{node}={endpoint}" for node, endpoint in sorted(args.route_endpoint_map.items())),
            file=sys.stderr,
        )

    print(
        f"loading rows_per_table={args.rows_per_table} tables={','.join(args.tables)} "
        f"start_index={args.start_index} reference_rows_per_table={args.reference_rows_per_table} "
        f"batch_size={args.batch_size} workers={args.workers} endpoint={args.endpoint}",
        file=sys.stderr,
    )

    print_lock = threading.Lock()
    started = time.monotonic()
    results: list[tuple[str, int, float]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        future_by_table = {
            pool.submit(load_table, cli, table, args, print_lock): table
            for table in args.tables
        }
        for future in concurrent.futures.as_completed(future_by_table):
            table = future_by_table[future]
            try:
                result = future.result()
            except Exception as exc:
                print(f"{table}: failed: {exc}", file=sys.stderr)
                return 1
            results.append(result)
            _, rows, elapsed = result
            print(f"{table}: done {rows} rows in {elapsed:.1f}s", file=sys.stderr)

    if args.create_indexes and not args.dry_run:
        create_indexes(cli)

    elapsed = max(0.001, time.monotonic() - started)
    total_rows = sum(rows for _, rows, _ in results)
    print(
        json.dumps(
            {
                "rows": total_rows,
                "tables": {table: {"rows": rows, "seconds": round(seconds, 3)} for table, rows, seconds in results},
                "seconds": round(elapsed, 3),
                "rows_per_second": round(total_rows / elapsed, 2),
                "dry_run": args.dry_run,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
