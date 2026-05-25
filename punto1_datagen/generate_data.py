from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import numpy as np
import pandas as pd
from faker import Faker


EVENT_DISTRIBUTION = {
    "page_view": 0.40,
    "product_view": 0.25,
    "click": 0.20,
    "search": 0.10,
    "cart_event": 0.05,
}

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
DEVICE_PROBABILITIES = [0.55, 0.35, 0.10]

COUNTRIES = ["Colombia", "Mexico", "Argentina", "Chile", "Peru", "Ecuador", "Uruguay"]
COUNTRY_PROBABILITIES = [0.40, 0.25, 0.15, 0.06, 0.05, 0.05, 0.04]

PAGE_TYPES = ["home", "category", "product", "cart", "checkout"]
REFERRERS = [
    "direct",
    "google",
    "facebook",
    "instagram",
    "email",
    "affiliate",
    "price_comparison",
]
ELEMENT_TYPES = ["button", "banner", "link", "image", "menu_item", "filter", "sort"]
CART_ACTIONS = ["add", "remove"]
SEARCH_TERMS = [
    "smartphone",
    "laptop",
    "audifonos bluetooth",
    "zapatillas",
    "camiseta",
    "cafetera",
    "monitor",
    "silla ergonomica",
    "reloj inteligente",
    "mochila",
    "tablet",
    "teclado mecanico",
    "mouse gamer",
    "camara seguridad",
    "licuadora",
]


def lognormal_parameters(mean: float, std: float) -> tuple[float, float]:
    variance = std**2
    sigma = np.sqrt(np.log(1 + variance / mean**2))
    mu = np.log(mean) - sigma**2 / 2
    return mu, sigma


def exact_event_counts(records_per_day: int) -> dict[str, int]:
    counts = {
        event_type: int(records_per_day * weight)
        for event_type, weight in EVENT_DISTRIBUTION.items()
    }
    remainder = records_per_day - sum(counts.values())
    if remainder:
        counts["page_view"] += remainder
    return counts


def random_timestamps(rng: np.random.Generator, day: datetime, count: int) -> list[str]:
    seconds = rng.integers(0, 24 * 60 * 60, size=count)
    timestamps = [day + timedelta(seconds=int(second)) for second in seconds]
    timestamps.sort()
    return [timestamp.isoformat(timespec="seconds") for timestamp in timestamps]


def random_time_on_page(rng: np.random.Generator, count: int) -> np.ndarray:
    mu, sigma = lognormal_parameters(mean=45, std=30)
    values = rng.lognormal(mean=mu, sigma=sigma, size=count)
    return np.clip(np.round(values), 1, 600).astype(int)


def build_catalog(fake: Faker) -> pd.DataFrame:
    categories = [f"category_{index:02d}" for index in range(1, 51)]
    products = []
    for index in range(1, 501):
        products.append(
            {
                "product_id": f"PROD-{index:04d}",
                "category": categories[(index - 1) % len(categories)],
                "price": round(fake.pyfloat(min_value=8, max_value=1500, right_digits=2), 2),
            }
        )
    return pd.DataFrame(products)


def build_sessions(
    rng: np.random.Generator,
    users: list[str],
    records_per_day: int,
) -> pd.DataFrame:
    session_count = max(len(users), records_per_day // 8)
    session_users = rng.choice(users, size=session_count, replace=True)
    return pd.DataFrame(
        {
            "session_id": [str(uuid4()) for _ in range(session_count)],
            "user_id": session_users,
        }
    )


def sample_sessions(
    rng: np.random.Generator,
    sessions: pd.DataFrame,
    count: int,
) -> pd.DataFrame:
    selected = rng.integers(0, len(sessions), size=count)
    return sessions.iloc[selected].reset_index(drop=True)


def page_url(page_type: str, product_id: str | None = None, category: str | None = None) -> str:
    if page_type == "home":
        return "/"
    if page_type == "category":
        return f"/category/{category or 'category_01'}"
    if page_type == "product":
        return f"/product/{product_id or 'PROD-0001'}"
    return f"/{page_type}"


def generate_page_views(
    rng: np.random.Generator,
    sessions: pd.DataFrame,
    catalog: pd.DataFrame,
    day: datetime,
    count: int,
) -> pd.DataFrame:
    sampled_sessions = sample_sessions(rng, sessions, count)
    page_types = rng.choice(PAGE_TYPES, size=count, p=[0.30, 0.25, 0.30, 0.10, 0.05])
    sampled_products = catalog.iloc[rng.integers(0, len(catalog), size=count)].reset_index(drop=True)
    urls = [
        page_url(page_type, product_id=row.product_id, category=row.category)
        for page_type, row in zip(page_types, sampled_products.itertuples(index=False))
    ]
    return pd.DataFrame(
        {
            "user_id": sampled_sessions["user_id"],
            "session_id": sampled_sessions["session_id"],
            "page_url": urls,
            "page_type": page_types,
            "timestamp": random_timestamps(rng, day, count),
            "time_on_page_seconds": random_time_on_page(rng, count),
            "referrer": rng.choice(REFERRERS, size=count, p=[0.30, 0.28, 0.12, 0.10, 0.08, 0.07, 0.05]),
            "device_type": rng.choice(DEVICE_TYPES, size=count, p=DEVICE_PROBABILITIES),
            "country": rng.choice(COUNTRIES, size=count, p=COUNTRY_PROBABILITIES),
        }
    )


def generate_clicks(
    rng: np.random.Generator,
    sessions: pd.DataFrame,
    catalog: pd.DataFrame,
    day: datetime,
    count: int,
) -> pd.DataFrame:
    sampled_sessions = sample_sessions(rng, sessions, count)
    element_types = rng.choice(ELEMENT_TYPES, size=count, p=[0.28, 0.16, 0.20, 0.12, 0.10, 0.09, 0.05])
    page_types = rng.choice(PAGE_TYPES, size=count, p=[0.25, 0.25, 0.35, 0.10, 0.05])
    sampled_products = catalog.iloc[rng.integers(0, len(catalog), size=count)].reset_index(drop=True)
    urls = [
        page_url(page_type, product_id=row.product_id, category=row.category)
        for page_type, row in zip(page_types, sampled_products.itertuples(index=False))
    ]
    return pd.DataFrame(
        {
            "user_id": sampled_sessions["user_id"],
            "session_id": sampled_sessions["session_id"],
            "element_id": [f"{element_type}_{index:05d}" for index, element_type in enumerate(element_types, start=1)],
            "element_type": element_types,
            "page_url": urls,
            "timestamp": random_timestamps(rng, day, count),
            "x_position": rng.integers(0, 1921, size=count),
            "y_position": rng.integers(0, 1081, size=count),
        }
    )


def generate_searches(
    rng: np.random.Generator,
    sessions: pd.DataFrame,
    day: datetime,
    count: int,
) -> pd.DataFrame:
    sampled_sessions = sample_sessions(rng, sessions, count)
    return pd.DataFrame(
        {
            "user_id": sampled_sessions["user_id"],
            "session_id": sampled_sessions["session_id"],
            "query": rng.choice(SEARCH_TERMS, size=count),
            "results_count": rng.poisson(lam=42, size=count).clip(0, 500),
            "timestamp": random_timestamps(rng, day, count),
        }
    )


def generate_product_views(
    rng: np.random.Generator,
    sessions: pd.DataFrame,
    catalog: pd.DataFrame,
    day: datetime,
    count: int,
) -> pd.DataFrame:
    sampled_sessions = sample_sessions(rng, sessions, count)
    sampled_products = catalog.iloc[rng.integers(0, len(catalog), size=count)].reset_index(drop=True)
    return pd.DataFrame(
        {
            "user_id": sampled_sessions["user_id"],
            "session_id": sampled_sessions["session_id"],
            "product_id": sampled_products["product_id"],
            "category": sampled_products["category"],
            "price": sampled_products["price"],
            "timestamp": random_timestamps(rng, day, count),
            "time_on_page_seconds": random_time_on_page(rng, count),
        }
    )


def generate_cart_events(
    rng: np.random.Generator,
    sessions: pd.DataFrame,
    catalog: pd.DataFrame,
    day: datetime,
    count: int,
) -> pd.DataFrame:
    sampled_sessions = sample_sessions(rng, sessions, count)
    sampled_products = catalog.iloc[rng.integers(0, len(catalog), size=count)].reset_index(drop=True)
    return pd.DataFrame(
        {
            "user_id": sampled_sessions["user_id"],
            "session_id": sampled_sessions["session_id"],
            "product_id": sampled_products["product_id"],
            "action": rng.choice(CART_ACTIONS, size=count, p=[0.78, 0.22]),
            "timestamp": random_timestamps(rng, day, count),
        }
    )


def write_event_file(output_dir: Path, event_type: str, frame: pd.DataFrame) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_dir / f"{event_type}.csv", index=False)


def generate_day(
    rng: np.random.Generator,
    users: list[str],
    catalog: pd.DataFrame,
    base_output: Path,
    day: datetime,
    records_per_day: int,
) -> dict[str, int]:
    sessions = build_sessions(rng, users, records_per_day)
    counts = exact_event_counts(records_per_day)
    output_dir = base_output / f"year={day:%Y}" / f"month={day:%m}" / f"day={day:%d}"

    generators = {
        "page_view": lambda amount: generate_page_views(rng, sessions, catalog, day, amount),
        "product_view": lambda amount: generate_product_views(rng, sessions, catalog, day, amount),
        "click": lambda amount: generate_clicks(rng, sessions, catalog, day, amount),
        "search": lambda amount: generate_searches(rng, sessions, day, amount),
        "cart_event": lambda amount: generate_cart_events(rng, sessions, catalog, day, amount),
    }

    for event_type in EVENT_DISTRIBUTION:
        frame = generators[event_type](counts[event_type])
        write_event_file(output_dir, event_type, frame)

    return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic ShopStream event data.")
    parser.add_argument("--output-dir", default="data", help="Base output directory for partitioned CSV files.")
    parser.add_argument("--records-per-day", type=int, default=500_000, help="Total records generated for each day.")
    parser.add_argument("--start-date", default="2025-06-01", help="First date to generate, in YYYY-MM-DD format.")
    parser.add_argument("--days", type=int, default=5, help="Number of consecutive days to generate.")
    parser.add_argument("--seed", type=int, default=20250601, help="Random seed for reproducible data.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.records_per_day < 1:
        raise ValueError("--records-per-day must be greater than zero")
    if args.days < 1:
        raise ValueError("--days must be greater than zero")

    fake = Faker("es_CO")
    Faker.seed(args.seed)
    rng = np.random.default_rng(args.seed)

    users = [str(uuid4()) for _ in range(10_000)]
    catalog = build_catalog(fake)
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    output_dir = Path(args.output_dir)

    summary = {event_type: 0 for event_type in EVENT_DISTRIBUTION}
    for offset in range(args.days):
        day = start_date + timedelta(days=offset)
        counts = generate_day(rng, users, catalog, output_dir, day, args.records_per_day)
        for event_type, count in counts.items():
            summary[event_type] += count
        print(f"Generated {sum(counts.values()):,} records for {day:%Y-%m-%d}")

    print("\nResumen por tipo de evento:")
    for event_type in EVENT_DISTRIBUTION:
        print(f"{event_type}: {summary[event_type]:,}")
    print(f"total: {sum(summary.values()):,}")


if __name__ == "__main__":
    main()
