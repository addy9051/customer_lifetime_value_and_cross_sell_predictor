"""
Synthetic Corporate Travel Data Generator
==========================================
Generates realistic B2B corporate travel data modeled after Amex GBT client profiles.

Entities:
    - Corporate Accounts (5,000)
    - Traveler Profiles (~50,000)
    - Bookings (~1,000,000)
    - Service Contracts (~15,000)
    - Support Tickets (~100,000)

Usage:
    python -m data.generate_synthetic_data --output-dir data/synthetic --seed 42
"""

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)

# =============================================================================
# Constants & Configuration
# =============================================================================

NUM_ACCOUNTS = 5_000
TARGET_BOOKINGS = 1_000_000
OBSERVATION_START = datetime(2021, 1, 1)
OBSERVATION_END = datetime(2024, 12, 31)
OBSERVATION_DAYS = (OBSERVATION_END - OBSERVATION_START).days

INDUSTRIES = [
    "Technology", "Financial Services", "Healthcare", "Manufacturing",
    "Consulting", "Legal", "Energy", "Pharmaceuticals", "Retail",
    "Media & Entertainment", "Government", "Education", "Telecommunications",
    "Logistics & Transportation", "Real Estate",
]

REGIONS = [
    "North America", "EMEA", "APAC", "LATAM",
]

TIERS = ["Platinum", "Gold", "Silver", "Bronze"]
TIER_WEIGHTS = [0.08, 0.17, 0.35, 0.40]

# Amex GBT product portfolio
GBT_PRODUCTS = ["Neo", "Egencia Analytics Studio", "Meetings & Events", "Travel Consulting"]

BOOKING_TYPES = ["Flight", "Hotel", "Rail", "Car"]
BOOKING_TYPE_WEIGHTS = [0.40, 0.35, 0.15, 0.10]

DESTINATION_REGIONS = [
    "Domestic", "Europe", "Asia-Pacific", "Latin America",
    "Middle East & Africa", "Canada",
]
DESTINATION_WEIGHTS = [0.45, 0.22, 0.15, 0.08, 0.05, 0.05]

TICKET_SEVERITIES = ["P1", "P2", "P3", "P4"]
TICKET_SEVERITY_WEIGHTS = [0.03, 0.12, 0.40, 0.45]

TICKET_CATEGORIES = ["Billing", "Booking", "Technical", "Policy"]
TICKET_CATEGORY_WEIGHTS = [0.20, 0.35, 0.25, 0.20]

TRAVELER_TIERS = ["VIP", "Frequent", "Standard"]
TRAVELER_TIER_WEIGHTS = [0.05, 0.25, 0.70]

TRAVELER_ROLES = [
    "Executive", "Senior Manager", "Manager", "Director",
    "Analyst", "Consultant", "Engineer", "Sales Representative",
]


# =============================================================================
# Account-level behavior profiles (drives realistic correlations)
# =============================================================================

def _account_behavior_profile(tier: str, is_churned: bool, rng: np.random.Generator) -> dict:
    """Generate correlated behavioral parameters for one account.

    Higher-tier accounts book more, spend more, have better support health,
    and are less likely to have high cancellation rates — unless they are
    churning, in which case metrics degrade.
    """
    tier_multiplier = {"Platinum": 3.0, "Gold": 2.0, "Silver": 1.2, "Bronze": 0.7}[tier]
    churn_decay = 0.4 if is_churned else 1.0  # churned accounts trail off

    return {
        "avg_travelers": max(2, int(rng.normal(10 * tier_multiplier, 3))),
        "booking_rate_per_traveler_per_year": max(0.5, rng.normal(5.0 * tier_multiplier * churn_decay, 2.0)),
        "avg_booking_amount": max(50, rng.normal(400 * tier_multiplier, 100)),
        "cancellation_prob": min(0.5, max(0.02, rng.beta(2, 10) + (0.15 if is_churned else 0))),
        "out_of_policy_prob": min(0.4, max(0.01, rng.beta(2, 12) + (0.08 if is_churned else 0))),
        "ticket_rate_per_month": max(0.1, rng.exponential(0.8 * (2.0 if is_churned else 1.0))),
        "product_adoption_prob": min(0.9, 0.2 + 0.15 * tier_multiplier * churn_decay),
    }


# =============================================================================
# Entity Generators
# =============================================================================

def generate_accounts(rng: np.random.Generator) -> pd.DataFrame:
    """Generate 5,000 corporate client accounts."""
    logger.info("Generating %d corporate accounts...", NUM_ACCOUNTS)

    tiers = rng.choice(TIERS, size=NUM_ACCOUNTS, p=TIER_WEIGHTS)
    industries = rng.choice(INDUSTRIES, size=NUM_ACCOUNTS)
    regions = rng.choice(REGIONS, size=NUM_ACCOUNTS, p=[0.45, 0.30, 0.18, 0.07])

    # Onboarding dates spread across pre-observation + observation window
    onboarding_days = rng.integers(0, OBSERVATION_DAYS + 365, size=NUM_ACCOUNTS)
    onboarding_dates = [OBSERVATION_START - timedelta(days=365) + timedelta(days=int(d)) for d in onboarding_days]

    # Churn: ~18% of accounts churn during observation period
    churn_mask = rng.random(NUM_ACCOUNTS) < 0.18
    # Tier-adjusted churn: Bronze/Silver churn more
    tier_churn_boost = np.array([{"Platinum": -0.08, "Gold": -0.04, "Silver": 0.03, "Bronze": 0.08}[t] for t in tiers])
    churn_mask = (rng.random(NUM_ACCOUNTS) + tier_churn_boost) < 0.18

    churn_dates = []
    for i in range(NUM_ACCOUNTS):
        if churn_mask[i]:
            # Churn happens somewhere in the observation window, biased later
            earliest = max(onboarding_dates[i], OBSERVATION_START)
            days_available = (OBSERVATION_END - earliest).days
            if days_available > 0:
                churn_day = int(rng.beta(2, 1.5) * days_available)  # biased toward end
                churn_dates.append(earliest + timedelta(days=churn_day))
            else:
                churn_mask[i] = False
                churn_dates.append(None)
        else:
            churn_dates.append(None)

    # Annual contract value correlated with tier
    acv_base = {"Platinum": 500_000, "Gold": 200_000, "Silver": 80_000, "Bronze": 25_000}
    acv = np.array([max(5000, rng.normal(acv_base[t], acv_base[t] * 0.3)) for t in tiers])

    accounts = pd.DataFrame({
        "account_id": [f"ACCT-{i:05d}" for i in range(NUM_ACCOUNTS)],
        "company_name": [fake.company() for _ in range(NUM_ACCOUNTS)],
        "industry": industries,
        "region": regions,
        "tier": tiers,
        "onboarding_date": onboarding_dates,
        "is_churned": churn_mask,
        "churn_date": churn_dates,
        "annual_contract_value": np.round(acv, 2),
    })

    logger.info(
        "  → Churn rate: %.1f%% | Tier distribution: %s",
        churn_mask.mean() * 100,
        dict(zip(*np.unique(tiers, return_counts=True))),
    )
    return accounts


def generate_service_contracts(accounts: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Generate service contracts for each account based on tier/behavior."""
    logger.info("Generating service contracts...")
    rows = []

    for _, acct in accounts.iterrows():
        profile = _account_behavior_profile(acct["tier"], acct["is_churned"], rng)
        num_products = max(1, int(rng.binomial(len(GBT_PRODUCTS), profile["product_adoption_prob"])))
        adopted_products = rng.choice(GBT_PRODUCTS, size=num_products, replace=False)

        for product in adopted_products:
            start = acct["onboarding_date"] + timedelta(days=int(rng.integers(0, 180)))
            # Contract typically 12 or 24 months
            duration_months = rng.choice([12, 24], p=[0.6, 0.4])
            end = start + timedelta(days=int(duration_months * 30.44))

            is_active = True
            if acct["is_churned"] and acct["churn_date"] is not None:
                if end > acct["churn_date"]:
                    end = acct["churn_date"]
                    is_active = False

            contract_value = max(
                1000,
                rng.normal(acct["annual_contract_value"] * 0.15, acct["annual_contract_value"] * 0.05),
            )

            rows.append({
                "contract_id": f"CTR-{len(rows):06d}",
                "account_id": acct["account_id"],
                "product": product,
                "start_date": start,
                "end_date": end,
                "contract_value": round(contract_value, 2),
                "is_active": is_active,
            })

    contracts = pd.DataFrame(rows)
    logger.info("  → Generated %d contracts across %d products", len(contracts), len(GBT_PRODUCTS))
    return contracts


def generate_travelers(accounts: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Generate traveler profiles linked to accounts."""
    logger.info("Generating traveler profiles...")
    rows = []

    for _, acct in accounts.iterrows():
        profile = _account_behavior_profile(acct["tier"], acct["is_churned"], rng)
        num_travelers = profile["avg_travelers"]

        for _ in range(num_travelers):
            rows.append({
                "traveler_id": f"TRV-{len(rows):07d}",
                "account_id": acct["account_id"],
                "role": rng.choice(TRAVELER_ROLES),
                "travel_tier": rng.choice(TRAVELER_TIERS, p=TRAVELER_TIER_WEIGHTS),
            })

    travelers = pd.DataFrame(rows)
    logger.info("  → Generated %d travelers (avg %.1f per account)", len(travelers), len(travelers) / NUM_ACCOUNTS)
    return travelers


def generate_bookings(
    travelers: pd.DataFrame,
    accounts: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate ~1M booking records with realistic seasonality and correlations."""
    logger.info("Generating bookings (target: ~%d)...", TARGET_BOOKINGS)

    # Pre-compute account lookup for churn dates
    acct_lookup = accounts.set_index("account_id")[
        ["tier", "is_churned", "churn_date", "onboarding_date"]
    ].to_dict("index")

    # Estimate bookings per traveler to hit ~1M total
    num_travelers = len(travelers)
    avg_bookings_per_traveler = TARGET_BOOKINGS / num_travelers
    observation_years = OBSERVATION_DAYS / 365.25

    rows = []
    booking_counter = 0

    for _, traveler in travelers.iterrows():
        acct_info = acct_lookup[traveler["account_id"]]
        profile = _account_behavior_profile(acct_info["tier"], acct_info["is_churned"], rng)

        # Scale booking rate to hit target
        raw_rate = profile["booking_rate_per_traveler_per_year"]
        scale_factor = avg_bookings_per_traveler / (5.0 * observation_years)  # normalize around median
        num_bookings = max(0, int(rng.poisson(raw_rate * observation_years * scale_factor)))

        # Effective window: from account onboarding (or obs start) to churn (or obs end)
        window_start = max(acct_info["onboarding_date"], OBSERVATION_START)
        window_end = OBSERVATION_END
        if acct_info["is_churned"] and acct_info["churn_date"] is not None:
            window_end = min(window_end, acct_info["churn_date"])

        window_days = (window_end - window_start).days
        if window_days <= 0 or num_bookings == 0:
            continue

        # Generate booking dates with seasonal patterns
        # More bookings in Q1 (Jan-Mar) and Q3 (Sep-Nov) — corporate travel peaks
        booking_day_offsets = rng.integers(0, window_days, size=num_bookings)
        booking_dates = [window_start + timedelta(days=int(d)) for d in booking_day_offsets]

        for bdate in booking_dates:
            # Seasonal amount variation
            month = bdate.month
            seasonal_mult = 1.0 + 0.15 * np.sin(2 * np.pi * (month - 3) / 12)  # peak in Q1/Q3

            btype = rng.choice(BOOKING_TYPES, p=BOOKING_TYPE_WEIGHTS)
            type_mult = {"Flight": 1.0, "Hotel": 0.6, "Rail": 0.3, "Car": 0.25}[btype]

            amount = max(
                20,
                rng.lognormal(
                    np.log(profile["avg_booking_amount"] * type_mult * seasonal_mult),
                    0.4,
                ),
            )

            is_cancelled = rng.random() < profile["cancellation_prob"]
            is_out_of_policy = rng.random() < profile["out_of_policy_prob"]

            # Travel date is 1-60 days after booking date
            travel_date = bdate + timedelta(days=int(rng.integers(1, 61)))

            rows.append({
                "booking_id": f"BKG-{booking_counter:08d}",
                "traveler_id": traveler["traveler_id"],
                "booking_type": btype,
                "booking_date": bdate,
                "travel_date": travel_date,
                "amount": round(amount, 2),
                "is_out_of_policy": is_out_of_policy,
                "is_cancelled": is_cancelled,
                "destination_region": rng.choice(DESTINATION_REGIONS, p=DESTINATION_WEIGHTS),
            })
            booking_counter += 1

    bookings = pd.DataFrame(rows)
    logger.info("  → Generated %d bookings (target was %d)", len(bookings), TARGET_BOOKINGS)

    # Log distribution summary
    if len(bookings) > 0:
        logger.info(
            "  → Amount stats: mean=$%.0f, median=$%.0f, max=$%.0f",
            bookings["amount"].mean(),
            bookings["amount"].median(),
            bookings["amount"].max(),
        )
        logger.info("  → Cancel rate: %.1f%% | OOP rate: %.1f%%",
                     bookings["is_cancelled"].mean() * 100,
                     bookings["is_out_of_policy"].mean() * 100)

    return bookings


def generate_support_tickets(accounts: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Generate support tickets with severity and resolution time correlated to account health."""
    logger.info("Generating support tickets...")
    rows = []

    for _, acct in accounts.iterrows():
        profile = _account_behavior_profile(acct["tier"], acct["is_churned"], rng)

        window_start = max(acct["onboarding_date"], OBSERVATION_START)
        window_end = OBSERVATION_END
        if acct["is_churned"] and acct["churn_date"] is not None:
            window_end = min(window_end, acct["churn_date"])

        window_months = max(1, (window_end - window_start).days / 30.44)
        num_tickets = max(0, int(rng.poisson(profile["ticket_rate_per_month"] * window_months)))
        window_days = (window_end - window_start).days

        if window_days <= 0:
            continue

        for _ in range(num_tickets):
            created = window_start + timedelta(days=int(rng.integers(0, max(1, window_days))))
            severity = rng.choice(TICKET_SEVERITIES, p=TICKET_SEVERITY_WEIGHTS)

            # Resolution time inversely correlated with severity
            severity_hours = {"P1": 2, "P2": 8, "P3": 24, "P4": 72}[severity]
            resolution_hours = max(0.5, rng.lognormal(np.log(severity_hours), 0.6))

            resolved = created + timedelta(hours=resolution_hours)

            rows.append({
                "ticket_id": f"TKT-{len(rows):07d}",
                "account_id": acct["account_id"],
                "created_date": created,
                "resolved_date": resolved,
                "severity": severity,
                "category": rng.choice(TICKET_CATEGORIES, p=TICKET_CATEGORY_WEIGHTS),
                "resolution_hours": round(resolution_hours, 2),
            })

    tickets = pd.DataFrame(rows)
    logger.info("  → Generated %d support tickets", len(tickets))
    return tickets


# =============================================================================
# CLV Proxy Label
# =============================================================================

def compute_clv_labels(
    accounts: pd.DataFrame,
    bookings: pd.DataFrame,
    travelers: pd.DataFrame,
    contracts: pd.DataFrame,
    tickets: pd.DataFrame,
) -> pd.DataFrame:
    """Compute a 12-month forward CLV proxy for each account.

    CLV_12m = Σ(booking revenue, forward 12 months)
            + Σ(active contract value, annualized)
            - Σ(support cost proxy: $50/P4, $150/P3, $500/P2, $2000/P1)

    The cutoff date is set at the midpoint of the observation window to allow
    a 12-month look-forward while retaining historical features.
    """
    logger.info("Computing CLV proxy labels...")

    cutoff_date = OBSERVATION_START + timedelta(days=OBSERVATION_DAYS // 2)
    forward_end = cutoff_date + timedelta(days=365)

    # Map traveler → account
    traveler_to_account = travelers.set_index("traveler_id")["account_id"].to_dict()
    bookings = bookings.copy()
    bookings["account_id"] = bookings["traveler_id"].map(traveler_to_account)

    # Forward booking revenue
    forward_bookings = bookings[
        (bookings["booking_date"] >= cutoff_date)
        & (bookings["booking_date"] < forward_end)
        & (~bookings["is_cancelled"])
    ]
    booking_revenue = forward_bookings.groupby("account_id")["amount"].sum().rename("booking_revenue_12m")

    # Active contract value (annualized)
    active_contracts = contracts[
        (contracts["start_date"] <= cutoff_date)
        & (contracts["end_date"] >= cutoff_date)
    ]
    contract_revenue = active_contracts.groupby("account_id")["contract_value"].sum().rename("contract_value_active")

    # Support cost proxy (forward 12 months)
    support_cost_map = {"P1": 2000, "P2": 500, "P3": 150, "P4": 50}
    forward_tickets = tickets[
        (tickets["created_date"] >= cutoff_date)
        & (tickets["created_date"] < forward_end)
    ].copy()
    forward_tickets["cost_proxy"] = forward_tickets["severity"].map(support_cost_map)
    support_cost = forward_tickets.groupby("account_id")["cost_proxy"].sum().rename("support_cost_12m")

    # Assemble
    clv = accounts[["account_id"]].set_index("account_id")
    clv = clv.join(booking_revenue).join(contract_revenue).join(support_cost).fillna(0)
    clv["clv_12m"] = clv["booking_revenue_12m"] + clv["contract_value_active"] - clv["support_cost_12m"]
    clv["clv_12m"] = clv["clv_12m"].clip(lower=0)  # floor at 0

    logger.info(
        "  → CLV stats: mean=$%.0f, median=$%.0f, max=$%.0f, zero_pct=%.1f%%",
        clv["clv_12m"].mean(),
        clv["clv_12m"].median(),
        clv["clv_12m"].max(),
        (clv["clv_12m"] == 0).mean() * 100,
    )

    return clv.reset_index()


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic corporate travel data")
    parser.add_argument(
        "--output-dir", type=str, default="data/synthetic", help="Output directory for CSV/Parquet files"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--format", choices=["csv", "parquet", "both"], default="both", help="Output format")
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("Synthetic Corporate Travel Data Generator")
    logger.info("=" * 60)
    logger.info("Target: %d accounts, ~%d bookings", NUM_ACCOUNTS, TARGET_BOOKINGS)
    logger.info("Output: %s (%s format)", output_dir, args.format)
    logger.info("Seed: %d", args.seed)
    logger.info("=" * 60)

    # Generate entities
    accounts = generate_accounts(rng)
    contracts = generate_service_contracts(accounts, rng)
    travelers = generate_travelers(accounts, rng)
    bookings = generate_bookings(travelers, accounts, rng)
    tickets = generate_support_tickets(accounts, rng)

    # Compute CLV labels
    clv_labels = compute_clv_labels(accounts, bookings, travelers, contracts, tickets)

    # Save outputs
    datasets = {
        "corporate_accounts": accounts,
        "service_contracts": contracts,
        "traveler_profiles": travelers,
        "bookings": bookings,
        "support_tickets": tickets,
        "clv_labels": clv_labels,
    }

    for name, df in datasets.items():
        if args.format in ("csv", "both"):
            csv_path = output_dir / f"{name}.csv"
            df.to_csv(csv_path, index=False)
            logger.info("Saved %s → %s (%d rows)", name, csv_path, len(df))

        if args.format in ("parquet", "both"):
            parquet_path = output_dir / f"{name}.parquet"
            df.to_parquet(parquet_path, index=False, engine="pyarrow")
            logger.info("Saved %s → %s (%d rows)", name, parquet_path, len(df))

    # Summary statistics
    logger.info("=" * 60)
    logger.info("GENERATION COMPLETE — SUMMARY")
    logger.info("=" * 60)
    for name, df in datasets.items():
        logger.info("  %-25s %10d rows  |  %3d columns", name, len(df), len(df.columns))
    total_rows = sum(len(df) for df in datasets.values())
    logger.info("  %-25s %10d rows total", "TOTAL", total_rows)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
