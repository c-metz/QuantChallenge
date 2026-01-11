from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
from matplotlib import pyplot as plt

import epex_data

ROOT = Path(__file__).resolve().parent
OPTIMIZER_DIR = ROOT.parent.parent / "bess-optimizer"
if str(OPTIMIZER_DIR) not in sys.path:
    sys.path.insert(0, str(OPTIMIZER_DIR))

from optimizer_ortools import optimizer

ENERGY_CAP_MWH = 2.0
POWER_CAP_MW = 1.0
N_CYCLES = 1.5
EXPECTED_QUARTERS = 96

SOLAR_PROFILE_MW = (
    np.array(
        [
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            5,
            15,
            30,
            55,
            110,
            180,
            260,
            350,
            440,
            530,
            610,
            710,
            745,
            780,
            810,
            840,
            865,
            885,
            900,
            900,
            900,
            900,
            900,
            895,
            880,
            865,
            845,
            820,
            790,
            755,
            715,
            670,
            580,
            480,
            385,
            270,
            220,
            185,
            150,
            110,
            80,
            55,
            30,
            10,
            2,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ],
        dtype=float,
    )
    * 0.001
)


def validate_vector(name, values, expected=EXPECTED_QUARTERS) -> None:
    if len(values) != expected:
        raise ValueError(f"{name} length {len(values)} does not match {expected}.")
    if any(val is None for val in values):
        raise ValueError(f"{name} contains missing values.")


def compute_cycles(charge, discharge, dt, energy_cap) -> float:
    total_charge = float(np.sum(np.asarray(charge) * dt))
    total_discharge = float(np.sum(np.asarray(discharge) * dt))
    return (total_charge + total_discharge) / (2 * energy_cap)


def run_sequential(daa, ida, idc, pv_vector, n_cycles, energy_cap, power_cap):
    opt = optimizer()

    soc1, cha1, dis1, cur1, pv_to_bess1, p1, pv_alloc1 = opt.step1_optimize_daa(
        n_cycles=n_cycles,
        energy_cap=energy_cap,
        power_cap=power_cap,
        daa_price_vector=daa,
        pv_vector=pv_vector,
    )

    (
        soc2,
        cha2,
        dis2,
        cha2c,
        dis2c,
        cur2,
        pv_to_bess2,
        p2,
        cha_tot2,
        dis_tot2,
        pv_alloc2,
    ) = opt.step2_optimize_ida(
        n_cycles=n_cycles,
        energy_cap=energy_cap,
        power_cap=power_cap,
        ida_price_vector=ida,
        pv_vector=pv_vector,
        step1_cha_daa=cha1,
        step1_dis_daa=dis1,
        step1_pv_allocation=pv_alloc1,
    )

    (
        soc3,
        cha3,
        dis3,
        cha3c,
        dis3c,
        cur3,
        pv_to_bess3,
        p3,
        cha_tot3,
        dis_tot3,
    ) = opt.step3_optimize_idc(
        n_cycles=n_cycles,
        energy_cap=energy_cap,
        power_cap=power_cap,
        idc_price_vector=idc,
        pv_vector=pv_vector,
        step2_cha_phys=cha_tot2,
        step2_dis_phys=dis_tot2,
        step1_pv_allocation=pv_alloc1,
        step2_pv_allocation=pv_alloc2,
    )

    cycles = compute_cycles(cha_tot3, dis_tot3, opt.DT, energy_cap)
    return {
        "profit_total": p1 + p2 + p3,
        "profit_by_market": {"daa": p1, "ida": p2, "idc": p3},
        "soc": soc3,
        "cha_tot": cha_tot3,
        "dis_tot": dis_tot3,
        "cycles": cycles,
    }


def run_full_stack(daa, ida, idc, pv_vector, n_cycles, energy_cap, power_cap):
    opt = optimizer()
    start = time.perf_counter()
    result = opt.optimize_full_stack_single_milp(
        n_cycles=n_cycles,
        energy_cap=energy_cap,
        power_cap=power_cap,
        daa_price_vector=daa,
        ida_price_vector=ida,
        idc_price_vector=idc,
        pv_vector=pv_vector,
    )
    result["solve_time_sec"] = time.perf_counter() - start
    result["cycles"] = compute_cycles(
        result["cha_phys"], result["dis_phys"], opt.DT, energy_cap
    )
    return result


def print_summary(delivery_date, bess_seq, bess_full) -> None:
    print("=" * 70)
    print(f"Delivery date: {delivery_date}")
    print("=" * 70)
    print("BESS-only sequential")
    print(f"  DAA:   {bess_seq['profit_by_market']['daa']:.2f} EUR")
    print(f"  IDA:   {bess_seq['profit_by_market']['ida']:.2f} EUR")
    print(f"  IDC:   {bess_seq['profit_by_market']['idc']:.2f} EUR")
    print(f"  Total: {bess_seq['profit_total']:.2f} EUR")
    print(f"  Cycles: {bess_seq['cycles']:.2f}")
    print()

    bess_improvement = bess_full["profit"] - bess_seq["profit_total"]
    bess_pct = (
        (bess_improvement / bess_seq["profit_total"] * 100.0)
        if bess_seq["profit_total"]
        else 0.0
    )
    print("BESS-only full-stack")
    print(f"  Total: {bess_full['profit']:.2f} EUR")
    print(f"  DAA:   {bess_full['profit_by_market']['daa']:.2f} EUR")
    print(f"  IDA:   {bess_full['profit_by_market']['ida']:.2f} EUR")
    print(f"  IDC:   {bess_full['profit_by_market']['idc']:.2f} EUR")
    print(f"  Cycles: {bess_full['cycles']:.2f}")
    print(f"  Improvement: {bess_improvement:.2f} EUR ({bess_pct:+.2f}%)")
    print(f"  Solve time: {bess_full['solve_time_sec']:.2f} s")
    print("=" * 70)


def _create_timestamp_axis(delivery_date: str, n_quarters: int):
    """Create datetime array for x-axis based on delivery date."""
    import pandas as pd
    start = pd.Timestamp(delivery_date)
    return pd.date_range(start=start, periods=n_quarters, freq="15min")


def plot_price_profiles(daa, ida, idc, delivery_date) -> None:
    timestamps = _create_timestamp_axis(delivery_date, len(daa))
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(timestamps, daa, label="DAA", color="black")
    ax.plot(timestamps, ida, label="IDA", color="teal")
    ax.plot(timestamps, idc, label="IDC", color="darkseagreen")
    ax.set_title(f"EPEX prices for {delivery_date}")
    ax.set_xlabel("Time")
    ax.set_ylabel("Price (EUR/MWh)")
    ax.grid(alpha=0.3)
    ax.legend(loc="best")
    fig.autofmt_xdate()
    plt.tight_layout()


def plot_profit_comparison(bess_seq, bess_full) -> None:
    labels = [
        "BESS sequential",
        "BESS full-stack",
    ]
    values = [
        bess_seq["profit_total"],
        bess_full["profit"],
    ]
    colors = ["steelblue", "mediumorchid"]

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(labels, values, color=colors, alpha=0.8, edgecolor="black")
    ax.set_ylabel("Total profit (EUR)")
    ax.set_title("BESS Optimization Strategy Comparison")
    ax.grid(axis="y", alpha=0.3)
    ax.set_ylim(min(0.0, min(values) * 1.1), max(values) * 1.15)

    for bar, val in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            bar.get_height(),
            f"{val:.2f}",
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold",
        )

    plt.tight_layout()


def plot_soc_comparison(bess_seq, bess_full, delivery_date) -> None:
    timestamps = _create_timestamp_axis(delivery_date, len(bess_seq["soc"]))
    fig, ax = plt.subplots(figsize=(12, 4))

    ax.plot(timestamps, bess_seq["soc"], label="Sequential", color="steelblue")
    ax.plot(timestamps, bess_full["soc"], label="Full-stack", color="mediumorchid")
    ax.set_title("BESS State of Charge")
    ax.set_xlabel("Time")
    ax.set_ylabel("SOC (MWh)")
    ax.grid(alpha=0.3)
    ax.legend(loc="best")
    fig.autofmt_xdate()
    plt.tight_layout()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute previous-day PnL for sequential vs full-stack optimizations."
    )
    parser.add_argument(
        "--delivery-date",
        help="Delivery date in YYYY-MM-DD (defaults to yesterday).",
    )
    parser.add_argument("--db-path", default=epex_data.DEFAULT_DB_PATH)
    parser.add_argument("--refresh", action="store_true", help="Force re-fetch.")
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Only read from the database, do not scrape.",
    )
    args = parser.parse_args()

    delivery_date, _, prices = epex_data.get_prices_for_delivery_date(
        delivery_date=args.delivery_date,
        db_path=args.db_path,
        refresh=args.refresh,
        allow_fetch=not args.no_fetch,
        expected_quarters=EXPECTED_QUARTERS,
    )

    daa = prices["daa"]
    ida = prices["ida"]
    idc = prices["idc"]

    validate_vector("DAA", daa)
    validate_vector("IDA", ida)
    validate_vector("IDC", idc)

    bess_pv = [0.0] * len(daa)

    bess_seq = run_sequential(
        daa, ida, idc, bess_pv, N_CYCLES, ENERGY_CAP_MWH, POWER_CAP_MW
    )
    bess_full = run_full_stack(
        daa, ida, idc, bess_pv, N_CYCLES, ENERGY_CAP_MWH, POWER_CAP_MW
    )

    print_summary(delivery_date, bess_seq, bess_full)

    plot_price_profiles(daa, ida, idc, delivery_date)
    plot_profit_comparison(bess_seq, bess_full)
    plot_soc_comparison(bess_seq, bess_full, delivery_date)
    plt.show()


if __name__ == "__main__":
    main()
