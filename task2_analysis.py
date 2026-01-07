
"""FlexPower Quant Challenge - Task 2 analysis helpers.

Computes Task 2.1-2.7 metrics and provides a simple DA->ID strategy backtest.
"""
from __future__ import annotations
from pathlib import Path
from typing import Dict, Tuple
import pandas as pd
import numpy as np

DATA_FILE = Path(__file__).resolve().parent / "analysis_task_data.xlsx"

COL_MAP = {
    "Wind Day Ahead Forecast [in MW]": "wind_da",
    "Wind Intraday Forecast [in MW]": "wind_id",
    "PV Day Ahead Forecast [in MW]": "pv_da",
    "PV Intraday Forecast [in MW]": "pv_id",
    "Day Ahead Price hourly [in EUR/MWh]": "da_price",
    "Intraday Price Price Quarter Hourly  [in EUR/MWh]": "id_price_qh",
    "Intraday Price Hourly  [in EUR/MWh]": "id_price_h",
    "Imbalance Price Quarter Hourly  [in EUR/MWh]": "imb_price_qh",
}

def load_data(data_path: Path = DATA_FILE) -> pd.DataFrame:
    df = pd.read_excel(data_path, sheet_name="DE_Wind_PV_Prices")
    df = df.rename(columns=COL_MAP)
    df["timestamp"] = pd.to_datetime(df["time"], dayfirst=True)
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour
    df["minute"] = df["timestamp"].dt.minute
    for c in ("wind_da", "wind_id", "pv_da", "pv_id"):
        df[f"{c}_mwh"] = df[c] * 0.25
    return df

def task2_1_totals(df: pd.DataFrame) -> Dict[str, float]:
    return {
        "wind_da_mwh": float(df["wind_da_mwh"].sum()),
        "wind_id_mwh": float(df["wind_id_mwh"].sum()),
        "pv_da_mwh": float(df["pv_da_mwh"].sum()),
        "pv_id_mwh": float(df["pv_id_mwh"].sum()),
    }

def task2_2_profiles(df: pd.DataFrame) -> pd.DataFrame:
    profile = df.groupby(["hour", "minute"]).agg(
        wind_da=("wind_da", "mean"),
        wind_id=("wind_id", "mean"),
        pv_da=("pv_da", "mean"),
        pv_id=("pv_id", "mean"),
    ).reset_index()
    profile["slot_minutes"] = profile["hour"] * 60 + profile["minute"]
    return profile

def task2_3_values(df: pd.DataFrame) -> Dict[str, float]:
    hourly = df.groupby(["date", "hour"]).agg(
        wind_da=("wind_da", "mean"),
        pv_da=("pv_da", "mean"),
        da_price=("da_price", "mean"),
    ).reset_index()
    wind_value = (hourly["wind_da"] * hourly["da_price"]).sum() / hourly["wind_da"].sum()
    pv_value = (hourly["pv_da"] * hourly["da_price"]).sum() / hourly["pv_da"].sum()
    avg_price = float(hourly["da_price"].mean())
    return {
        "wind_value": float(wind_value),
        "pv_value": float(pv_value),
        "avg_da_price": avg_price,
    }

def task2_4_extremes(df: pd.DataFrame) -> Dict[str, Tuple]:
    daily_energy = df.groupby("date")[
        ["wind_da_mwh", "pv_da_mwh"]
    ].sum()
    daily_energy["total_mwh"] = daily_energy.sum(axis=1)
    price_by_day = df.groupby("date")["da_price"].mean()
    max_day = daily_energy["total_mwh"].idxmax()
    min_day = daily_energy["total_mwh"].idxmin()
    return {
        "max_day": (max_day, float(daily_energy.loc[max_day, "total_mwh"]), float(price_by_day.loc[max_day])),
        "min_day": (min_day, float(daily_energy.loc[min_day, "total_mwh"]), float(price_by_day.loc[min_day])),
    }

def task2_5_weekday_weekend(df: pd.DataFrame) -> Dict[str, float]:
    df = df.copy()
    df["weekday"] = df["timestamp"].dt.weekday
    is_weekend = df["weekday"] >= 5
    weekday_mean = float(df.loc[~is_weekend, "da_price"].mean())
    weekend_mean = float(df.loc[is_weekend, "da_price"].mean())
    hour_profile = df.groupby(["weekday", "hour"])["da_price"].mean().reset_index()
    return {
        "weekday_mean": weekday_mean,
        "weekend_mean": weekend_mean,
        "hour_profile": hour_profile,
    }

def task2_6_battery_revenue(df: pd.DataFrame, capacity_mwh: float = 1.0) -> Dict[str, float]:
    hourly_prices = df.groupby(["date", "hour"])["da_price"].mean().reset_index()
    revenues = []
    for _, group in hourly_prices.groupby("date"):
        prices = group.sort_values("hour")["da_price"].reset_index(drop=True)
        min_idx = prices.idxmin()
        max_after_min_idx = prices[min_idx:].idxmax()
        buy = prices.loc[min_idx]
        sell = prices.loc[max_after_min_idx]
        revenues.append((sell - buy) * capacity_mwh)
    total = float(sum(revenues))
    return {
        "total_revenue_eur": total,
        "avg_per_day_eur": total / len(revenues),
    }

def _prepare_hourly(df: pd.DataFrame) -> pd.DataFrame:
    hourly = df.groupby(["date", "hour"]).agg(
        wind_da=("wind_da", "mean"),
        wind_id=("wind_id", "mean"),
        pv_da=("pv_da", "mean"),
        pv_id=("pv_id", "mean"),
        da_price=("da_price", "mean"),
        id_price_h=("id_price_h", "mean"),
    ).reset_index()
    hourly["wind_delta"] = hourly["wind_id"] - hourly["wind_da"]
    hourly["pv_delta"] = hourly["pv_id"] - hourly["pv_da"]
    hourly["res_delta"] = hourly["wind_delta"] + hourly["pv_delta"]
    hourly["price_diff"] = hourly["id_price_h"] - hourly["da_price"]
    return hourly

def run_strategy(
    df: pd.DataFrame,
    position_mw: float = 100.0,
    threshold: float = 0.0,
    wind_weight: float = 1.0,
    pv_weight: float = 1.0,
):
    """Directional DA->ID strategy based on forecast revisions.

    If weighted delta > threshold => short DA (position -1), otherwise long DA (position +1) when below -threshold.
    PnL per hour = position * (ID_price - DA_price) * position_mw.
    Returns (summary dict, hourly DataFrame, daily PnL Series).
    """
    hourly = _prepare_hourly(df)
    signal = wind_weight * hourly["wind_delta"] + pv_weight * hourly["pv_delta"]
    hourly["position"] = signal.apply(lambda x: -1 if x > threshold else (1 if x < -threshold else 0))
    hourly["pnl"] = hourly["position"] * hourly["price_diff"] * position_mw
    daily_pnl = hourly.groupby("date")["pnl"].sum()
    corr = float(hourly[["price_diff", "res_delta"]].corr().loc["price_diff", "res_delta"])
    summary = {
        "position_mw": position_mw,
        "threshold_mw": threshold,
        "wind_weight": wind_weight,
        "pv_weight": pv_weight,
        "total_pnl_eur": float(hourly["pnl"].sum()),
        "positive_hour_share": float((hourly["pnl"] > 0).mean()),
        "daily_mean": float(daily_pnl.mean()),
        "daily_std": float(daily_pnl.std()),
        "max_day": float(daily_pnl.max()),
        "min_day": float(daily_pnl.min()),
        "max_drawdown": float((daily_pnl.cumsum() - daily_pnl.cumsum().cummax()).min()),
        "corr_price_resdelta": corr,
    }
    return summary, hourly, daily_pnl

def run_all() -> None:
    df = load_data()
    print("Task 2.1 totals (MWh):", task2_1_totals(df))
    print("Task 2.3 values (EUR/MWh):", task2_3_values(df))
    print("Task 2.4 extremes:", task2_4_extremes(df))
    ww = task2_5_weekday_weekend(df)
    print("Task 2.5 weekday vs weekend:", {"weekday_mean": ww["weekday_mean"], "weekend_mean": ww["weekend_mean"]})
    print("Task 2.6 battery revenue (1 MWh/day):", task2_6_battery_revenue(df))
    summary, _, _ = run_strategy(df)
    print("Task 2.7 strategy summary:", summary)

if __name__ == "__main__":
    run_all()
