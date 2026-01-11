
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


def run_ml_strategy(
    df: pd.DataFrame,
    position_mw: float = 100.0,
    min_train_days: int = 30,
    long_threshold: float = 0.55,
    short_threshold: float = 0.45,
):
    """Daily-retrained XGBoost strategy with 11:00 cutoff for next-day trading.
    
    Each day D at 11:00, train model on data up to D 11:00 (included).
    Then predict for ALL hours of day D+1 (next day).
    
    Only future info available for D+1:
    - Temporal features: hour, day_of_week, is_weekend, month, day_of_year
    - DA renewable forecasts: wind_da, pv_da, total_renewable_da
    
    Historical features are computed from data up to D 11:00 only.
    
    Returns (summary dict, results DataFrame, daily PnL Series).
    """
    from xgboost import XGBClassifier
    
    # Prepare dataframe
    df_ml = df.copy()
    df_ml['timestamp'] = pd.to_datetime(df_ml['timestamp'])
    df_ml['hour'] = df_ml['timestamp'].dt.hour
    df_ml['minute'] = df_ml['timestamp'].dt.minute
    
    # Price spread target
    id_price_col = 'id_price_h'
    df_ml['price_spread'] = df_ml[id_price_col] - df_ml['da_price']
    
    # Features that are KNOWN for next day (D+1) when making decision on day D at 11:00
    future_known_features = [
        'hour', 'day_of_week', 'is_weekend', 'month', 'day_of_year',
        'total_renewable_da', 'wind_da', 'pv_da'
    ]
    
    # Features computed from HISTORICAL data only (up to D 11:00)
    historical_features = [
        'da_price_lag_1d', 'id_price_lag_1d', 'spread_lag_1d', 'renewable_lag_1d',
        'da_price_lag_2d', 'id_price_lag_2d', 'spread_lag_2d', 'renewable_lag_2d',
        'da_price_lag_7d', 'id_price_lag_7d', 'spread_lag_7d', 'renewable_lag_7d',
        'da_price_mean_24h', 'da_price_std_24h', 'id_price_mean_24h',
        'spread_mean_24h', 'renewable_mean_24h',
        'da_price_hour_mean', 'spread_hour_mean'
    ]
    
    all_features = future_known_features + historical_features
    
    # Get unique dates
    unique_dates = sorted(df_ml['date'].unique())
    
    # Initialize results storage
    all_predictions = []
    
    for day_idx in range(min_train_days, len(unique_dates) - 1):
        current_date = unique_dates[day_idx]  # Day D
        next_date = unique_dates[day_idx + 1]  # Day D+1 (prediction target)
        
        # === STEP 1: Get training data up to Day D at 11:00 (included) ===
        cutoff_time = pd.Timestamp(current_date) + pd.Timedelta(hours=11)
        train_mask = df_ml['timestamp'] <= cutoff_time
        train_raw = df_ml[train_mask].copy()
        
        if len(train_raw) < 100:
            continue
        
        # === STEP 2: Compute features on training data ===
        # Temporal features
        train_raw['day_of_week'] = train_raw['timestamp'].dt.dayofweek
        train_raw['is_weekend'] = (train_raw['day_of_week'] >= 5).astype(int)
        train_raw['month'] = train_raw['timestamp'].dt.month
        train_raw['day_of_year'] = train_raw['timestamp'].dt.dayofyear
        
        # Renewable features
        train_raw['total_renewable_da'] = train_raw['wind_da'] + train_raw['pv_da']
        
        # Lagged features (1, 2, 7 days)
        for lag in [1, 2, 7]:
            lag_periods = lag * 96  # 96 quarter-hours per day
            train_raw[f'da_price_lag_{lag}d'] = train_raw['da_price'].shift(lag_periods)
            train_raw[f'id_price_lag_{lag}d'] = train_raw[id_price_col].shift(lag_periods)
            train_raw[f'spread_lag_{lag}d'] = train_raw['price_spread'].shift(lag_periods)
            train_raw[f'renewable_lag_{lag}d'] = train_raw['total_renewable_da'].shift(lag_periods)
        
        # Rolling statistics (24h window)
        window = 96
        train_raw['da_price_mean_24h'] = train_raw['da_price'].rolling(window=window, min_periods=1).mean()
        train_raw['da_price_std_24h'] = train_raw['da_price'].rolling(window=window, min_periods=1).std()
        train_raw['id_price_mean_24h'] = train_raw[id_price_col].rolling(window=window, min_periods=1).mean()
        train_raw['spread_mean_24h'] = train_raw['price_spread'].rolling(window=window, min_periods=1).mean()
        train_raw['renewable_mean_24h'] = train_raw['total_renewable_da'].rolling(window=window, min_periods=1).mean()
        
        # Hour-of-day statistics (computed from historical data only)
        hourly_stats_train = train_raw.groupby('hour').agg({
            'da_price': 'mean',
            'price_spread': 'mean'
        }).reset_index()
        hourly_stats_train.columns = ['hour', 'da_price_hour_mean', 'spread_hour_mean']
        train_raw = train_raw.merge(hourly_stats_train, on='hour', how='left', suffixes=('', '_drop'))
        train_raw = train_raw.loc[:, ~train_raw.columns.str.endswith('_drop')]
        
        # Drop NaN rows
        train_clean = train_raw.dropna(subset=all_features)
        
        if len(train_clean) < 100:
            continue
        
        # === STEP 3: Prepare training data ===
        X_train = train_clean[all_features]
        y_train = (train_clean['price_spread'] > 0).astype(int)
        
        # Time weights: favor recent data
        n_samples = len(train_clean)
        decay = 0.01
        time_weights = np.exp(decay * np.arange(n_samples))
        time_weights = 0.5 + 0.5 * (time_weights - time_weights.min()) / (time_weights.max() - time_weights.min() + 1e-10)
        
        # === STEP 4: Train model ===
        model = XGBClassifier(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42,
            eval_metric='logloss'
        )
        model.fit(X_train, y_train, sample_weight=time_weights)
        
        # === STEP 5: Prepare test data for Day D+1 ===
        test_raw = df_ml[df_ml['date'] == next_date].copy()
        
        if len(test_raw) == 0:
            continue
        
        # For D+1, we only know: temporal features + DA renewable forecasts
        test_raw['day_of_week'] = test_raw['timestamp'].dt.dayofweek
        test_raw['is_weekend'] = (test_raw['day_of_week'] >= 5).astype(int)
        test_raw['month'] = test_raw['timestamp'].dt.month
        test_raw['day_of_year'] = test_raw['timestamp'].dt.dayofyear
        test_raw['total_renewable_da'] = test_raw['wind_da'] + test_raw['pv_da']
        
        # Create combined dataset for lag computation
        combined = pd.concat([train_raw, test_raw], ignore_index=True).sort_values('timestamp')
        
        # Recompute lagged features on combined data
        for lag in [1, 2, 7]:
            lag_periods = lag * 96
            combined[f'da_price_lag_{lag}d'] = combined['da_price'].shift(lag_periods)
            combined[f'id_price_lag_{lag}d'] = combined[id_price_col].shift(lag_periods)
            combined[f'spread_lag_{lag}d'] = combined['price_spread'].shift(lag_periods)
            combined[f'renewable_lag_{lag}d'] = combined['total_renewable_da'].shift(lag_periods)
        
        # Get last known rolling values from training data
        last_rolling_values = train_raw.iloc[-1][['da_price_mean_24h', 'da_price_std_24h', 
                                                   'id_price_mean_24h', 'spread_mean_24h', 
                                                   'renewable_mean_24h']]
        
        # Extract D+1 rows from combined
        test_combined = combined[combined['date'] == next_date].copy()
        
        # Fill rolling stats with last known values
        for col in ['da_price_mean_24h', 'da_price_std_24h', 'id_price_mean_24h', 
                    'spread_mean_24h', 'renewable_mean_24h']:
            test_combined[col] = last_rolling_values[col]
        
        # Hour-of-day stats from training data
        test_combined = test_combined.drop(columns=['da_price_hour_mean', 'spread_hour_mean'], errors='ignore')
        test_combined = test_combined.merge(hourly_stats_train, on='hour', how='left')
        
        # Fill missing features with training median
        for feat in all_features:
            if feat not in test_combined.columns:
                test_combined[feat] = X_train[feat].median()
            elif test_combined[feat].isna().any():
                test_combined[feat] = test_combined[feat].fillna(X_train[feat].median())
        
        # === STEP 6: Make predictions for D+1 ===
        X_test = test_combined[all_features]
        
        probs = model.predict_proba(X_test)[:, 1]
        
        # Trading signals
        test_combined['signal'] = 0
        test_combined.loc[probs > long_threshold, 'signal'] = 1
        test_combined.loc[probs < short_threshold, 'signal'] = -1
        test_combined['prob'] = probs
        
        # Calculate PnL
        test_combined['pnl'] = test_combined['signal'] * test_combined['price_spread'] * position_mw
        
        # Store results
        all_predictions.append(test_combined[['timestamp', 'date', 'hour', 'price_spread', 
                                               'signal', 'prob', 'pnl']])
    
    # === COMBINE ALL RESULTS ===
    if not all_predictions:
        return {"error": "No predictions generated"}, pd.DataFrame(), pd.Series(dtype=float)
    
    results_df = pd.concat(all_predictions, ignore_index=True)
    
    # Calculate metrics
    total_pnl = results_df['pnl'].sum()
    trades_df = results_df[results_df['signal'] != 0].copy()
    n_trades = len(trades_df)
    winning_trades = (trades_df['pnl'] > 0).sum() if n_trades > 0 else 0
    losing_trades = (trades_df['pnl'] < 0).sum() if n_trades > 0 else 0
    win_rate = winning_trades / n_trades if n_trades > 0 else 0
    
    daily_pnl = results_df.groupby('date')['pnl'].sum()
    
    summary = {
        "position_mw": position_mw,
        "min_train_days": min_train_days,
        "long_threshold": long_threshold,
        "short_threshold": short_threshold,
        "total_pnl_eur": float(total_pnl),
        "n_trades": n_trades,
        "winning_trades": int(winning_trades),
        "losing_trades": int(losing_trades),
        "win_rate": float(win_rate),
        "avg_pnl_per_trade": float(total_pnl / n_trades) if n_trades > 0 else 0.0,
        "market_coverage": float(n_trades / len(results_df)) if len(results_df) > 0 else 0.0,
        "daily_mean": float(daily_pnl.mean()),
        "daily_std": float(daily_pnl.std()),
        "max_day": float(daily_pnl.max()),
        "min_day": float(daily_pnl.min()),
        "max_drawdown": float((daily_pnl.cumsum() - daily_pnl.cumsum().cummax()).min()),
        "long_positions": int((results_df['signal'] == 1).sum()),
        "short_positions": int((results_df['signal'] == -1).sum()),
        "no_positions": int((results_df['signal'] == 0).sum()),
    }
    
    return summary, results_df, daily_pnl

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
    print("\nRunning ML strategy (this may take a while)...")
    ml_summary, _, _ = run_ml_strategy(df)
    print("ML strategy summary:", ml_summary)

if __name__ == "__main__":
    run_all()
