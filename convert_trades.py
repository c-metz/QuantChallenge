"""
Convert 2021 Trading Strategy Results using Strategy 3: PnL-Weighted XGBoost with
Confidence-Based Position Sizing (matching task2_analysis.ipynb exactly).

Strategy 3 approach:
- Daily retraining with 11:00 cutoff
- PnL-weighted sample weights (spread_magnitude × time_decay)
- Confidence-based position sizing (scales with probability)
- Thresholds: 0.90 (long) / 0.10 (short)
- Model: XGBClassifier(n_estimators=200, max_depth=9)
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

DB_PATH = Path(__file__).resolve().parent / "trades.sqlite"
DATA_PATH = Path(__file__).resolve().parent / "analysis_task_data.xlsx"

def init_database():
    """Initialize the trades database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS trades")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id TEXT PRIMARY KEY,
            trader_id TEXT NOT NULL,
            delivery_day TEXT NOT NULL,
            delivery_hour INTEGER NOT NULL,
            quantity REAL NOT NULL,
            price REAL NOT NULL,
            side TEXT NOT NULL,
            strategy TEXT,
            timestamp TEXT NOT NULL,
            CHECK (side IN ('buy', 'sell')),
            CHECK (delivery_hour >= 0 AND delivery_hour <= 23)
        )
    """)
    
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_delivery_day ON trades(delivery_day)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trader_id ON trades(trader_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trader_delivery ON trades(trader_id, delivery_day)")
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {DB_PATH}")

def load_and_prepare_data():
    """Load the 2021 energy market data"""
    print("\nLoading 2021 energy market data...")
    
    # Load data from the correct sheet
    df = pd.read_excel(DATA_PATH, sheet_name="DE_Wind_PV_Prices")
    
    # Rename columns to match our code
    df.columns = ['hour_label', 'timestamp', 'wind_da', 'wind_id', 'pv_da', 'pv_id', 
                  'da_price', 'id_price_qh', 'id_price_h', 'imbalance_price_qh']
    
    # Parse timestamps and extract date/hour
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['timestamp'].dt.date
    df['hour'] = df['timestamp'].dt.hour
    df['minute'] = df['timestamp'].dt.minute
    
    # Calculate spread (ID hourly - DA hourly)
    df['spread'] = df['id_price_h'] - df['da_price']
    
    print(f"Loaded {len(df)} data points from 2021")
    return df

def create_strategy_features(df):
    """
    Create base features for ML strategy.
    
    NOTE: Lagged features, rolling statistics, and hourly stats are NOT computed here
    to avoid data leakage. They are computed inside the daily loop with proper 11:00 cutoff,
    matching the notebook approach exactly.
    """
    print("\nPreparing base features for strategy...")
    
    df_ml = df.copy()
    
    # Temporal features (no lookahead - these are known in advance)
    df_ml['day_of_week'] = df_ml['timestamp'].dt.dayofweek
    df_ml['is_weekend'] = (df_ml['day_of_week'] >= 5).astype(int)
    df_ml['month'] = df_ml['timestamp'].dt.month
    df_ml['day_of_year'] = df_ml['timestamp'].dt.dayofyear
    
    # Renewable generation (DA forecasts are known in advance)
    df_ml['total_renewable_da'] = df_ml['wind_da'] + df_ml['pv_da']
    
    # NOTE: Lagged features, rolling statistics, and hourly stats are computed in the
    # daily loop with proper 11:00 cutoff to avoid lookahead bias (matching notebook)
    
    print(f"Prepared {len(df_ml)} records with base features")
    print("  (Lagged, rolling stats, hourly stats computed in daily loop with 11:00 cutoff)")
    return df_ml

def apply_daily_ml_strategy(df_ml):
    """
    Apply Strategy 3: PnL-Weighted XGBoost with Confidence-Based Position Sizing.
    
    Matches task2_analysis.ipynb Strategy 3 exactly:
    - Training cutoff: Day D at 11:00 (included)
    - Prediction target: All hours of Day D+1
    - Sample weights: spread_magnitude × time_decay (PnL-weighted)
    - Position sizing: Scales linearly with confidence (0 to max_position_mw)
    - Thresholds: 0.90 (long) / 0.10 (short)
    """
    from xgboost import XGBClassifier
    
    print("\n" + "="*80)
    print("STRATEGY 3: PNL-WEIGHTED XGBOOST + CONFIDENCE-BASED POSITION SIZING")
    print("="*80)
    
    # === FEATURE DEFINITIONS (matching notebook exactly) ===
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
    print(f"Total features: {len(all_features)}")
    print(f"  - Future-known features (DA forecasts + temporal): {len(future_known_features)}")
    print(f"  - Historical features (computed from past data): {len(historical_features)}")
    
    # === POSITION SIZING PARAMETERS (matching notebook) ===
    max_position_mw = 100  # Maximum position size in MW
    long_threshold = 0.90  # Probability threshold for long position
    short_threshold = 0.10 # Probability threshold for short position
    
    print(f"\nPosition sizing parameters:")
    print(f"  - Max position: {max_position_mw} MW")
    print(f"  - Long threshold: {long_threshold} (prob > {long_threshold})")
    print(f"  - Short threshold: {short_threshold} (prob < {short_threshold})")
    print(f"  - Position scales linearly with confidence from threshold to extreme")
    
    trades_list = []
    unique_days = sorted(df_ml['date'].unique())
    min_train_days = 30
    
    print(f"\nDate range: {unique_days[0]} to {unique_days[-1]}")
    print(f"Total days: {len(unique_days)}")
    print(f"Starting daily retraining loop (min {min_train_days} days for training)...")
    print("-" * 80)
    
    for day_idx in range(min_train_days, len(unique_days) - 1):
        current_day = unique_days[day_idx]   # Day D
        next_day = unique_days[day_idx + 1]  # Day D+1 (prediction target)
        
        # === STEP 1: Get training data up to Day D at 11:00 (included) ===
        cutoff_time = pd.Timestamp(current_day) + pd.Timedelta(hours=11)
        train_mask = df_ml['timestamp'] <= cutoff_time
        train_data = df_ml[train_mask].copy()
        
        if len(train_data) < 100:
            continue
        
        # === STEP 2: Compute lagged features on training data (matching notebook) ===
        id_price_col = 'id_price_h'
        for lag in [1, 2, 7]:
            lag_periods = lag * 96  # 96 quarter-hours per day
            train_data[f'da_price_lag_{lag}d'] = train_data['da_price'].shift(lag_periods)
            train_data[f'id_price_lag_{lag}d'] = train_data[id_price_col].shift(lag_periods)
            train_data[f'spread_lag_{lag}d'] = train_data['spread'].shift(lag_periods)
            train_data[f'renewable_lag_{lag}d'] = train_data['total_renewable_da'].shift(lag_periods)
        
        # === STEP 3: Compute rolling statistics from training data only (11:00 cutoff) ===
        window = 96  # 24 hours of quarter-hourly data
        train_data['da_price_mean_24h'] = train_data['da_price'].rolling(window=window, min_periods=1).mean()
        train_data['da_price_std_24h'] = train_data['da_price'].rolling(window=window, min_periods=1).std()
        train_data['id_price_mean_24h'] = train_data[id_price_col].rolling(window=window, min_periods=1).mean()
        train_data['spread_mean_24h'] = train_data['spread'].rolling(window=window, min_periods=1).mean()
        train_data['renewable_mean_24h'] = train_data['total_renewable_da'].rolling(window=window, min_periods=1).mean()
        
        # === STEP 4: Compute hour-of-day statistics from training data only ===
        hourly_stats_train = train_data.groupby('hour').agg({
            'da_price': 'mean',
            'spread': 'mean'
        }).reset_index()
        hourly_stats_train.columns = ['hour', 'da_price_hour_mean', 'spread_hour_mean']
        
        # Merge hourly stats into training data
        train_data = train_data.drop(columns=['da_price_hour_mean', 'spread_hour_mean'], errors='ignore')
        train_data = train_data.merge(hourly_stats_train, on='hour', how='left')
        
        # Drop NaN rows for training
        train_clean = train_data.dropna(subset=all_features)
        
        if len(train_clean) < 100:
            continue
        
        # === STEP 5: Prepare training data with PnL-weighted samples ===
        X_train = train_clean[all_features]
        y_train = (train_clean['spread'] > 0).astype(int)
        
        # PNL-WEIGHTED SAMPLE WEIGHTS (matching notebook)
        # Weight each sample by the absolute spread magnitude
        spread_weights = np.abs(train_clean['spread'].values)
        spread_weights = spread_weights / (spread_weights.mean() + 1e-10)
        
        # Time weights: favor recent data
        n_samples = len(train_clean)
        decay = 0.01
        time_weights = np.exp(decay * np.arange(n_samples))
        time_weights = 0.5 + 0.5 * (time_weights - time_weights.min()) / (time_weights.max() - time_weights.min() + 1e-10)
        
        # Combine: PnL impact × time recency
        combined_weights = spread_weights * time_weights
        combined_weights = combined_weights / (combined_weights.mean() + 1e-10)
        
        # === STEP 6: Train model with PnL-weighted samples ===
        model = XGBClassifier(
            n_estimators=200,   # Matching notebook
            max_depth=9,        # Matching notebook
            learning_rate=0.1,
            random_state=42,
            eval_metric='logloss',
            verbosity=0
        )
        model.fit(X_train, y_train, sample_weight=combined_weights)
        
        # === STEP 7: Prepare test data for Day D+1 (matching notebook exactly) ===
        test_data = df_ml[df_ml['date'] == next_day].copy()
        
        if len(test_data) == 0:
            continue
        
        # Create combined dataset for lag computation (matching notebook)
        combined = pd.concat([train_data, test_data], ignore_index=True).sort_values('timestamp')
        
        # Recompute lagged features on combined data (matching notebook)
        for lag in [1, 2, 7]:
            lag_periods = lag * 96
            combined[f'da_price_lag_{lag}d'] = combined['da_price'].shift(lag_periods)
            combined[f'id_price_lag_{lag}d'] = combined[id_price_col].shift(lag_periods)
            combined[f'spread_lag_{lag}d'] = combined['spread'].shift(lag_periods)
            combined[f'renewable_lag_{lag}d'] = combined['total_renewable_da'].shift(lag_periods)
        
        # Get last known rolling values from training data
        last_rolling_values = train_data.iloc[-1][['da_price_mean_24h', 'da_price_std_24h',
                                                    'id_price_mean_24h', 'spread_mean_24h',
                                                    'renewable_mean_24h']]
        
        # Extract D+1 rows from combined
        test_combined = combined[combined['date'] == next_day].copy()
        
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
        
        # === STEP 8: Make predictions with CONFIDENCE-BASED POSITION SIZING ===
        X_test = test_combined[all_features]
        probs = model.predict_proba(X_test)[:, 1]
        
        # Initialize arrays
        signals = np.zeros(len(probs))
        position_sizes = np.zeros(len(probs))
        
        # Long positions: scale from 0 MW at threshold to max MW at probability = 1.0
        long_mask = probs > long_threshold
        if long_mask.any():
            long_confidence = (probs[long_mask] - long_threshold) / (1.0 - long_threshold)
            signals[long_mask] = 1
            position_sizes[long_mask] = long_confidence * max_position_mw
        
        # Short positions: scale from 0 MW at threshold to max MW at probability = 0.0
        short_mask = probs < short_threshold
        if short_mask.any():
            short_confidence = (short_threshold - probs[short_mask]) / short_threshold
            signals[short_mask] = -1
            position_sizes[short_mask] = short_confidence * max_position_mw
        
        test_combined['signal'] = signals
        test_combined['prob'] = probs
        test_combined['position_mw'] = position_sizes
        
        # Calculate PnL: signal * spread * position_size (variable per prediction)
        test_combined['pnl'] = test_combined['signal'] * test_combined['spread'] * test_combined['position_mw']
        
        trades_list.append(test_combined)
        
        # Progress update every 30 days
        if (day_idx - min_train_days) % 30 == 0:
            total_pnl = sum([d['pnl'].sum() for d in trades_list])
            print(f"Day {day_idx}/{len(unique_days)-1} ({current_day}): "
                  f"Predicting {next_day}, Total PnL = €{total_pnl:,.0f}")
    
    # === COMBINE ALL RESULTS ===
    results_df = pd.concat(trades_list, ignore_index=True)
    
    # Calculate metrics (matching notebook output)
    trades_df = results_df[results_df['signal'] != 0].copy()
    n_trades = len(trades_df)
    total_pnl = results_df['pnl'].sum()
    winning_trades = (trades_df['pnl'] > 0).sum() if n_trades > 0 else 0
    losing_trades = (trades_df['pnl'] < 0).sum() if n_trades > 0 else 0
    win_rate = winning_trades / n_trades if n_trades > 0 else 0
    avg_position = trades_df['position_mw'].mean() if n_trades > 0 else 0
    
    print("\n" + "="*80)
    print("STRATEGY 3: CONFIDENCE-BASED POSITION SIZING RESULTS")
    print("="*80)
    print(f"Total P&L: €{total_pnl:,.2f}")
    print(f"Number of trades: {n_trades:,}")
    print(f"Winning trades: {winning_trades:,} ({win_rate*100:.1f}%)")
    print(f"Losing trades: {losing_trades:,}")
    print(f"Average profit per trade: €{total_pnl/n_trades if n_trades > 0 else 0:,.2f}")
    print(f"Average position size: {avg_position:.1f} MW (max: {max_position_mw} MW)")
    print(f"Market coverage: {n_trades/(len(results_df))*100:.1f}%")
    print("="*80)
    
    return results_df

def convert_to_trades(df_results, strategy_name="Strategy_3_PnL_Confidence"):
    """
    Convert trading signals to actual trade records.
    Uses variable position sizes from confidence-based sizing (Strategy 3).
    """
    print("\nConverting signals to trade records...")
    
    trades = []
    
    # Filter only actual trades (signal != 0)
    df_trades = df_results[df_results['signal'] != 0].copy()
    
    for idx, row in df_trades.iterrows():
        delivery_day = row['date']
        delivery_hour = int(row['hour'])
        timestamp = row['timestamp']
        signal = row['signal']
        da_price = row['da_price']
        id_price = row['id_price_h']
        # Use variable position size from confidence-based sizing
        position_size = row['position_mw']
        
        # Trading decision made day before at 11:00
        trade_timestamp = timestamp - timedelta(days=1)
        
        if signal == 1:  # LONG position (buy DA, sell ID)
            # Trade 1: BUY at Day-Ahead
            trades.append({
                'trade_id': f"S3_DA_{trade_timestamp.strftime('%Y%m%d_%H%M%S')}_{idx}_BUY",
                'trader_id': 'strategy_3_pnl_confidence',
                'delivery_day': delivery_day.isoformat(),
                'delivery_hour': delivery_hour,
                'quantity': position_size,
                'price': da_price,
                'side': 'buy',
                'strategy': strategy_name,
                'timestamp': trade_timestamp.isoformat()
            })
            
            # Trade 2: SELL at Intraday
            trades.append({
                'trade_id': f"S3_ID_{trade_timestamp.strftime('%Y%m%d_%H%M%S')}_{idx}_SELL",
                'trader_id': 'strategy_3_pnl_confidence',
                'delivery_day': delivery_day.isoformat(),
                'delivery_hour': delivery_hour,
                'quantity': position_size,
                'price': id_price,
                'side': 'sell',
                'strategy': strategy_name,
                'timestamp': (trade_timestamp + timedelta(hours=2)).isoformat()  # ID trade 2h later
            })
            
        elif signal == -1:  # SHORT position (sell DA, buy ID)
            # Trade 1: SELL at Day-Ahead
            trades.append({
                'trade_id': f"S3_DA_{trade_timestamp.strftime('%Y%m%d_%H%M%S')}_{idx}_SELL",
                'trader_id': 'strategy_3_pnl_confidence',
                'delivery_day': delivery_day.isoformat(),
                'delivery_hour': delivery_hour,
                'quantity': position_size,
                'price': da_price,
                'side': 'sell',
                'strategy': strategy_name,
                'timestamp': trade_timestamp.isoformat()
            })
            
            # Trade 2: BUY at Intraday
            trades.append({
                'trade_id': f"S3_ID_{trade_timestamp.strftime('%Y%m%d_%H%M%S')}_{idx}_BUY",
                'trader_id': 'strategy_3_pnl_confidence',
                'delivery_day': delivery_day.isoformat(),
                'delivery_hour': delivery_hour,
                'quantity': position_size,
                'price': id_price,
                'side': 'buy',
                'strategy': strategy_name,
                'timestamp': (trade_timestamp + timedelta(hours=2)).isoformat()
            })
    
    print(f"Created {len(trades)} trade records ({len(trades)//2} positions)")
    return trades

def insert_trades_to_db(trades):
    """Insert trades into database"""
    print("\nInserting trades into database...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    for trade in trades:
        cursor.execute("""
            INSERT INTO trades (trade_id, trader_id, delivery_day, delivery_hour,
                              quantity, price, side, strategy, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade['trade_id'],
            trade['trader_id'],
            trade['delivery_day'],
            trade['delivery_hour'],
            trade['quantity'],
            trade['price'],
            trade['side'],
            trade['strategy'],
            trade['timestamp']
        ))
    
    conn.commit()
    
    # Get statistics
    cursor.execute("SELECT COUNT(*) FROM trades")
    total_trades = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN side='buy' THEN quantity ELSE 0 END) as total_buy,
            SUM(CASE WHEN side='sell' THEN quantity ELSE 0 END) as total_sell,
            SUM(CASE WHEN side='sell' THEN quantity*price ELSE -quantity*price END) as total_pnl
        FROM trades
    """)
    stats = cursor.fetchone()
    
    conn.close()
    
    print(f"  Inserted {total_trades} trades")
    print(f"  Total BUY volume: {stats[0]:,.2f} MW")
    print(f"  Total SELL volume: {stats[1]:,.2f} MW")
    print(f"  Total PnL: €{stats[2]:,.2f}")
    
    return total_trades

def main():
    """Main execution"""
    print("="*80)
    print("REAL 2021 TRADING DATA CONVERTER")
    print("Converting Strategy 3 (PnL-Weighted XGBoost + Confidence Sizing) to database")
    print("Matching task2_analysis.ipynb Strategy 3 exactly")
    print("="*80)
    
    # Initialize database
    init_database()
    
    # Load real data
    df = load_and_prepare_data()
    
    # Prepare features
    df_ml = create_strategy_features(df)
    
    # Apply strategy
    df_results = apply_daily_ml_strategy(df_ml)
    
    # Convert to trades
    trades = convert_to_trades(df_results)
    
    # Insert to database
    total_trades = insert_trades_to_db(trades)
    
    print("\n" + "="*80)
    print("CONVERSION COMPLETE!")
    print("="*80)
    print(f"\n Database: {DB_PATH}")
    print(f" Total Trades: {total_trades}")
    print(f" Trader ID: strategy_3_pnl_confidence")
    print(f" Strategy: Strategy_3_PnL_Confidence")
    print(f" Period: 2021 (real data)")
    print(f" Approach: PnL-Weighted XGBoost + Confidence-Based Position Sizing")
    print(f" Thresholds: 0.90 (long) / 0.10 (short)")
    print("\n You can now:")
    print("  1. Run API: python task1_api.py")
    print("  2. View reports: python task3_report.py strategy_3_pnl_confidence 2021-06-15")
    print("  3. Launch dashboard: streamlit run task4_dashboard.py")
    print("="*80)

if __name__ == "__main__":
    main()
