"""
Convert 2021 Trading Strategy Results for 2021 data, Strategy 4 (Daily XGBoost), to trades in the database.
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
    """Create features for ML strategy (simplified version)"""
    print("\nPreparing features for strategy...")
    
    df_ml = df.copy()
    
    # Temporal features
    df_ml['day_of_week'] = df_ml['timestamp'].dt.dayofweek
    df_ml['is_weekend'] = (df_ml['day_of_week'] >= 5).astype(int)
    df_ml['month'] = df_ml['timestamp'].dt.month
    df_ml['day_of_year'] = df_ml['timestamp'].dt.dayofyear
    
    # Renewable generation
    df_ml['total_renewable_da'] = df_ml['wind_da'] + df_ml['pv_da']
    
    # Lagged features
    for lag in [1, 2, 7]:
        lag_hours = lag * 96
        df_ml[f'da_price_lag_{lag}d'] = df_ml['da_price'].shift(lag_hours)
        df_ml[f'id_price_lag_{lag}d'] = df_ml['id_price_h'].shift(lag_hours)
        df_ml[f'spread_lag_{lag}d'] = df_ml['spread'].shift(lag_hours)
        df_ml[f'renewable_lag_{lag}d'] = df_ml['total_renewable_da'].shift(lag_hours)
    
    # Rolling statistics
    window = 96
    df_ml['da_price_mean_24h'] = df_ml['da_price'].rolling(window=window, min_periods=1).mean()
    df_ml['da_price_std_24h'] = df_ml['da_price'].rolling(window=window, min_periods=1).std()
    df_ml['id_price_mean_24h'] = df_ml['id_price_h'].rolling(window=window, min_periods=1).mean()
    df_ml['spread_mean_24h'] = df_ml['spread'].rolling(window=window, min_periods=1).mean()
    df_ml['renewable_mean_24h'] = df_ml['total_renewable_da'].rolling(window=window, min_periods=1).mean()
    
    # Hour-of-day statistics
    hourly_stats = df_ml.groupby('hour').agg({
        'da_price': ['mean', 'std'],
        'id_price_h': ['mean', 'std'],
        'spread': 'mean'
    }).reset_index()
    hourly_stats.columns = ['hour', 'da_price_hour_mean', 'da_price_hour_std', 
                            'id_price_hour_mean', 'id_price_hour_std', 'spread_hour_mean']
    df_ml = df_ml.merge(hourly_stats, on='hour', how='left')
    
    # Drop NaN
    df_ml = df_ml.dropna()
    
    print(f"Prepared {len(df_ml)} records with features")
    return df_ml

def apply_daily_ml_strategy(df_ml):
    """Apply simplified daily ML strategy to generate trading signals"""
    from xgboost import XGBClassifier
    
    print("\nApplying Daily XGBoost ML Strategy...")
    
    feature_columns = [
        'hour', 'day_of_week', 'is_weekend', 'month', 'day_of_year',
        'total_renewable_da', 'wind_da', 'pv_da',
        'da_price_lag_1d', 'id_price_lag_1d', 'spread_lag_1d', 'renewable_lag_1d',
        'da_price_lag_2d', 'id_price_lag_2d', 'spread_lag_2d', 'renewable_lag_2d',
        'da_price_lag_7d', 'id_price_lag_7d', 'spread_lag_7d', 'renewable_lag_7d',
        'da_price_mean_24h', 'da_price_std_24h', 'id_price_mean_24h',
        'spread_mean_24h', 'renewable_mean_24h',
        'da_price_hour_mean', 'spread_hour_mean'
    ]
    
    trades_list = []
    unique_days = df_ml['date'].unique()
    min_train_days = 30   # i.e., 30 days of data minimum to start training
    
    for i, current_day in enumerate(unique_days[min_train_days:], min_train_days):
        # Training data
        train_data = df_ml[df_ml['date'] < current_day].copy()
        if len(train_data) < 100:
            continue
        
        X_train = train_data[feature_columns]
        y_train = (train_data['spread'] > 0).astype(int)
        
        # Time weights
        n_samples = len(train_data)
        decay = 0.01
        time_weights = np.exp(decay * np.arange(n_samples))
        time_weights = 0.5 + 0.5 * (time_weights - time_weights.min()) / (time_weights.max() - time_weights.min())
        
        # Train model
        model = XGBClassifier(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42,
            eval_metric='logloss',
            verbosity=0
        )
        model.fit(X_train, y_train, sample_weight=time_weights)
        
        # Predict for current day
        test_data = df_ml[df_ml['date'] == current_day].copy()
        if len(test_data) == 0:
            continue
        
        X_test = test_data[feature_columns]
        probs = model.predict_proba(X_test)[:, 1]
        
        # Generate signals
        test_data['signal'] = 0
        test_data.loc[probs > 0.55, 'signal'] = 1   # LONG
        test_data.loc[probs < 0.45, 'signal'] = -1  # SHORT
        test_data['prob'] = probs
        
        trades_list.append(test_data)
        
        if (i - min_train_days) % 50 == 0:
            print(f"  Processed day {i}/{len(unique_days)} ({current_day})")
    
    results_df = pd.concat(trades_list, ignore_index=True)
    print(f"Generated {len(results_df[results_df['signal'] != 0])} trading signals")
    
    return results_df

def convert_to_trades(df_results, strategy_name="ML_Daily_XGBoost"):
    """Convert trading signals to actual trade records"""
    print("\nConverting signals to trade records...")
    
    trades = []
    position_size = 100  # MW
    
    # Filter only actual trades (signal != 0)
    df_trades = df_results[df_results['signal'] != 0].copy()
    
    for idx, row in df_trades.iterrows():
        delivery_day = row['date']
        delivery_hour = int(row['hour'])
        timestamp = row['timestamp']
        signal = row['signal']
        da_price = row['da_price']
        id_price = row['id_price_h']
        
        # Trading decision made day before
        trade_timestamp = timestamp - timedelta(days=1)
        
        if signal == 1:  # LONG position (buy DA, sell ID)
            # Trade 1: BUY at Day-Ahead
            trades.append({
                'trade_id': f"ML_DA_{trade_timestamp.strftime('%Y%m%d_%H%M%S')}_{idx}_BUY",
                'trader_id': 'strategy_ml_daily',
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
                'trade_id': f"ML_ID_{trade_timestamp.strftime('%Y%m%d_%H%M%S')}_{idx}_SELL",
                'trader_id': 'strategy_ml_daily',
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
                'trade_id': f"ML_DA_{trade_timestamp.strftime('%Y%m%d_%H%M%S')}_{idx}_SELL",
                'trader_id': 'strategy_ml_daily',
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
                'trade_id': f"ML_ID_{trade_timestamp.strftime('%Y%m%d_%H%M%S')}_{idx}_BUY",
                'trader_id': 'strategy_ml_daily',
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
    print("Converting Strategy 4 (Daily XGBoost) trades to database")
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
    print(f" Trader ID: strategy_ml_daily")
    print(f" Strategy: ML_Daily_XGBoost")
    print(f" Period: 2021 (real data)")
    print("\n You can now:")
    print("  1. Run API: python task1_api.py")
    print("  2. View reports: python task3_report.py strategy_ml_daily 2021-06-15")
    print("  3. Launch dashboard: streamlit run task4_dashboard.py")
    print("="*80)

if __name__ == "__main__":
    main()
