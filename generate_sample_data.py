"""
Sample Trade Data Generator
Generates realistic sample trades for testing the applications
"""

import sqlite3
import random
from datetime import datetime, date, timedelta
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "trades.sqlite"

def init_database():
    """Initialize the trades database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
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
    print(f"✓ Database initialized at {DB_PATH}")

def generate_sample_trades(num_days=30, traders=None):
    """Generate sample trades for testing"""
    
    if traders is None:
        traders = ["trader1", "trader2", "trader3"]
    
    strategies = ["Peak_Strategy", "Solar_Strategy", "ML_Strategy_Daily", "Arbitrage"]
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Clear existing data
    cursor.execute("DELETE FROM trades")
    
    trades_count = 0
    base_date = date.today() - timedelta(days=num_days)
    
    print(f"\nGenerating sample trades...")
    print(f"Date range: {base_date} to {date.today()}")
    print(f"Traders: {', '.join(traders)}")
    
    for day_offset in range(num_days):
        delivery_day = base_date + timedelta(days=day_offset)
        
        for trader_id in traders:
            # Generate 5-20 trades per day per trader
            num_trades_today = random.randint(5, 20)
            
            for _ in range(num_trades_today):
                # Random hour
                delivery_hour = random.randint(0, 23)
                
                # Higher activity during peak hours (7-9, 17-19)
                if random.random() < 0.3:
                    delivery_hour = random.choice([7, 8, 9, 17, 18, 19])
                
                # Random side with slight sell bias
                side = random.choices(['buy', 'sell'], weights=[0.45, 0.55])[0]
                
                # Quantity: 10-200 MW
                quantity = round(random.uniform(10, 200), 2)
                
                # Price: 20-150 EUR/MWh (higher during peaks)
                if delivery_hour in [7, 8, 9, 17, 18, 19]:
                    price = round(random.uniform(60, 150), 2)
                else:
                    price = round(random.uniform(20, 80), 2)
                
                # Strategy
                strategy = random.choice(strategies)
                
                # Timestamp (during the day before delivery)
                trade_time = datetime.combine(
                    delivery_day - timedelta(days=1),
                    datetime.min.time()
                ) + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )
                
                trade_id = f"{trader_id}_{trade_time.strftime('%Y%m%d_%H%M%S_%f')}"
                
                cursor.execute("""
                    INSERT INTO trades (trade_id, trader_id, delivery_day, delivery_hour,
                                      quantity, price, side, strategy, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trade_id,
                    trader_id,
                    delivery_day.isoformat(),
                    delivery_hour,
                    quantity,
                    price,
                    side,
                    strategy,
                    trade_time.isoformat()
                ))
                
                trades_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"✓ Generated {trades_count} sample trades")
    
    # Display summary
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("\nDatabase Summary:")
    for trader_id in traders:
        cursor.execute("""
            SELECT COUNT(*), 
                   SUM(CASE WHEN side='buy' THEN quantity ELSE 0 END),
                   SUM(CASE WHEN side='sell' THEN quantity ELSE 0 END),
                   SUM(CASE WHEN side='sell' THEN quantity*price ELSE -quantity*price END)
            FROM trades 
            WHERE trader_id = ?
        """, (trader_id,))
        
        count, buy_vol, sell_vol, pnl = cursor.fetchone()
        print(f"  {trader_id}: {count} trades, PnL: €{pnl:,.2f}")
    
    conn.close()

if __name__ == "__main__":
    print("="*60)
    print("SAMPLE TRADE DATA GENERATOR")
    print("="*60)
    
    init_database()
    generate_sample_trades(num_days=30, traders=["trader1", "trader2", "trader3"])
    
    print("\n" + "="*60)
    print("✓ Sample data generation complete!")
    print("="*60)
    print("\nYou can now:")
    print("  1. Run the API: python task1_api.py")
    print("  2. View reports: python task3_report.py trader1 2024-01-04")
    print("  3. Launch dashboard: streamlit run task4_dashboard.py")
    print("  4. Use Docker: docker-compose up")
    print("="*60)
