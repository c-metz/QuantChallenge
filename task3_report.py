"""
Task 3: PnL Report Generator (Terminal)
Computes and displays PnL for a trader on a given delivery day
"""

import sqlite3
import sys
from pathlib import Path
from datetime import date
from typing import Dict, List, Tuple
from tabulate import tabulate

DB_PATH = Path(__file__).resolve().parent / "trades.sqlite"

def compute_hourly_pnl(trader_id: str, delivery_day: date) -> Dict[int, dict]:
    """
    Compute PnL for each hour of a delivery day for a specific trader.
    
    Returns dict with hour as key and dict containing:
    - num_trades: Number of trades
    - buy_da_mw: BUY Day-Ahead volume
    - sell_da_mw: SELL Day-Ahead volume
    - buy_ida_mw: BUY Intraday volume
    - sell_ida_mw: SELL Intraday volume
    - pnl_eur: Profit/Loss in EUR
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Query trades for this trader and delivery day
    query = """
        SELECT delivery_hour, quantity, price, side, trade_id
        FROM trades
        WHERE trader_id = ? AND delivery_day = ?
        ORDER BY delivery_hour
    """
    
    cursor.execute(query, (trader_id, delivery_day.isoformat()))
    
    # Initialize results for all 24 hours
    hourly_data = {}
    for hour in range(24):
        hourly_data[hour] = {
            'num_trades': 0,
            'buy_da_mw': 0.0,
            'sell_da_mw': 0.0,
            'buy_ida_mw': 0.0,
            'sell_ida_mw': 0.0,
            'pnl_eur': 0.0
        }
    
    # Process trades
    for delivery_hour, quantity, price, side, trade_id in cursor.fetchall():
        hour_data = hourly_data[delivery_hour]
        hour_data['num_trades'] += 1
        
        # Determine if Day-Ahead or Intraday based on trade_id
        is_da = '_DA_' in trade_id
        is_ida = '_ID_' in trade_id
        
        if side == 'buy':
            if is_da:
                hour_data['buy_da_mw'] += quantity
            elif is_ida:
                hour_data['buy_ida_mw'] += quantity
            hour_data['pnl_eur'] -= quantity * price  # Cost
        else:  # sell
            if is_da:
                hour_data['sell_da_mw'] += quantity
            elif is_ida:
                hour_data['sell_ida_mw'] += quantity
            hour_data['pnl_eur'] += quantity * price  # Income
    
    conn.close()
    return hourly_data

def display_pnl_report(trader_id: str, delivery_day: date):
    """Generate and display PnL report in terminal"""
    
    try:
        hourly_data = compute_hourly_pnl(trader_id, delivery_day)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to fetch data: {e}")
        sys.exit(1)
    
    # Prepare table data
    table_data = []
    total_trades = 0
    total_buy_da = 0.0
    total_sell_da = 0.0
    total_buy_ida = 0.0
    total_sell_ida = 0.0
    total_pnl = 0.0
    
    for hour in range(24):
        data = hourly_data[hour]
        
        # Only show hours with trades
        if data['num_trades'] > 0:
            table_data.append([
                f"{hour:02d} - {hour+1:02d}",
                data['num_trades'],
                f"{data['buy_da_mw']:.2f}",
                f"{data['sell_da_mw']:.2f}",
                f"{data['buy_ida_mw']:.2f}",
                f"{data['sell_ida_mw']:.2f}",
                f"{data['pnl_eur']:+,.2f}"
            ])
            
            total_trades += data['num_trades']
            total_buy_da += data['buy_da_mw']
            total_sell_da += data['sell_da_mw']
            total_buy_ida += data['buy_ida_mw']
            total_sell_ida += data['sell_ida_mw']
            total_pnl += data['pnl_eur']
    
    # Print report
    print("\n" + "="*80)
    print(f"TRADING REPORT - {trader_id}")
    print(f"Delivery Day: {delivery_day}")
    print("="*80 + "\n")
    
    if not table_data:
        print("No trades found for this trader on this delivery day.")
        print("="*80 + "\n")
        return
    
    # Display table
    headers = ["Hour", "Trades", "BUY_DA [MW]", "SELL_DA [MW]", "BUY_IDA [MW]", "SELL_IDA [MW]", "PnL [EUR]"]
    print(tabulate(table_data, headers=headers, tablefmt="grid", stralign="right"))
    
    # Display totals
    print("\n" + "-"*80)
    total_row = [
        ["TOTAL", total_trades, f"{total_buy_da:.2f}", f"{total_sell_da:.2f}",
         f"{total_buy_ida:.2f}", f"{total_sell_ida:.2f}", f"{total_pnl:+,.2f}"]
    ]
    print(tabulate(total_row, headers=headers, tablefmt="grid", stralign="right"))
    print("-"*80)
    
    # Summary statistics
    print(f"\nSummary:")
    print(f"  • Total Trades: {total_trades}")
    print(f"  • Day-Ahead: {total_buy_da:.2f} MW bought, {total_sell_da:.2f} MW sold")
    print(f"  • Intraday: {total_buy_ida:.2f} MW bought, {total_sell_ida:.2f} MW sold")
    print(f"  • Net Position: {(total_sell_da + total_sell_ida) - (total_buy_da + total_buy_ida):+.2f} MW")
    print(f"  • Total PnL: €{total_pnl:+,.2f}")
    
    if total_pnl > 0:
        print(f"  • Result: ✓ PROFIT")
    elif total_pnl < 0:
        print(f"  • Result: ✗ LOSS")
    else:
        print(f"  • Result: ⊖ BREAK EVEN")
    
    print("="*80 + "\n")

def main():
    """Main entry point for CLI"""
    if len(sys.argv) < 3:
        print("Usage: python task3_report.py <trader_id> <delivery_day>")
        print("Example: python task3_report.py trader1 2023-02-20")
        sys.exit(1)
    
    trader_id = sys.argv[1]
    
    try:
        delivery_day = date.fromisoformat(sys.argv[2])
    except ValueError:
        print(f"ERROR: Invalid date format. Use YYYY-MM-DD")
        sys.exit(1)
    
    display_pnl_report(trader_id, delivery_day)

if __name__ == "__main__":
    main()
