
"""FlexPower Quant Challenge - Task 1 utilities and API.

Functions:
- compute_total_buy_volume
- compute_total_sell_volume
- compute_pnl
- FastAPI app exposing /v1/pnl/{strategy_id}
"""

import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional

DB_PATH = Path(__file__).resolve().parent / "trades.sqlite"
TABLE_NAME = "epex_12_20_12_13"

def _connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Create a sqlite connection, asserting the DB exists."""
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at {db_path}")
    return sqlite3.connect(db_path)

def compute_total_buy_volume(conn: Optional[sqlite3.Connection] = None) -> float:
    """Total buy volume (MW) across all trades."""
    close_conn = False
    if conn is None:
        conn = _connect()
        close_conn = True
    cur = conn.execute(
        f"select coalesce(sum(quantity), 0) from {TABLE_NAME} where side='buy'"
    )
    value = cur.fetchone()[0] or 0
    if close_conn:
        conn.close()
    return float(value)

def compute_total_sell_volume(conn: Optional[sqlite3.Connection] = None) -> float:
    """Total sell volume (MW) across all trades."""
    close_conn = False
    if conn is None:
        conn = _connect()
        close_conn = True
    cur = conn.execute(
        f"select coalesce(sum(quantity), 0) from {TABLE_NAME} where side='sell'"
    )
    value = cur.fetchone()[0] or 0
    if close_conn:
        conn.close()
    return float(value)

def compute_pnl(strategy_id: str, conn: Optional[sqlite3.Connection] = None) -> float:
    """PnL in EUR for a strategy: sells = +q*p, buys = -q*p."""
    close_conn = False
    if conn is None:
        conn = _connect()
        close_conn = True
    cur = conn.execute(
        f"select quantity, price, side from {TABLE_NAME} where strategy=?",
        (strategy_id,),
    )
    total = 0.0
    for qty, price, side in cur.fetchall():
        total += qty * price if side == "sell" else -qty * price
    if close_conn:
        conn.close()
    return float(total)

def create_app(db_path: Path = DB_PATH):
    """FastAPI app exposing /v1/pnl/{strategy_id}."""
    from fastapi import FastAPI, HTTPException

    app = FastAPI(title="Energy Trading API", version="1.0.0")

    @app.get("/v1/pnl/{strategy_id}")
    def get_pnl(strategy_id: str):
        try:
            with sqlite3.connect(db_path) as conn:
                value = compute_pnl(strategy_id, conn)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=500, detail=str(exc))
        return {
            "strategy": strategy_id,
            "value": value,
            "unit": "euro",
            "capture_time": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        }

    return app

# ASGI entrypoint
app = create_app()

if __name__ == "__main__":
    with sqlite3.connect(DB_PATH) as conn:
        print("Total buy volume (MW):", compute_total_buy_volume(conn))
        print("Total sell volume (MW):", compute_total_sell_volume(conn))
        strategies = [row[0] for row in conn.execute(
            f"select distinct strategy from {TABLE_NAME}"
        ).fetchall()]
        for strat in strategies:
            print(f"PnL for {strat}:", compute_pnl(strat, conn))
