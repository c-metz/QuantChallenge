"""
Task 1: RESTful Web API for Trades Management
Stores and queries trades with authentication support
"""

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, date
import sqlite3
from pathlib import Path
import secrets

# Database configuration
DB_PATH = Path(__file__).resolve().parent / "trades.sqlite"

# Security
security = HTTPBasic()

# Hardcoded credentials (in production, use environment variables and hashed passwords)
VALID_CREDENTIALS = {
    "trader1": "password123",
    "trader2": "secret456",
    "admin": "admin789"
}

# Pydantic models
class Trade(BaseModel):
    """Trade model matching the API specification"""
    trade_id: Optional[str] = None
    trader_id: str = Field(..., description="Unique trader identifier")
    delivery_day: date = Field(..., description="Energy delivery date (YYYY-MM-DD)")
    delivery_hour: int = Field(..., ge=0, le=23, description="Hour of delivery (0-23)")
    quantity: float = Field(..., description="Quantity in MW (positive for both buy/sell)")
    price: float = Field(..., description="Price in EUR/MWh")
    side: str = Field(..., pattern="^(buy|sell)$", description="Trade side: buy or sell")
    strategy: Optional[str] = Field(None, description="Strategy name")
    timestamp: Optional[datetime] = Field(None, description="Trade execution timestamp")

class TradeResponse(Trade):
    """Response model with computed trade_id"""
    trade_id: str
    timestamp: datetime

# Authentication
def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    """Verify username and password"""
    username = credentials.username
    password = credentials.password
    
    if username not in VALID_CREDENTIALS:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    
    # Constant-time comparison to prevent timing attacks
    if not secrets.compare_digest(password.encode(), VALID_CREDENTIALS[username].encode()):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    
    return username

# Database operations
def init_db():
    """Initialize database with trades table if not exists"""
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
    
    # Create indexes for faster queries
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_delivery_day ON trades(delivery_day)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_trader_id ON trades(trader_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_trader_delivery ON trades(trader_id, delivery_day)
    """)
    
    conn.commit()
    conn.close()

def generate_trade_id(trader_id: str, timestamp: datetime) -> str:
    """Generate unique trade ID"""
    return f"{trader_id}_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}"

# FastAPI app
app = FastAPI(
    title="Energy Trading API",
    description="RESTful API for storing and querying energy trades",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    init_db()

@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint"""
    return {
        "status": "online",
        "service": "Energy Trading API",
        "version": "1.0.0"
    }

@app.post("/trades", response_model=TradeResponse, status_code=status.HTTP_201_CREATED, tags=["Trades"])
async def create_trade(trade: Trade, username: str = Depends(verify_credentials)):
    """
    Create a new trade
    
    Requires authentication with username and password.
    Returns the created trade with generated trade_id and timestamp.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Generate trade_id and timestamp if not provided
        timestamp = trade.timestamp or datetime.now()
        trade_id = trade.trade_id or generate_trade_id(trade.trader_id, timestamp)
        
        # Insert trade
        cursor.execute("""
            INSERT INTO trades (trade_id, trader_id, delivery_day, delivery_hour, 
                              quantity, price, side, strategy, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade_id,
            trade.trader_id,
            trade.delivery_day.isoformat(),
            trade.delivery_hour,
            trade.quantity,
            trade.price,
            trade.side,
            trade.strategy,
            timestamp.isoformat()
        ))
        
        conn.commit()
        conn.close()
        
        return TradeResponse(
            trade_id=trade_id,
            trader_id=trade.trader_id,
            delivery_day=trade.delivery_day,
            delivery_hour=trade.delivery_hour,
            quantity=trade.quantity,
            price=trade.price,
            side=trade.side,
            strategy=trade.strategy,
            timestamp=timestamp
        )
        
    except sqlite3.IntegrityError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Trade with this ID already exists: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )

@app.get("/trades", response_model=List[TradeResponse], tags=["Trades"])
async def get_trades(
    delivery_day: Optional[date] = None,
    trader_id: Optional[str] = None,
    username: str = Depends(verify_credentials)
):
    """
    Query trades with optional filters
    
    Filters:
    - delivery_day: Filter by delivery date (YYYY-MM-DD)
    - trader_id: Filter by trader identifier
    
    Returns list of trades matching the filters (or all trades if no filters).
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Build query with filters
        query = "SELECT * FROM trades WHERE 1=1"
        params = []
        
        if delivery_day:
            query += " AND delivery_day = ?"
            params.append(delivery_day.isoformat())
        
        if trader_id:
            query += " AND trader_id = ?"
            params.append(trader_id)
        
        query += " ORDER BY timestamp DESC"
        
        cursor.execute(query, params)
        
        trades = []
        for row in cursor.fetchall():
            trades.append(TradeResponse(
                trade_id=row[0],
                trader_id=row[1],
                delivery_day=date.fromisoformat(row[2]),
                delivery_hour=row[3],
                quantity=row[4],
                price=row[5],
                side=row[6],
                strategy=row[7],
                timestamp=datetime.fromisoformat(row[8])
            ))
        
        conn.close()
        return trades
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )

@app.get("/trades/{trade_id}", response_model=TradeResponse, tags=["Trades"])
async def get_trade(trade_id: str, username: str = Depends(verify_credentials)):
    """Get a specific trade by ID"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM trades WHERE trade_id = ?", (trade_id,))
        row = cursor.fetchone()
        
        conn.close()
        
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Trade {trade_id} not found"
            )
        
        return TradeResponse(
            trade_id=row[0],
            trader_id=row[1],
            delivery_day=date.fromisoformat(row[2]),
            delivery_hour=row[3],
            quantity=row[4],
            price=row[5],
            side=row[6],
            strategy=row[7],
            timestamp=datetime.fromisoformat(row[8])
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )

@app.delete("/trades/{trade_id}", tags=["Trades"])
async def delete_trade(trade_id: str, username: str = Depends(verify_credentials)):
    """Delete a trade by ID"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM trades WHERE trade_id = ?", (trade_id,))
        
        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Trade {trade_id} not found"
            )
        
        conn.commit()
        conn.close()
        
        return {"message": f"Trade {trade_id} deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    print("Starting Energy Trading API...")
    print("Default credentials:")
    print("  - trader1:password123")
    print("  - trader2:secret456")
    print("  - admin:admin789")
    uvicorn.run(app, host="0.0.0.0", port=8000)
