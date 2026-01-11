from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sqlite3
import time
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = "https://www.epexspot.com/en/market-results"

# Selenium imports for WAF bypass (lazy loaded)
_selenium_available = None
_webdriver = None
DEFAULT_DB_PATH = "epex_prices.sqlite"
EXPECTED_QUARTERS = 96


@dataclass(frozen=True)
class MarketConfig:
    key: str
    name: str
    market_area: str
    auction: str
    modality: str
    sub_modality: str
    product: Optional[str] = None
    data_mode: str = "table"
    include_trading_date: bool = True
    extra_params: Dict[str, str] = field(default_factory=dict)


MARKETS: Dict[str, MarketConfig] = {
    "daa": MarketConfig(
        key="daa",
        name="Day-Ahead Auction DE-LU",
        market_area="DE-LU",
        auction="MRC",
        modality="Auction",
        sub_modality="DayAhead",
        product="15",
    ),
    "ida": MarketConfig(
        key="ida",
        name="Intraday Auction DE-LU",
        market_area="DE-LU",
        auction="IDA1",
        modality="Auction",
        sub_modality="Intraday",
    ),
    "idc": MarketConfig(
        key="idc",
        name="Intraday Continuous (ID1) DE-LU",
        market_area="DE",
        auction="",
        modality="Continuous",
        sub_modality="",
        product="15",
        include_trading_date=False,
    ),
}


class _HTMLTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: List[List[List[str]]] = []
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._current_table: List[List[str]] = []
        self._current_row: List[str] = []
        self._current_cell: List[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag == "table":
            self._in_table = True
            self._current_table = []
        elif self._in_table and tag == "tr":
            self._in_row = True
            self._current_row = []
        elif self._in_table and self._in_row and tag in ("td", "th"):
            self._in_cell = True
            self._current_cell = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th") and self._in_cell:
            cell = "".join(self._current_cell).strip()
            self._current_row.append(cell)
            self._in_cell = False
        elif tag == "tr" and self._in_row:
            if self._current_row:
                self._current_table.append(self._current_row)
            self._in_row = False
        elif tag == "table" and self._in_table:
            if self._current_table:
                self.tables.append(self._current_table)
            self._in_table = False


def _to_date(value) -> dt.date:
    if value is None:
        return dt.date.today()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value))


def get_delivery_and_trading_dates(delivery_date: Optional[str] = None) -> Tuple[str, str]:
    if delivery_date:
        delivery = _to_date(delivery_date)
    else:
        delivery = dt.date.today() - dt.timedelta(days=1)
    trading = delivery - dt.timedelta(days=1)
    return delivery.isoformat(), trading.isoformat()


def build_market_url(cfg: MarketConfig, delivery_date: str, trading_date: Optional[str]) -> str:
    trading_value = trading_date if cfg.include_trading_date and trading_date else ""
    params = {
        "market_area": cfg.market_area,
        "auction": cfg.auction,
        "trading_date": trading_value,
        "delivery_date": delivery_date,
        "underlying_year": "",
        "modality": cfg.modality,
        "sub_modality": cfg.sub_modality,
        "technology": "",
        "data_mode": cfg.data_mode,
        "period": "",
        "production_period": "",
    }
    if cfg.product is not None:
        params["product"] = cfg.product
    params.update(cfg.extra_params)
    return f"{BASE_URL}?{urlencode(params)}"


def _is_waf_challenge(html: str) -> bool:
    """Check if the response is an AWS WAF challenge page."""
    return "awsWafCookieDomainList" in html or "challenge.js" in html or "captcha.js" in html


def _fetch_with_selenium(url: str, timeout: int = 90) -> str:
    """Fetch HTML using undetected Chrome to bypass WAF protection.
    
    Note: This may require manual CAPTCHA solving if the site presents one.
    """
    try:
        import undetected_chromedriver as uc
    except ImportError:
        raise RuntimeError(
            "undetected-chromedriver is required to bypass WAF protection. "
            "Install with: pip install undetected-chromedriver"
        )
    
    options = uc.ChromeOptions()
    # Run in visible mode so user can solve CAPTCHA if needed
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    driver = uc.Chrome(options=options, headless=False)
    
    try:
        print(f"Opening browser to fetch: {url}")
        print("If a CAPTCHA appears, please solve it manually...")
        driver.get(url)
        
        # Wait for the page to load
        for i in range(timeout // 2):
            html = driver.page_source
            # Check for actual data content, not just the absence of WAF
            if ("<table" in html and "Price" in html) or "__NEXT_DATA__" in html:
                print("Page loaded successfully!")
                return html
            if i % 5 == 0:
                print(f"Waiting for page to load... ({i*2}s)")
            time.sleep(2)
        
        # Return whatever we have after timeout
        print("Timeout reached, returning current page content")
        return driver.page_source
    finally:
        try:
            driver.quit()
        except OSError:
            # Ignore Windows handle errors during cleanup
            pass


def fetch_html(url: str, timeout: int = 30, use_selenium_fallback: bool = True) -> str:
    """Fetch HTML from URL, using Selenium if WAF protection is detected."""
    from urllib.error import HTTPError, URLError
    
    # Always use Selenium for EPEX website due to bot protection
    if use_selenium_fallback and "epexspot.com" in url:
        print("Using Selenium to fetch EPEX page (bot protection active)...")
        return _fetch_with_selenium(url, timeout=90)
    
    try:
        req = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        html = data.decode("utf-8", errors="replace")
        
        # Check if we got a WAF challenge page
        if use_selenium_fallback and _is_waf_challenge(html):
            print("WAF protection detected, using Selenium to fetch page...")
            return _fetch_with_selenium(url, timeout=90)
        
        return html
    
    except (HTTPError, URLError) as e:
        if use_selenium_fallback:
            print(f"HTTP request failed ({e}), using Selenium to fetch page...")
            return _fetch_with_selenium(url, timeout=90)
        raise


def _parse_price_value(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("\xa0", "").replace(" ", "").replace("\u2212", "-")
    if not any(ch.isdigit() for ch in text):
        return None
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text and "." not in text:
        text = text.replace(",", ".")
    text = re.sub(r"[^0-9.\-]", "", text)
    try:
        return float(text)
    except ValueError:
        return None


def _select_candidate(candidates: List[List[float]], expected_quarters: int) -> Optional[List[float]]:
    if not candidates:
        return None
    for candidate in candidates:
        if len(candidate) == expected_quarters:
            return candidate
    return max(candidates, key=len)


def _extract_prices_from_table(table: List[List[str]], expected_quarters: int) -> Optional[List[float]]:
    """Extract prices from a table, looking for the best price column."""
    # Try to find all rows with "price" header and try each one
    price_candidates: List[List[float]] = []
    
    for idx, row in enumerate(table):
        for col, cell in enumerate(row):
            # Look for price header (case insensitive)
            if "price" in cell.lower() and ("€" in cell or "eur" in cell.lower() or "mwh" in cell.lower()):
                # Extract prices from rows after this header
                prices: List[float] = []
                for data_row in table[idx + 1:]:
                    if col < len(data_row):
                        value = _parse_price_value(data_row[col])
                        if value is not None:
                            prices.append(value)
                    else:
                        # If we can't get the expected column, try the last numeric
                        numeric = [_parse_price_value(c) for c in data_row]
                        numeric = [v for v in numeric if v is not None]
                        if numeric:
                            prices.append(numeric[-1])
                
                if prices:
                    price_candidates.append(prices)
    
    # If we found candidates, return the one closest to expected_quarters
    if price_candidates:
        # Prefer exact match
        for candidate in price_candidates:
            if len(candidate) == expected_quarters:
                return candidate
        # Otherwise return the longest
        return max(price_candidates, key=len)
    
    # Fallback: try to extract any numeric values from the last column
    prices: List[float] = []
    for row in table:
        if len(row) >= 4:  # Expect at least 4 columns for price data
            value = _parse_price_value(row[-1])
            if value is not None:
                prices.append(value)
    
    if not prices:
        return None
    return prices


def _extract_prices_from_tables(html: str, expected_quarters: int) -> Optional[List[float]]:
    parser = _HTMLTableParser()
    parser.feed(html)
    candidates: List[List[float]] = []
    for table in parser.tables:
        prices = _extract_prices_from_table(table, expected_quarters)
        if prices:
            candidates.append(prices)
    return _select_candidate(candidates, expected_quarters)


def _extract_prices_from_json(data, expected_quarters: int) -> Optional[List[float]]:
    candidates: List[List[float]] = []

    def add_candidate(values) -> None:
        cleaned: List[float] = []
        for val in values:
            parsed = _parse_price_value(val)
            if parsed is None:
                return
            cleaned.append(parsed)
        if cleaned:
            candidates.append(cleaned)

    def walk(obj) -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, list) and "price" in key.lower():
                    add_candidate(value)
                walk(value)
        elif isinstance(obj, list):
            if obj and all(isinstance(item, dict) for item in obj):
                keys = set()
                for item in obj:
                    keys.update(item.keys())
                price_key = None
                for key in keys:
                    if "price" in key.lower():
                        price_key = key
                        break
                if price_key:
                    add_candidate([item.get(price_key) for item in obj])
            for item in obj:
                walk(item)

    walk(data)
    return _select_candidate(candidates, expected_quarters)


def _extract_prices_from_next_data(html: str, expected_quarters: int) -> Optional[List[float]]:
    match = re.search(r"<script[^>]*id=\"__NEXT_DATA__\"[^>]*>(.*?)</script>", html, re.DOTALL)
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None
    return _extract_prices_from_json(data, expected_quarters)


def parse_prices_from_html(html: str, expected_quarters: int = EXPECTED_QUARTERS) -> List[float]:
    # Check if we got a WAF/CAPTCHA page
    if _is_waf_challenge(html):
        raise ValueError(
            "EPEX website returned a CAPTCHA challenge page.\n"
            "The website now requires human verification to access market data.\n"
            "Options:\n"
            "  1. Run with --no-fetch to use cached data from the database\n"
            "  2. Visit the website manually and copy the data\n"
            "  3. Wait and try again later (bot protection may be temporary)"
        )
    
    prices = _extract_prices_from_next_data(html, expected_quarters)
    if prices is None:
        prices = _extract_prices_from_tables(html, expected_quarters)
    if prices is None:
        raise ValueError(
            "Could not locate price data in the HTML response.\n"
            "The website structure may have changed, or access was blocked."
        )
    if expected_quarters and len(prices) != expected_quarters:
        raise ValueError(f"Expected {expected_quarters} prices, got {len(prices)}.")
    return [float(val) for val in prices]


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS epex_prices (
            market TEXT NOT NULL,
            delivery_date TEXT NOT NULL,
            quarter INTEGER NOT NULL,
            price REAL NOT NULL,
            trading_date TEXT,
            source_url TEXT,
            fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (market, delivery_date, quarter)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_epex_prices_market_date ON epex_prices (market, delivery_date)"
    )


def store_market_prices(
    conn: sqlite3.Connection,
    market_key: str,
    delivery_date: str,
    trading_date: Optional[str],
    prices: List[float],
    source_url: str,
) -> None:
    rows = [
        (market_key, delivery_date, idx + 1, float(price), trading_date, source_url)
        for idx, price in enumerate(prices)
    ]
    conn.executemany(
        """
        INSERT INTO epex_prices (
            market, delivery_date, quarter, price, trading_date, source_url
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(market, delivery_date, quarter) DO UPDATE SET
            price=excluded.price,
            trading_date=excluded.trading_date,
            source_url=excluded.source_url,
            fetched_at=datetime('now')
        """,
        rows,
    )


def load_market_prices(conn: sqlite3.Connection, market_key: str, delivery_date: str) -> List[float]:
    rows = conn.execute(
        """
        SELECT quarter, price
        FROM epex_prices
        WHERE market = ? AND delivery_date = ?
        ORDER BY quarter
        """,
        (market_key, delivery_date),
    ).fetchall()
    return [row[1] for row in rows]


def fetch_market_prices(
    cfg: MarketConfig, delivery_date: str, trading_date: Optional[str], expected_quarters: int = EXPECTED_QUARTERS
) -> Tuple[List[float], str]:
    url = build_market_url(cfg, delivery_date, trading_date)
    html = fetch_html(url)
    # Don't enforce expected_quarters during parsing - check after
    prices = parse_prices_from_html(html, expected_quarters=0)
    return prices, url


def get_prices_for_delivery_date(
    delivery_date: Optional[str] = None,
    db_path: str = DEFAULT_DB_PATH,
    refresh: bool = False,
    allow_fetch: bool = True,
    expected_quarters: int = EXPECTED_QUARTERS,
    markets: Optional[Dict[str, MarketConfig]] = None,
) -> Tuple[str, str, Dict[str, List[float]]]:
    delivery_date, trading_date = get_delivery_and_trading_dates(delivery_date)
    results: Dict[str, List[float]] = {}
    
    # Use provided markets or default to all
    markets_to_fetch = markets if markets is not None else MARKETS

    with sqlite3.connect(db_path) as conn:
        init_db(conn)

        for cfg in markets_to_fetch.values():
            prices: Optional[List[float]] = None
            if not refresh:
                prices = load_market_prices(conn, cfg.key, delivery_date)
                if prices and len(prices) == expected_quarters:
                    results[cfg.key] = prices
                    continue

            if not allow_fetch:
                raise RuntimeError(
                    f"Missing {cfg.name} data for {delivery_date} in {db_path}."
                )

            prices, url = fetch_market_prices(cfg, delivery_date, trading_date, expected_quarters)
            
            # Handle markets that return more data than expected (e.g., IDC returns multiple days)
            if expected_quarters and len(prices) > expected_quarters:
                print(f"Note: {cfg.name} returned {len(prices)} prices, truncating to {expected_quarters}")
                prices = prices[:expected_quarters]
            elif expected_quarters and len(prices) != expected_quarters:
                raise ValueError(
                    f"{cfg.name} returned {len(prices)} prices, expected {expected_quarters}."
                )
            
            store_market_prices(conn, cfg.key, delivery_date, trading_date, prices, url)
            results[cfg.key] = prices

        conn.commit()

    return delivery_date, trading_date, results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch previous-day EPEX quarter-hour prices and store them in SQLite."
    )
    parser.add_argument(
        "--delivery-date",
        help="Delivery date in YYYY-MM-DD (defaults to yesterday).",
    )
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--refresh", action="store_true", help="Force re-fetch.")
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Only read from the database, do not scrape.",
    )
    parser.add_argument(
        "--markets",
        nargs="+",
        choices=list(MARKETS.keys()),
        default=["daa"],
        help="Markets to fetch. Available: daa (Day-Ahead), ida (Intraday Auction), idc (Intraday Continuous). Default: daa",
    )
    args = parser.parse_args()

    # Filter markets based on user selection
    selected_markets = {k: v for k, v in MARKETS.items() if k in args.markets}
    
    delivery_date, _, prices = get_prices_for_delivery_date(
        delivery_date=args.delivery_date,
        db_path=args.db_path,
        refresh=args.refresh,
        allow_fetch=not args.no_fetch,
        markets=selected_markets,
    )

    print(f"Delivery date: {delivery_date}")
    for key, values in prices.items():
        cfg = MARKETS[key]
        print(
            f"{cfg.name}: {len(values)} points, min {min(values):.2f}, max {max(values):.2f}"
        )


if __name__ == "__main__":
    main()
