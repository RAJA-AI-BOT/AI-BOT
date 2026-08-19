from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import time
import json
import secrets
import hmac
import hashlib
import base64
import threading
import queue
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from collections import Counter, OrderedDict
from datetime import datetime, timezone
from urllib.request import Request as UrlRequest, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError

# Lazy-load yfinance only when a market scan actually starts.
# This keeps the Flask/Gunicorn web shell lighter during Render cold boot.
_yf_module = None
_yf_import_lock = threading.Lock()

def _get_yfinance():
    global _yf_module
    if _yf_module is None:
        with _yf_import_lock:
            if _yf_module is None:
                import yfinance as _yf
                _yf_module = _yf
    return _yf_module


# Pandas is used only when Twelve Data backup candles are needed.
_pd_module = None
_pd_import_lock = threading.Lock()

def _get_pandas():
    global _pd_module
    if _pd_module is None:
        with _pd_import_lock:
            if _pd_module is None:
                import pandas as _pd
                _pd_module = _pd
    return _pd_module


try:
    import psycopg
except Exception:
    psycopg = None

app = Flask(__name__, static_folder=".", template_folder=".")
CORS(app)

# =========================================================
# RAJA AI MULTI-TIMEFRAME BACKEND
# Yahoo Finance 1-minute OHLCV is the PRIMARY base/reference feed.
# If Yahoo is unavailable/stale and TWELVE_DATA_API_KEY is configured,
# Twelve Data 1-minute OHLCV becomes the LIVE BACKUP reference feed.
# 2m, 5m, 10m, 15m and 30m are resampled from the chosen 1-minute feed.
#
# IMPORTANT: "(OTC)" assets are underlying-market/reference proxies.
# They are NOT exact broker OTC quotes (Quotex/Pocket Option).
# =========================================================

YAHOO_SYMBOLS = {
    # ---------------- Crypto Live ----------------
    "BTC-USD": "BTC-USD",
    "ETH-USD": "ETH-USD",
    "SOL-USD": "SOL-USD",
    "LTC-USD": "LTC-USD",
    "XRP-USD": "XRP-USD",
    "ADA-USD": "ADA-USD",
    "DOGE-USD": "DOGE-USD",

    # ---------------- Crypto OTC proxies ----------------
    # Yahoo underlying-market proxies; not exact Quotex OTC quotes.
    # Keep this list synchronized with the 17 Quotex Crypto OTC assets in index.html.
    "Zcash (OTC)": "ZEC-USD",
    "Chainlink (OTC)": "LINK-USD",
    "Bitcoin (OTC)": "BTC-USD",
    "Binance Coin (OTC)": "BNB-USD",
    "Ethereum (OTC)": "ETH-USD",
    "Bitcoin Cash (OTC)": "BCH-USD",
    "Cosmos (OTC)": "ATOM-USD",
    "Ethereum Classic (OTC)": "ETC-USD",
    "Axie Infinity (OTC)": "AXS-USD",
    "Trump (OTC)": "TRUMP35336-USD",
    "Dash (OTC)": "DASH-USD",
    "Solana (OTC)": "SOL-USD",
    "Toncoin (OTC)": "TON11419-USD",
    "Litecoin (OTC)": "LTC-USD",
    "Avalanche (OTC)": "AVAX-USD",
    "Polkadot (OTC)": "DOT-USD",
    "Ripple (OTC)": "XRP-USD",

    # ---------------- Forex Live ----------------
    "EUR/USD": "EURUSD=X",
    "GBP/USD": "GBPUSD=X",
    "USD/JPY": "USDJPY=X",
    "AUD/USD": "AUDUSD=X",
    "USD/CAD": "USDCAD=X",
    "USD/CHF": "USDCHF=X",
    "NZD/USD": "NZDUSD=X",
    "EUR/GBP": "EURGBP=X",
    "EUR/JPY": "EURJPY=X",
    "GBP/JPY": "GBPJPY=X",
    "AUD/JPY": "AUDJPY=X",
    "EUR/AUD": "EURAUD=X",
    "GBP/AUD": "GBPAUD=X",
    "CAD/JPY": "CADJPY=X",
    "EUR/CAD": "EURCAD=X",
    "GBP/CAD": "GBPCAD=X",
    "NZD/JPY": "NZDJPY=X",
    "AUD/NZD": "AUDNZD=X",
    "EUR/CHF": "EURCHF=X",
    "GBP/CHF": "GBPCHF=X",
    # Gold live reference uses Yahoo gold futures because the old spot-style symbol returned 404.
    "XAUUSD": "GC=F",

    # ---------------- Current Quotex Forex OTC list ----------------
    # These are Yahoo underlying/FX proxies; exact Quotex OTC candles can differ.
    "USD/BRL (OTC)": "USDBRL=X",
    "NZD/CHF (OTC)": "NZDCHF=X",
    "NZD/JPY (OTC)": "NZDJPY=X",
    "USD/COP (OTC)": "USDCOP=X",
    "USD/MXN (OTC)": "USDMXN=X",
    "AUD/NZD (OTC)": "AUDNZD=X",
    "USD/BDT (OTC)": "USDBDT=X",
    "USD/DZD (OTC)": "USDDZD=X",
    "USD/NGN (OTC)": "USDNGN=X",
    "USD/PHP (OTC)": "USDPHP=X",
    "USD/PKR (OTC)": "USDPKR=X",
    "USD/ZAR (OTC)": "USDZAR=X",
    "USD/INR (OTC)": "USDINR=X",
    "USD/EGP (OTC)": "USDEGP=X",
    "USD/IDR (OTC)": "USDIDR=X",
    "USD/ARS (OTC)": "USDARS=X",
    "GBP/NZD (OTC)": "GBPNZD=X",
    "EUR/NZD (OTC)": "EURNZD=X",
    "NZD/USD (OTC)": "NZDUSD=X",
    "NZD/CAD (OTC)": "NZDCAD=X",
    "CAD/CHF (OTC)": "CADCHF=X",

    # ---------------- Pocket Option OTC reference proxies ----------------
    # Pocket Option pair lists are broker-specific in index.html.
    # These Yahoo symbols are REFERENCE proxies only, not exact Pocket Option OTC candles.

    # Pocket Option Crypto OTC additions
    "BNB (OTC)": "BNB-USD",
    "Cardano (OTC)": "ADA-USD",
    "Polygon (OTC)": "MATIC-USD",
    "TRON (OTC)": "TRX-USD",
    "Bitcoin ETF (OTC)": "IBIT",
    "Dogecoin (OTC)": "DOGE-USD",

    # Pocket Option Forex OTC additions (30-pair preferred set)
    "EUR/USD (OTC)": "EURUSD=X",
    "GBP/USD (OTC)": "GBPUSD=X",
    "USD/JPY (OTC)": "USDJPY=X",
    "AUD/USD (OTC)": "AUDUSD=X",
    "USD/CAD (OTC)": "USDCAD=X",
    "USD/CHF (OTC)": "USDCHF=X",
    "EUR/JPY (OTC)": "EURJPY=X",
    "GBP/JPY (OTC)": "GBPJPY=X",
    "AUD/JPY (OTC)": "AUDJPY=X",
    "CAD/JPY (OTC)": "CADJPY=X",
    "EUR/CHF (OTC)": "EURCHF=X",
    "EUR/GBP (OTC)": "EURGBP=X",
    "AUD/CAD (OTC)": "AUDCAD=X",
    "AUD/CHF (OTC)": "AUDCHF=X",
    "GBP/AUD (OTC)": "GBPAUD=X",
    "CHF/JPY (OTC)": "CHFJPY=X",
    "USD/SGD (OTC)": "USDSGD=X",
    "USD/CNH (OTC)": "USDCNH=X",
    "USD/MYR (OTC)": "USDMYR=X",
    "EUR/TRY (OTC)": "EURTRY=X",

    # Pocket Option Stocks OTC
    "Apple (OTC)": "AAPL",
    "American Express (OTC)": "AXP",
    "Boeing Company (OTC)": "BA",
    "Cisco (OTC)": "CSCO",
    "Facebook Inc (OTC)": "META",
    "Intel (OTC)": "INTC",
    "Johnson & Johnson (OTC)": "JNJ",
    "McDonald's (OTC)": "MCD",
    "Microsoft (OTC)": "MSFT",
    "Pfizer Inc (OTC)": "PFE",
    "Tesla (OTC)": "TSLA",
    "ExxonMobil (OTC)": "XOM",
    "Advanced Micro Devices (OTC)": "AMD",
}
# Twelve Data LIVE BACKUP symbols. These are underlying/reference instruments,
# never exact Quotex/Pocket Option OTC candles.
TWELVE_DATA_SYMBOLS = {
    # Crypto Live
    "BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD", "SOL-USD": "SOL/USD",
    "LTC-USD": "LTC/USD", "XRP-USD": "XRP/USD", "ADA-USD": "ADA/USD",
    "DOGE-USD": "DOGE/USD",

    # Crypto OTC reference instruments
    "Zcash (OTC)": "ZEC/USD", "Chainlink (OTC)": "LINK/USD",
    "Bitcoin (OTC)": "BTC/USD", "Binance Coin (OTC)": "BNB/USD",
    "Ethereum (OTC)": "ETH/USD", "Bitcoin Cash (OTC)": "BCH/USD",
    "Cosmos (OTC)": "ATOM/USD", "Ethereum Classic (OTC)": "ETC/USD",
    "Axie Infinity (OTC)": "AXS/USD", "Trump (OTC)": "TRUMP/USD",
    "Dash (OTC)": "DASH/USD", "Solana (OTC)": "SOL/USD",
    "Toncoin (OTC)": "TON/USD", "Litecoin (OTC)": "LTC/USD",
    "Avalanche (OTC)": "AVAX/USD", "Polkadot (OTC)": "DOT/USD",
    "Ripple (OTC)": "XRP/USD", "BNB (OTC)": "BNB/USD",
    "Cardano (OTC)": "ADA/USD", "Polygon (OTC)": "MATIC/USD",
    "TRON (OTC)": "TRX/USD", "Dogecoin (OTC)": "DOGE/USD",
    "Bitcoin ETF (OTC)": "IBIT",

    # Gold
    "XAUUSD": "XAU/USD",

    # Pocket Option Stocks OTC reference instruments
    "Apple (OTC)": "AAPL", "American Express (OTC)": "AXP",
    "Boeing Company (OTC)": "BA", "Cisco (OTC)": "CSCO",
    "Facebook Inc (OTC)": "META", "Intel (OTC)": "INTC",
    "Johnson & Johnson (OTC)": "JNJ", "McDonald's (OTC)": "MCD",
    "Microsoft (OTC)": "MSFT", "Pfizer Inc (OTC)": "PFE",
    "Tesla (OTC)": "TSLA", "ExxonMobil (OTC)": "XOM",
    "Advanced Micro Devices (OTC)": "AMD",
}

# Forex Live/OTC symbols already use slash notation in the app. Twelve Data accepts
# that notation directly, so populate the remaining FX mappings automatically.
for _raja_pair in YAHOO_SYMBOLS:
    _clean_pair = str(_raja_pair).replace(" (OTC)", "").strip()
    if "/" in _clean_pair:
        TWELVE_DATA_SYMBOLS.setdefault(_raja_pair, _clean_pair)

ALL_PAIRS = list(YAHOO_SYMBOLS.keys())
UNIQUE_YAHOO_SYMBOLS = list(dict.fromkeys(YAHOO_SYMBOLS.values()))
FOREX_OTC_PAIRS = [pair for pair in YAHOO_SYMBOLS if pair.endswith(" (OTC)") and "/" in pair]

POCKET_OPTION_FOREX_OTC_PAIRS = ['EUR/USD (OTC)', 'GBP/USD (OTC)', 'USD/JPY (OTC)', 'AUD/USD (OTC)', 'USD/CAD (OTC)', 'USD/CHF (OTC)', 'EUR/JPY (OTC)', 'GBP/JPY (OTC)', 'AUD/JPY (OTC)', 'CAD/JPY (OTC)', 'EUR/CHF (OTC)', 'EUR/GBP (OTC)', 'AUD/CAD (OTC)', 'AUD/CHF (OTC)', 'CAD/CHF (OTC)', 'NZD/JPY (OTC)', 'AUD/NZD (OTC)', 'EUR/NZD (OTC)', 'GBP/AUD (OTC)', 'CHF/JPY (OTC)', 'USD/MXN (OTC)', 'USD/BRL (OTC)', 'USD/INR (OTC)', 'USD/SGD (OTC)', 'USD/CNH (OTC)', 'USD/IDR (OTC)', 'USD/PHP (OTC)', 'USD/MYR (OTC)', 'USD/COP (OTC)', 'EUR/TRY (OTC)']
POCKET_OPTION_CRYPTO_OTC_PAIRS = ['BNB (OTC)', 'Polkadot (OTC)', 'Ethereum (OTC)', 'Toncoin (OTC)', 'Cardano (OTC)', 'Polygon (OTC)', 'TRON (OTC)', 'Avalanche (OTC)', 'Bitcoin (OTC)', 'Bitcoin ETF (OTC)', 'Solana (OTC)', 'Chainlink (OTC)', 'Litecoin (OTC)', 'Dogecoin (OTC)']
POCKET_OPTION_STOCKS_OTC_PAIRS = ['Apple (OTC)', 'American Express (OTC)', 'Boeing Company (OTC)', 'Cisco (OTC)', 'Facebook Inc (OTC)', 'Intel (OTC)', 'Johnson & Johnson (OTC)', "McDonald's (OTC)", 'Microsoft (OTC)', 'Pfizer Inc (OTC)', 'Tesla (OTC)', 'ExxonMobil (OTC)', 'Advanced Micro Devices (OTC)']

TIMEFRAMES = {
    "1m": 1,
    "2m": 2,
    "5m": 5,
    "10m": 10,
    "15m": 15,
    "30m": 30,
}

# Selected trade expiry must be confirmed by the matching analysis timeframe.
# 15s/30s are intentionally excluded because the base Yahoo feed is 1-minute.
EXPIRY_CONFIRMATION_TIMEFRAME = {
    "1m": "1m",
    "2m": "2m",
    "5m": "5m",
    "10m": "10m",
    "15m": "15m",
    "30m": "30m",
}

# One Yahoo 1m download per unique symbol; all higher TFs are resampled.
CACHE_DURATION = int(os.environ.get("RAJA_CACHE_SECONDS", "90"))
STALE_CACHE_MAX_AGE = int(os.environ.get("RAJA_STALE_CACHE_SECONDS", "180"))
YAHOO_FAILURE_COOLDOWN = int(os.environ.get("RAJA_YAHOO_FAILURE_COOLDOWN", "180"))
# Keep three Yahoo fetches in flight to match the default three batch workers; this reduces
# partial 21-pair scans without opening an aggressive request storm.
YAHOO_FETCH_CONCURRENCY = max(1, min(3, int(os.environ.get("RAJA_YAHOO_CONCURRENCY", "3"))))
YAHOO_MIN_GAP_SECONDS = max(0.0, float(os.environ.get("RAJA_YAHOO_MIN_GAP", "0.30")))
BATCH_CACHE_DURATION = max(5, int(os.environ.get("RAJA_BATCH_CACHE_SECONDS", "30")))
BATCH_SCAN_WORKERS = max(1, min(4, int(os.environ.get("RAJA_BATCH_WORKERS", "4"))))
YAHOO_REQUEST_TIMEOUT_SECONDS = max(3.0, min(15.0, float(os.environ.get("RAJA_YAHOO_REQUEST_TIMEOUT", "7"))))
YAHOO_SYMBOL_LOCK_WAIT_SECONDS = max(2.0, min(20.0, float(os.environ.get("RAJA_YAHOO_SYMBOL_LOCK_WAIT", "8"))))
YAHOO_SEMAPHORE_WAIT_SECONDS = max(2.0, min(25.0, float(os.environ.get("RAJA_YAHOO_SEMAPHORE_WAIT", "12"))))
# Reject market candles that are too old for a live 1-minute trading decision.
# This prevents a freshly-downloaded but old weekend/closed-market candle from being labelled "fresh".
MAX_SOURCE_CANDLE_AGE_SECONDS = max(120, min(3600, int(os.environ.get("RAJA_MAX_SOURCE_CANDLE_AGE", "300"))))
# Browser batch timeout is 90s. Keep the backend deadline comfortably below that
# so slow Yahoo symbols become a safe PARTIAL response instead of a browser failure.
# 58s also leaves headroom for auth, news-safety checks, DB work and network latency.
BATCH_SCAN_DEADLINE_SECONDS = max(25.0, min(75.0, float(os.environ.get("RAJA_BATCH_DEADLINE_SECONDS", "75"))))
FOREX_OTC_FALLBACK_DEADLINE_SECONDS = max(BATCH_SCAN_DEADLINE_SECONDS, min(78.0, float(os.environ.get("RAJA_FOREX_OTC_FALLBACK_DEADLINE_SECONDS", "72"))))

# Twelve Data live backup. The key stays server-side in Railway environment variables.
TWELVE_DATA_API_KEY = (
    os.environ.get("TWELVE_DATA_API_KEY")
    or os.environ.get("RAJA_TWELVE_DATA_API_KEY")
    or ""
).strip()
TWELVE_DATA_ENABLED = bool(TWELVE_DATA_API_KEY)
TWELVE_DATA_BASE_URL = (os.environ.get("RAJA_TWELVE_DATA_BASE_URL") or "https://api.twelvedata.com/time_series").strip()
TWELVE_DATA_OUTPUTSIZE = max(900, min(5000, int(os.environ.get("RAJA_TWELVE_DATA_OUTPUTSIZE", "4000"))))
TWELVE_DATA_CACHE_SECONDS = max(15, int(os.environ.get("RAJA_TWELVE_DATA_CACHE_SECONDS", "45")))
TWELVE_DATA_FAILURE_COOLDOWN = max(30, int(os.environ.get("RAJA_TWELVE_DATA_FAILURE_COOLDOWN", "120")))
TWELVE_DATA_GLOBAL_RATE_LIMIT_COOLDOWN = max(30, int(os.environ.get("RAJA_TWELVE_DATA_RATE_LIMIT_COOLDOWN", "90")))
TWELVE_DATA_REQUEST_TIMEOUT_SECONDS = max(3.0, min(20.0, float(os.environ.get("RAJA_TWELVE_DATA_REQUEST_TIMEOUT", "9"))))
TWELVE_DATA_FETCH_CONCURRENCY = max(1, min(3, int(os.environ.get("RAJA_TWELVE_DATA_CONCURRENCY", "2"))))
TWELVE_DATA_MIN_GAP_SECONDS = max(0.0, float(os.environ.get("RAJA_TWELVE_DATA_MIN_GAP", "0.20")))

market_cache = {}
cache_lock = threading.RLock()

symbol_fetch_locks = {}
symbol_fetch_locks_guard = threading.Lock()
failed_symbol_until = {}
failed_symbol_lock = threading.Lock()

yahoo_fetch_semaphore = threading.BoundedSemaphore(YAHOO_FETCH_CONCURRENCY)
yahoo_pace_lock = threading.Lock()
last_yahoo_fetch_started = 0.0

# Twelve Data backup cache/circuit breaker. It is separate from Yahoo so the
# primary feed and backup feed never overwrite one another.
twelve_data_cache = {}
twelve_data_cache_lock = threading.RLock()
twelve_data_symbol_locks = {}
twelve_data_symbol_locks_guard = threading.Lock()
twelve_data_failed_until = {}
twelve_data_failed_lock = threading.Lock()
twelve_data_global_blocked_until = 0.0
twelve_data_global_lock = threading.Lock()
twelve_data_fetch_semaphore = threading.BoundedSemaphore(TWELVE_DATA_FETCH_CONCURRENCY)
twelve_data_pace_lock = threading.Lock()
last_twelve_data_fetch_started = 0.0

batch_cache = {}
batch_cache_lock = threading.RLock()
batch_key_locks = {}
batch_key_locks_guard = threading.Lock()

# Forex Factory exposes an official weekly JSON export from the calendar page.
# Proxy it through this backend so the browser avoids cross-origin issues and
# can keep a short cache instead of hitting the source on every click.
FOREX_FACTORY_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
MARKET_NEWS_CACHE_SECONDS = max(30, int(os.environ.get("RAJA_NEWS_CACHE_SECONDS", "60")))
market_news_cache = {"timestamp": 0.0, "data": []}
market_news_lock = threading.RLock()

# Duplicate rotation is handled client-side per browser, so one customer's
# scan never suppresses another customer's signal.
recent_signal_lock = threading.Lock()
recent_signals = {}
DUPLICATE_SIGNAL_COOLDOWN = 0


# =========================================================
# LICENSE STORE
# =========================================================

BASE_DIR = Path(__file__).resolve().parent

# One deterministic build ID for the deployed RAJA AI app.
# It changes whenever backend/frontend/PWA source changes, but not on a simple
# Render cold-start. Installed clients use it to detect a real new deployment.
def _compute_app_build_id():
    digest = hashlib.sha256()
    build_files = [
        Path(__file__).resolve(),
        BASE_DIR / "index.html",
        BASE_DIR / "sw.js",
        BASE_DIR / "manifest.json",
        BASE_DIR / "raja-ai-icon-192.png",
        BASE_DIR / "raja-ai-icon-512.png",
        BASE_DIR / "raja-ai-icon-192-v2.png",
        BASE_DIR / "raja-ai-icon-512-v2.png",
        BASE_DIR / "raja-ai-icon-192-v3.png",
        BASE_DIR / "raja-ai-icon-512-v3.png",
        BASE_DIR / "raja-splash-logo.png",
    ]
    found = False
    for path in build_files:
        try:
            if path.exists() and path.is_file():
                digest.update(path.name.encode("utf-8", errors="ignore"))
                digest.update(path.read_bytes())
                found = True
        except Exception:
            continue
    return digest.hexdigest()[:16] if found else "unknown"

APP_BUILD_ID = _compute_app_build_id()
APP_BUILD_TOKEN = "__RAJA_APP_BUILD__"

DATA_DIR = Path(os.environ.get("RAJA_DATA_DIR", str(BASE_DIR))).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

LICENSE_FILE = DATA_DIR / "licenses.json"
license_lock = threading.RLock()

ADMIN_PASSWORD = (os.environ.get("RAJA_ADMIN_PASSWORD") or "").strip()

# Quotex Forex OTC fallback + RAJA Quotex Bridge.
# The bridge receives market candles/ticks from the customer's own logged-in
# Quotex browser tab. No Quotex password/cookie is sent to RAJA AI.
RAJA_QUOTEX_OTC_URL = (os.environ.get("RAJA_QUOTEX_OTC_URL") or "https://qxbroker.com/en/").strip()
RAJA_QUOTEX_OTC_COMPANION_URL = (os.environ.get("RAJA_QUOTEX_OTC_COMPANION_URL") or "").strip()
RAJA_REQUIRE_QUOTEX_BRIDGE_FOR_OTC = str(os.environ.get("RAJA_REQUIRE_QUOTEX_BRIDGE_FOR_OTC", "1")).strip().lower() not in {"0", "false", "no", "off"}
QUOTEX_BRIDGE_PAIR_CODE_TTL_SECONDS = max(120, min(1800, int(os.environ.get("RAJA_QUOTEX_BRIDGE_PAIR_CODE_TTL", "600"))))
QUOTEX_BRIDGE_TOKEN_TTL_SECONDS = max(3600, min(31536000, int(os.environ.get("RAJA_QUOTEX_BRIDGE_TOKEN_TTL", str(30 * 24 * 3600)))))
QUOTEX_BRIDGE_MAX_CANDLES = max(1900, min(5000, int(os.environ.get("RAJA_QUOTEX_BRIDGE_MAX_CANDLES", "2500"))))
_QUOTEX_BRIDGE_SECRET_TEXT = (os.environ.get("RAJA_QUOTEX_BRIDGE_SECRET") or ADMIN_PASSWORD or "").strip()
if _QUOTEX_BRIDGE_SECRET_TEXT:
    QUOTEX_BRIDGE_SECRET = hashlib.sha256(_QUOTEX_BRIDGE_SECRET_TEXT.encode("utf-8")).digest()
else:
    # Functional fallback for local/testing. Set RAJA_QUOTEX_BRIDGE_SECRET in Railway
    # so paired extensions remain valid across server restarts/deployments.
    QUOTEX_BRIDGE_SECRET = secrets.token_bytes(32)

quotex_bridge_pair_codes = {}
quotex_bridge_pair_codes_lock = threading.RLock()
quotex_bridge_candles = {}
quotex_bridge_status = {}
quotex_bridge_data_lock = threading.RLock()


# =========================================================
# RAJA QUOTEX OTC BRIDGE
# =========================================================
def _bridge_b64e(raw):
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _bridge_b64d(text):
    text = str(text or "")
    return base64.urlsafe_b64decode(text + ("=" * (-len(text) % 4)))


def _issue_quotex_bridge_token(user, device):
    payload = {
        "v": 1,
        "u": normalize_user_id(user),
        "d": str(device or "")[:160],
        "iat": int(time.time()),
        "exp": int(time.time()) + QUOTEX_BRIDGE_TOKEN_TTL_SECONDS,
    }
    encoded = _bridge_b64e(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    sig = _bridge_b64e(hmac.new(QUOTEX_BRIDGE_SECRET, encoded.encode("ascii"), hashlib.sha256).digest())
    return encoded + "." + sig


def _validate_quotex_bridge_token(token):
    try:
        encoded, sig = str(token or "").strip().split(".", 1)
        expected = _bridge_b64e(hmac.new(QUOTEX_BRIDGE_SECRET, encoded.encode("ascii"), hashlib.sha256).digest())
        if not hmac.compare_digest(sig, expected):
            return None
        payload = json.loads(_bridge_b64d(encoded).decode("utf-8"))
        if int(payload.get("v") or 0) != 1 or int(payload.get("exp") or 0) <= int(time.time()):
            return None
        user = normalize_user_id(payload.get("u"))
        device = str(payload.get("d") or "").strip()
        if not user or not device:
            return None
        return {"user": user, "device": device, "expires_at": int(payload["exp"])}
    except Exception:
        return None


def _quotex_bridge_pair_key(user, pair):
    return normalize_user_id(user), str(pair or "").strip()


def _normalize_bridge_epoch(value):
    try:
        value = float(value)
    except Exception:
        return None
    if value > 1000000000000:
        value /= 1000.0
    if value < 1000000000 or value > time.time() + 86400:
        return None
    return int(value)


def _bridge_upsert_candle(user, pair, candle):
    if pair not in YAHOO_SYMBOLS or "(OTC)" not in pair:
        return False
    if not isinstance(candle, dict):
        return False
    epoch = _normalize_bridge_epoch(candle.get("t", candle.get("time", candle.get("timestamp"))))
    try:
        o = float(candle.get("o", candle.get("open")))
        h = float(candle.get("h", candle.get("high")))
        l = float(candle.get("l", candle.get("low")))
        c = float(candle.get("c", candle.get("close")))
    except Exception:
        return False
    if epoch is None or min(o, h, l, c) <= 0 or h < max(o, c) or l > min(o, c):
        return False
    minute = int(epoch // 60 * 60)
    key = _quotex_bridge_pair_key(user, pair)
    with quotex_bridge_data_lock:
        book = quotex_bridge_candles.setdefault(key, OrderedDict())
        existing = book.get(minute)
        if existing:
            # Historical updates can refine the same minute; preserve the broadest H/L.
            existing["Open"] = float(existing.get("Open", o))
            existing["High"] = max(float(existing.get("High", h)), h)
            existing["Low"] = min(float(existing.get("Low", l)), l)
            existing["Close"] = c
        else:
            book[minute] = {"Open": o, "High": h, "Low": l, "Close": c, "Volume": 0.0}
        book.move_to_end(minute)
        while len(book) > QUOTEX_BRIDGE_MAX_CANDLES:
            book.popitem(last=False)
    return True


def _bridge_upsert_tick(user, pair, price, epoch=None):
    if pair not in YAHOO_SYMBOLS or "(OTC)" not in pair:
        return False
    try:
        price = float(price)
    except Exception:
        return False
    if price <= 0:
        return False
    epoch = _normalize_bridge_epoch(epoch) or int(time.time())
    minute = int(epoch // 60 * 60)
    key = _quotex_bridge_pair_key(user, pair)
    with quotex_bridge_data_lock:
        book = quotex_bridge_candles.setdefault(key, OrderedDict())
        row = book.get(minute)
        if row:
            row["High"] = max(float(row["High"]), price)
            row["Low"] = min(float(row["Low"]), price)
            row["Close"] = price
        else:
            book[minute] = {"Open": price, "High": price, "Low": price, "Close": price, "Volume": 0.0}
        book.move_to_end(minute)
        while len(book) > QUOTEX_BRIDGE_MAX_CANDLES:
            book.popitem(last=False)
    return True


def _set_quotex_bridge_status(user, device, pair=None, price=None, source_page=None):
    user = normalize_user_id(user)
    with quotex_bridge_data_lock:
        current = dict(quotex_bridge_status.get(user) or {})
        current.update({
            "connected": True,
            "last_seen": time.time(),
            "device": str(device or "")[:160],
        })
        if pair:
            current["pair"] = str(pair)[:120]
        if price is not None:
            try: current["price"] = float(price)
            except Exception: pass
        if source_page:
            current["source_page"] = str(source_page)[:300]
        quotex_bridge_status[user] = current


def _get_quotex_bridge_status(user, pair=None):
    user = normalize_user_id(user)
    now = time.time()
    with quotex_bridge_data_lock:
        status = dict(quotex_bridge_status.get(user) or {})
        if pair:
            book = quotex_bridge_candles.get(_quotex_bridge_pair_key(user, pair))
            status["candle_count"] = len(book) if book else 0
        else:
            status["pairs_with_data"] = sum(1 for (u, _p), book in quotex_bridge_candles.items() if u == user and book)
    last_seen = float(status.get("last_seen") or 0.0)
    age = max(0.0, now - last_seen) if last_seen else None
    status["age_seconds"] = round(age, 2) if age is not None else None
    status["connected"] = bool(last_seen and age <= 20.0)
    return status


def get_quotex_bridge_market_data(user, pair):
    """Return the user's exact Quotex bridge 1m OHLC stream as a pandas DataFrame."""
    user = normalize_user_id(user)
    key = _quotex_bridge_pair_key(user, pair)
    with quotex_bridge_data_lock:
        book = quotex_bridge_candles.get(key)
        rows = list(book.items()) if book else []
        status = dict(quotex_bridge_status.get(user) or {})
    source_info = {
        "source": "Quotex Bridge",
        "source_mode": "broker_otc_exact",
        "provider_symbol": pair,
        "yahoo_symbol": YAHOO_SYMBOLS.get(pair),
        "backup_used": False,
        "exact_broker_feed": True,
    }
    if not rows:
        source_info["unavailable_reason"] = "Quotex Bridge is not streaming this OTC pair yet. Open the same pair in Quotex and keep the bridge connected."
        return None, None, pair, source_info
    last_seen = float(status.get("last_seen") or 0.0)
    age = max(0.0, time.time() - last_seen) if last_seen else float("inf")
    if age > 20.0:
        source_info["unavailable_reason"] = f"Quotex Bridge feed is disconnected/stale ({int(age)}s since last tick)."
        return None, age, pair, source_info
    pd = _get_pandas()
    index = pd.to_datetime([epoch for epoch, _row in rows], unit="s", utc=True)
    frame = pd.DataFrame([row for _epoch, row in rows], index=index)
    frame = frame.sort_index()
    return frame, age, pair, source_info

# Permanent license storage:
# - Recommended on Render Free: set DATABASE_URL (for example a Neon/Supabase PostgreSQL URL).
# - If DATABASE_URL is absent, the app falls back to licenses.json. Render Free local files
#   are ephemeral, so the fallback is for local development/testing only.
DATABASE_URL = (os.environ.get("DATABASE_URL") or os.environ.get("RAJA_DATABASE_URL") or "").strip()
LICENSE_STORE_MODE = "postgres" if DATABASE_URL else "file"
DEVICE_SESSION_TTL_SECONDS = max(120, int(os.environ.get("RAJA_DEVICE_SESSION_TTL", "300")))
# User requested a one-time reset of all previously generated keys. This marker makes
# the reset run only once per persistent database. Change/empty the env var to control it.
LICENSE_RESET_VERSION = os.environ.get("RAJA_LICENSE_RESET_VERSION", "").strip()

SIGNALS_FILE = DATA_DIR / "signals.json"
signals_lock = threading.RLock()
SCAN_EVENTS_FILE = DATA_DIR / "scan_events.json"
scan_events_lock = threading.RLock()

# Free-trial anti-abuse store.
# A user/UID can claim a FREE TRIAL once, and after first login the device
# is also permanently recorded as having used a trial.
TRIAL_CLAIMS_FILE = DATA_DIR / "trial_claims.json"
trial_claims_lock = threading.RLock()

# Shared in-app broadcast + emergency scan-control state.
# PostgreSQL mode persists this inside raja_meta; file mode uses app_control.json.
APP_CONTROL_FILE = DATA_DIR / "app_control.json"
app_control_lock = threading.RLock()

DEFAULT_LICENSE_PLAN = "VIP"
AUTO_TRACK_EXPIRIES = {
    "1m": 60,
    "2m": 120,
    "5m": 300,
    "10m": 600,
    "15m": 900,
    "30m": 1800,
}


def normalize_user_id(value):
    """Canonicalize Telegram/user IDs so @Name and name resolve to the same customer."""
    user = str(value or "").strip()
    if user.startswith("@"):
        user = user[1:].strip()
    return user.casefold()


_license_store_ready = threading.Event()
_license_store_init_lock = threading.Lock()

# Small per-process PostgreSQL keep-alive pool. Reusing an already-authenticated
# connection avoids a fresh TCP/TLS/Postgres handshake on every login/heartbeat.
DB_CONNECT_TIMEOUT_SECONDS = max(3, min(10, int(os.environ.get("RAJA_DB_CONNECT_TIMEOUT", "5"))))
DB_POOL_SIZE = max(1, min(6, int(os.environ.get("RAJA_DB_POOL_SIZE", "3"))))
DB_POOL_IDLE_PING_SECONDS = max(20, min(300, int(os.environ.get("RAJA_DB_POOL_IDLE_PING", "60"))))
_db_pool = queue.LifoQueue(maxsize=DB_POOL_SIZE)


def _raw_db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; install requirements.txt")
    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
        application_name="raja_ai_premium",
    )


def _discard_db_connection(conn):
    try:
        conn.close()
    except Exception:
        pass


def _acquire_db_connection():
    now = time.monotonic()
    while True:
        try:
            conn, last_used = _db_pool.get_nowait()
        except queue.Empty:
            return _raw_db_connect()

        if getattr(conn, "closed", False):
            _discard_db_connection(conn)
            continue

        # Only ping connections that have been idle for a while. This keeps the
        # normal login path to one real license query while safely replacing dead
        # connections after provider/network idle timeouts.
        if now - float(last_used or 0.0) >= DB_POOL_IDLE_PING_SECONDS:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
                conn.rollback()
            except Exception:
                _discard_db_connection(conn)
                continue
        return conn


def _release_db_connection(conn, broken=False):
    if conn is None:
        return
    if broken or getattr(conn, "closed", False):
        _discard_db_connection(conn)
        return
    try:
        _db_pool.put_nowait((conn, time.monotonic()))
    except queue.Full:
        _discard_db_connection(conn)


class _DbLease:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        broken = False
        try:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        except Exception:
            broken = True
        _release_db_connection(self.conn, broken=broken)
        return False


def _prime_db_pool():
    if not DATABASE_URL or _db_pool.qsize() > 0:
        return
    conn = None
    try:
        conn = _raw_db_connect()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.rollback()
        _release_db_connection(conn)
        conn = None
    finally:
        if conn is not None:
            _discard_db_connection(conn)


def _db_connect():
    # Database schema initialization is warmed in a background thread at startup.
    # If a DB-backed request arrives before warmup finishes, perform the same
    # one-time initialization safely, then borrow a warm pooled connection.
    if DATABASE_URL and not _license_store_ready.is_set():
        _ensure_license_store_initialized()
    return _DbLease(_acquire_db_connection())


def initialize_license_store():
    if DATABASE_URL:
        # Persistent license + scan analytics storage.
        with _raw_db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raja_licenses (
                        license_key TEXT PRIMARY KEY,
                        active BOOLEAN NOT NULL DEFAULT TRUE,
                        user_id TEXT,
                        device_id TEXT,
                        device_label TEXT,
                        created_at BIGINT,
                        last_verified_at BIGINT,
                        session_token TEXT,
                        plan TEXT,
                        expires_at BIGINT,
                        last_login_at BIGINT
                    )
                """)
                for statement in [
                    "ALTER TABLE raja_licenses ADD COLUMN IF NOT EXISTS device_label TEXT",
                    "ALTER TABLE raja_licenses ADD COLUMN IF NOT EXISTS session_token TEXT",
                    "ALTER TABLE raja_licenses ADD COLUMN IF NOT EXISTS plan TEXT",
                    "ALTER TABLE raja_licenses ADD COLUMN IF NOT EXISTS expires_at BIGINT",
                    "ALTER TABLE raja_licenses ADD COLUMN IF NOT EXISTS last_login_at BIGINT",
                ]:
                    cur.execute(statement)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raja_meta (
                        meta_key TEXT PRIMARY KEY,
                        meta_value TEXT
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raja_scan_events (
                        event_id BIGSERIAL PRIMARY KEY,
                        created_at BIGINT NOT NULL,
                        user_id TEXT,
                        market TEXT,
                        pair TEXT,
                        mode TEXT,
                        signal_found BOOLEAN NOT NULL DEFAULT FALSE
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raja_signals (
                        signal_id TEXT PRIMARY KEY,
                        user_id TEXT,
                        created_at BIGINT NOT NULL,
                        payload TEXT NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_raja_signals_user_created
                    ON raja_signals(user_id, created_at DESC)
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS raja_trial_claims (
                        claim_type TEXT NOT NULL,
                        claim_value TEXT NOT NULL,
                        license_key TEXT,
                        created_at BIGINT NOT NULL,
                        PRIMARY KEY (claim_type, claim_value)
                    )
                """)

                # Preserve the existing one-time reset marker behavior; do not change the version
                # during ordinary feature upgrades or existing customer keys would be deleted.
                if LICENSE_RESET_VERSION:
                    cur.execute("SELECT meta_value FROM raja_meta WHERE meta_key = %s", ("license_reset_version",))
                    row = cur.fetchone()
                    current_version = str(row[0]) if row and row[0] is not None else ""
                    if current_version != LICENSE_RESET_VERSION:
                        cur.execute("DELETE FROM raja_licenses")
                        cur.execute("""
                            INSERT INTO raja_meta (meta_key, meta_value)
                            VALUES (%s, %s)
                            ON CONFLICT (meta_key) DO UPDATE SET meta_value = EXCLUDED.meta_value
                        """, ("license_reset_version", LICENSE_RESET_VERSION))
                        print(f"RAJA license reset applied once: {LICENSE_RESET_VERSION}")
        print("RAJA license store: PostgreSQL (persistent)")
        return

    with license_lock:
        if not LICENSE_FILE.exists():
            LICENSE_FILE.write_text("{}", encoding="utf-8")
        if not SCAN_EVENTS_FILE.exists():
            SCAN_EVENTS_FILE.write_text("[]", encoding="utf-8")
        if not TRIAL_CLAIMS_FILE.exists():
            TRIAL_CLAIMS_FILE.write_text("{}", encoding="utf-8")
        if LICENSE_RESET_VERSION:
            marker_file = DATA_DIR / ".raja_license_reset_version"
            current_version = marker_file.read_text(encoding="utf-8").strip() if marker_file.exists() else ""
            if current_version != LICENSE_RESET_VERSION:
                LICENSE_FILE.write_text("{}", encoding="utf-8")
                marker_file.write_text(LICENSE_RESET_VERSION, encoding="utf-8")
                print(f"RAJA local license reset applied once: {LICENSE_RESET_VERSION}")
    print("WARNING: RAJA license store is using local file storage (not persistent on Render Free).")


def _ensure_license_store_initialized():
    if _license_store_ready.is_set():
        return
    with _license_store_init_lock:
        if _license_store_ready.is_set():
            return
        initialize_license_store()
        _license_store_ready.set()


def _background_license_store_warmup():
    # Railway/remote PostgreSQL can become reachable a few seconds after the web
    # process itself starts. Retry quietly in the background and keep one DB
    # connection warm so the customer's first login does not pay the handshake cost.
    last_error = None
    for delay in (0, 2, 5):
        if delay:
            time.sleep(delay)
        try:
            _ensure_license_store_initialized()
            _prime_db_pool()
            return
        except Exception as exc:
            last_error = exc
    # Do not prevent Flask/Gunicorn from serving /health and the cached web shell.
    # The first DB-backed request will still retry initialization synchronously.
    print(f"RAJA license store background warmup warning: {last_error}")


def _start_license_store_warmup():
    if DATABASE_URL:
        threading.Thread(
            target=_background_license_store_warmup,
            name="raja-license-store-warmup",
            daemon=True,
        ).start()
    else:
        _ensure_license_store_initialized()


def load_licenses():
    with license_lock:
        if DATABASE_URL:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT license_key, active, user_id, device_id, device_label, created_at,
                               last_verified_at, session_token, plan, expires_at, last_login_at
                        FROM raja_licenses
                    """)
                    rows = cur.fetchall()

            data = {}
            for (key, active, user, device, device_label, created_at, last_verified_at,
                 session_token, plan, expires_at, last_login_at) in rows:
                data[str(key)] = {
                    "active": bool(active),
                    "user": user,
                    "device": device,
                    "device_label": device_label,
                    "created_at": created_at,
                    "last_verified_at": last_verified_at,
                    "session_token": session_token,
                    "plan": plan or DEFAULT_LICENSE_PLAN,
                    "expires_at": expires_at,
                    "last_login_at": last_login_at,
                }
            return data

        if not LICENSE_FILE.exists():
            LICENSE_FILE.write_text("{}", encoding="utf-8")
            return {}
        try:
            data = json.loads(LICENSE_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("Invalid license database")
            return data
        except Exception:
            try:
                backup = LICENSE_FILE.with_name(f"licenses.corrupt.{int(time.time())}.json")
                LICENSE_FILE.replace(backup)
            except Exception:
                pass
            LICENSE_FILE.write_text("{}", encoding="utf-8")
            return {}


def save_licenses(data):
    with license_lock:
        if DATABASE_URL:
            rows = []
            for key, record in (data or {}).items():
                record = record if isinstance(record, dict) else {}
                rows.append((
                    str(key), bool(record.get("active", False)), record.get("user"),
                    record.get("device"), record.get("device_label"), record.get("created_at"),
                    record.get("last_verified_at"), record.get("session_token"),
                    record.get("plan") or DEFAULT_LICENSE_PLAN, record.get("expires_at"),
                    record.get("last_login_at"),
                ))
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM raja_licenses")
                    if rows:
                        cur.executemany("""
                            INSERT INTO raja_licenses
                                (license_key, active, user_id, device_id, device_label, created_at,
                                 last_verified_at, session_token, plan, expires_at, last_login_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, rows)
            return
        temp = LICENSE_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        temp.replace(LICENSE_FILE)


def load_license_record(key):
    """Load one license without scanning the full PostgreSQL table."""
    key = str(key or "").strip()
    if not key:
        return None

    if DATABASE_URL:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT active, user_id, device_id, device_label, created_at,
                           last_verified_at, session_token, plan, expires_at, last_login_at
                    FROM raja_licenses
                    WHERE license_key = %s
                    LIMIT 1
                """, (key,))
                row = cur.fetchone()
        if not row:
            return None
        (active, user, device, device_label, created_at, last_verified_at,
         session_token, plan, expires_at, last_login_at) = row
        return {
            "active": bool(active),
            "user": user,
            "device": device,
            "device_label": device_label,
            "created_at": created_at,
            "last_verified_at": last_verified_at,
            "session_token": session_token,
            "plan": plan or DEFAULT_LICENSE_PLAN,
            "expires_at": expires_at,
            "last_login_at": last_login_at,
        }

    return load_licenses().get(key)


def save_license_record(key, record):
    """Upsert one license record; never DELETE + rebuild the whole license table."""
    key = str(key or "").strip()
    if not key:
        raise ValueError("License key is required")
    record = record if isinstance(record, dict) else {}

    if DATABASE_URL:
        values = (
            key, bool(record.get("active", False)), record.get("user"),
            record.get("device"), record.get("device_label"), record.get("created_at"),
            record.get("last_verified_at"), record.get("session_token"),
            record.get("plan") or DEFAULT_LICENSE_PLAN, record.get("expires_at"),
            record.get("last_login_at"),
        )
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO raja_licenses
                        (license_key, active, user_id, device_id, device_label, created_at,
                         last_verified_at, session_token, plan, expires_at, last_login_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (license_key) DO UPDATE SET
                        active = EXCLUDED.active,
                        user_id = EXCLUDED.user_id,
                        device_id = EXCLUDED.device_id,
                        device_label = EXCLUDED.device_label,
                        created_at = EXCLUDED.created_at,
                        last_verified_at = EXCLUDED.last_verified_at,
                        session_token = EXCLUDED.session_token,
                        plan = EXCLUDED.plan,
                        expires_at = EXCLUDED.expires_at,
                        last_login_at = EXCLUDED.last_login_at
                """, values)
        return

    with license_lock:
        licenses = load_licenses()
        licenses[key] = record
        save_licenses(licenses)



def find_active_license_for_user(user_ref):
    """Find one active, non-expired license for a user without rebuilding the license table."""
    user = normalize_user_id(user_ref)
    if not user:
        return None, None
    now = int(time.time())

    if DATABASE_URL:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT license_key, active, user_id, device_id, device_label, created_at,
                           last_verified_at, session_token, plan, expires_at, last_login_at
                    FROM raja_licenses
                    WHERE lower(user_id) = %s AND active = TRUE
                    ORDER BY created_at DESC NULLS LAST
                    LIMIT 20
                """, (user,))
                rows = cur.fetchall()
        for row in rows:
            key, active, bound_user, device, device_label, created_at, last_verified_at, session_token, plan, expires_at, last_login_at = row
            record = {
                "active": bool(active), "user": bound_user, "device": device,
                "device_label": device_label, "created_at": created_at,
                "last_verified_at": last_verified_at, "session_token": session_token,
                "plan": plan or DEFAULT_LICENSE_PLAN, "expires_at": expires_at,
                "last_login_at": last_login_at,
            }
            if not license_is_expired(record, now):
                return str(key), record
        return None, None

    for key, record in load_licenses().items():
        if (record.get("active", False)
                and normalize_user_id(record.get("user", "")) == user
                and not license_is_expired(record, now)):
            return key, record
    return None, None


def delete_license_record(key):
    key = str(key or "").strip()
    if not key:
        return False
    if DATABASE_URL:
        with license_lock:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM raja_licenses WHERE license_key=%s", (key,))
                    return cur.rowcount > 0
    with license_lock:
        licenses = load_licenses()
        existed = key in licenses
        licenses.pop(key, None)
        save_licenses(licenses)
        return existed


def clear_all_license_records():
    if DATABASE_URL:
        with license_lock:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM raja_licenses")
                    row = cur.fetchone()
                    removed = int(row[0] or 0) if row else 0
                    cur.execute("DELETE FROM raja_licenses")
                    return removed
    with license_lock:
        licenses = load_licenses()
        removed = len(licenses)
        save_licenses({})
        return removed


def reset_all_license_devices():
    """Clear device sessions atomically; return number of rows that were bound."""
    if DATABASE_URL:
        with license_lock:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(*) FROM raja_licenses
                        WHERE device_id IS NOT NULL OR session_token IS NOT NULL
                    """)
                    row = cur.fetchone()
                    updated = int(row[0] or 0) if row else 0
                    cur.execute("""
                        UPDATE raja_licenses
                        SET device_id=NULL, device_label=NULL, last_verified_at=NULL, session_token=NULL
                    """)
                    cur.execute("SELECT COUNT(*) FROM raja_licenses")
                    total_row = cur.fetchone()
                    total = int(total_row[0] or 0) if total_row else 0
                    return updated, total
    with license_lock:
        licenses = load_licenses()
        updated = 0
        for key, record in list(licenses.items()):
            record = record if isinstance(record, dict) else {}
            if record.get("device") or record.get("session_token"):
                updated += 1
            record["device"] = None
            record["device_label"] = None
            record["last_verified_at"] = None
            record["session_token"] = None
            licenses[key] = record
        save_licenses(licenses)
        return updated, len(licenses)


def _trial_claim_key(claim_type, claim_value):
    claim_type = str(claim_type or "").strip().lower()
    claim_value = str(claim_value or "").strip()
    if claim_type == "user":
        claim_value = normalize_user_id(claim_value)
    return claim_type, claim_value


def get_trial_claim(claim_type, claim_value):
    """Return the previous FREE TRIAL claim, if any."""
    claim_type, claim_value = _trial_claim_key(claim_type, claim_value)
    if not claim_type or not claim_value:
        return None

    if DATABASE_URL:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT license_key, created_at FROM raja_trial_claims "
                        "WHERE claim_type=%s AND claim_value=%s",
                        (claim_type, claim_value),
                    )
                    row = cur.fetchone()
            if row:
                return {"license_key": row[0], "created_at": int(row[1] or 0)}
            return None
        except Exception as exc:
            print(f"Trial claim DB read warning: {exc}")

    with trial_claims_lock:
        if not TRIAL_CLAIMS_FILE.exists():
            TRIAL_CLAIMS_FILE.write_text("{}", encoding="utf-8")
        try:
            data = json.loads(TRIAL_CLAIMS_FILE.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        item = data.get(f"{claim_type}:{claim_value}")
        return item if isinstance(item, dict) else None


def record_trial_claim(claim_type, claim_value, license_key):
    """Permanently record that a user/UID or device has consumed a FREE TRIAL."""
    claim_type, claim_value = _trial_claim_key(claim_type, claim_value)
    if not claim_type or not claim_value:
        return False
    license_key = str(license_key or "").strip()
    now = int(time.time())

    if DATABASE_URL:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO raja_trial_claims(claim_type, claim_value, license_key, created_at)
                        VALUES(%s,%s,%s,%s)
                        ON CONFLICT(claim_type, claim_value) DO NOTHING
                        """,
                        (claim_type, claim_value, license_key, now),
                    )
            return True
        except Exception as exc:
            print(f"Trial claim DB write warning: {exc}")

    with trial_claims_lock:
        if not TRIAL_CLAIMS_FILE.exists():
            TRIAL_CLAIMS_FILE.write_text("{}", encoding="utf-8")
        try:
            data = json.loads(TRIAL_CLAIMS_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                data = {}
        except Exception:
            data = {}
        key = f"{claim_type}:{claim_value}"
        if key not in data:
            data[key] = {"license_key": license_key, "created_at": now}
            temp = TRIAL_CLAIMS_FILE.with_suffix(".tmp")
            temp.write_text(json.dumps(data, indent=2), encoding="utf-8")
            temp.replace(TRIAL_CLAIMS_FILE)
    return True


def license_is_expired(record, now=None):
    expires_at = int((record or {}).get("expires_at") or 0)
    return bool(expires_at and expires_at <= int(now or time.time()))


def default_app_control_state():
    return {
        "maintenance": False,
        "maintenance_message": "RAJA AI scanning is temporarily paused for maintenance.",
        "broadcast": {
            "active": False,
            "id": "",
            "message": "",
            "level": "info",
            "created_at": 0,
        },
        "updated_at": 0,
    }


def normalize_app_control_state(raw):
    state = default_app_control_state()
    if isinstance(raw, dict):
        state["maintenance"] = bool(raw.get("maintenance", False))
        state["maintenance_message"] = str(
            raw.get("maintenance_message") or state["maintenance_message"]
        )[:300]
        state["updated_at"] = int(raw.get("updated_at") or 0)
        broadcast = raw.get("broadcast") if isinstance(raw.get("broadcast"), dict) else {}
        level = str(broadcast.get("level") or "info").lower()
        if level not in {"info", "warning", "critical"}:
            level = "info"
        state["broadcast"] = {
            "active": bool(broadcast.get("active", False)),
            "id": str(broadcast.get("id") or "")[:80],
            "message": str(broadcast.get("message") or "")[:500],
            "level": level,
            "created_at": int(broadcast.get("created_at") or 0),
        }
    return state


def load_app_control_state():
    if DATABASE_URL:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT meta_value FROM raja_meta WHERE meta_key=%s",
                        ("app_control_state",),
                    )
                    row = cur.fetchone()
            if row and row[0]:
                return normalize_app_control_state(json.loads(str(row[0])))
        except Exception as exc:
            print(f"App control DB read warning: {exc}")
        return default_app_control_state()

    with app_control_lock:
        if not APP_CONTROL_FILE.exists():
            APP_CONTROL_FILE.write_text(
                json.dumps(default_app_control_state(), indent=2), encoding="utf-8"
            )
        try:
            return normalize_app_control_state(
                json.loads(APP_CONTROL_FILE.read_text(encoding="utf-8"))
            )
        except Exception:
            return default_app_control_state()


def save_app_control_state(state):
    state = normalize_app_control_state(state)
    state["updated_at"] = int(time.time())
    payload = json.dumps(state, separators=(",", ":"))

    if DATABASE_URL:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raja_meta(meta_key, meta_value)
                    VALUES(%s,%s)
                    ON CONFLICT(meta_key)
                    DO UPDATE SET meta_value=EXCLUDED.meta_value
                    """,
                    ("app_control_state", payload),
                )
        return state

    with app_control_lock:
        temp = APP_CONTROL_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        temp.replace(APP_CONTROL_FILE)
    return state


def scan_maintenance_state():
    state = load_app_control_state()
    return state if state.get("maintenance") else None


def _load_scan_events(limit=5000):
    limit = max(1, min(int(limit), 20000))
    if DATABASE_URL:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT created_at, user_id, market, pair, mode, signal_found
                    FROM raja_scan_events ORDER BY created_at DESC LIMIT %s
                """, (limit,))
                rows = cur.fetchall()
        return [
            {"created_at": int(ts), "user": user, "market": market, "pair": pair,
             "mode": mode, "signal_found": bool(found)}
            for ts, user, market, pair, mode, found in rows
        ]
    with scan_events_lock:
        if not SCAN_EVENTS_FILE.exists():
            SCAN_EVENTS_FILE.write_text("[]", encoding="utf-8")
        try:
            items = json.loads(SCAN_EVENTS_FILE.read_text(encoding="utf-8"))
            return items[:limit] if isinstance(items, list) else []
        except Exception:
            return []


def _append_scan_event(user, market, pair, mode, signal_found=False):
    item = {
        "created_at": int(time.time()), "user": normalize_user_id(user),
        "market": str(market or "Unknown")[:80], "pair": str(pair or "")[:120],
        "mode": str(mode or "BALANCED")[:30], "signal_found": bool(signal_found),
    }
    if DATABASE_URL:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO raja_scan_events(created_at,user_id,market,pair,mode,signal_found)
                        VALUES(%s,%s,%s,%s,%s,%s)
                    """, (item["created_at"], item["user"], item["market"], item["pair"], item["mode"], item["signal_found"]))
            return
        except Exception as exc:
            print(f"Scan analytics DB warning: {exc}")
    with scan_events_lock:
        items = _load_scan_events(5000)
        items.insert(0, item)
        temp = SCAN_EVENTS_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(items[:5000], indent=2), encoding="utf-8")
        temp.replace(SCAN_EVENTS_FILE)


def _auth_session(data):
    data = data or {}
    key = str(data.get("key", "")).strip()
    user = normalize_user_id(data.get("user", ""))
    device = str(data.get("device", "")).strip()
    session_token = str(data.get("session_token", "")).strip()
    if not key or not user or not device or not session_token:
        return None, (jsonify({"status": "error", "message": "Active license session required."}), 401)

    # Fast path: one indexed key lookup instead of loading every license row.
    record = load_license_record(key)
    now = int(time.time())
    if not record or not record.get("active", False) or license_is_expired(record, now):
        return None, (jsonify({"status": "error", "message": "Invalid, expired or revoked license key."}), 401)
    if normalize_user_id(record.get("user", "")) != user:
        return None, (jsonify({"status": "error", "message": "License is assigned to a different user."}), 403)
    if str(record.get("device") or "") != device or str(record.get("session_token") or "") != session_token:
        return None, (jsonify({"status": "error", "message": "This session was replaced by another device. Please login again."}), 409)
    return {"key": key, "user": user, "device": device, "record": record}, None


_start_license_store_warmup()


# =========================================================
# SIGNAL TRACKING
# =========================================================

def load_signals():
    """Load tracked signals from PostgreSQL when available, otherwise local JSON."""
    with signals_lock:
        if DATABASE_URL:
            try:
                with _db_connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT payload FROM raja_signals "
                            "ORDER BY created_at DESC LIMIT 5000"
                        )
                        rows = cur.fetchall()
                items = []
                for row in rows:
                    try:
                        item = json.loads(str(row[0]))
                        if isinstance(item, dict):
                            items.append(item)
                    except Exception:
                        continue
                return items
            except Exception as exc:
                print(f"Signal DB read warning: {exc}")

        if not SIGNALS_FILE.exists():
            SIGNALS_FILE.write_text("[]", encoding="utf-8")
            return []

        try:
            data = json.loads(SIGNALS_FILE.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            return []


def save_signals(items):
    """Persist the current tracked-signal set."""
    with signals_lock:
        clean_items = [x for x in (items or []) if isinstance(x, dict)][:5000]

        if DATABASE_URL:
            try:
                rows = []
                for item in clean_items:
                    signal_id = str(item.get("id") or "").strip()
                    if not signal_id:
                        continue
                    rows.append((
                        signal_id,
                        normalize_user_id(item.get("user", "")),
                        int(item.get("created_at") or 0),
                        json.dumps(item, separators=(",", ":"), ensure_ascii=False),
                    ))
                with _db_connect() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM raja_signals")
                        if rows:
                            cur.executemany(
                                "INSERT INTO raja_signals(signal_id,user_id,created_at,payload) "
                                "VALUES(%s,%s,%s,%s) "
                                "ON CONFLICT (signal_id) DO UPDATE SET "
                                "user_id=EXCLUDED.user_id, "
                                "created_at=EXCLUDED.created_at, "
                                "payload=EXCLUDED.payload",
                                rows,
                            )
                return
            except Exception as exc:
                print(f"Signal DB write warning: {exc}")

        temp = SIGNALS_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(clean_items, indent=2), encoding="utf-8")
        temp.replace(SIGNALS_FILE)


def dataframe_epoch_rows(df):
    rows = []
    if df is None or df.empty:
        return rows

    for index_value, row in df.iterrows():
        try:
            epoch = int(index_value.timestamp())
        except Exception:
            continue
        rows.append((epoch, row))

    return rows


def resolve_tracked_signal(item):
    """Resolve a pending theoretical signal using the same live reference provider when possible."""
    pair = item.get("pair")
    yahoo_symbol = YAHOO_SYMBOLS.get(pair)
    if not yahoo_symbol:
        return False

    preferred_source = str(item.get("source") or "Yahoo Finance")
    data = None
    resolved_source = None

    if preferred_source == "Twelve Data" and TWELVE_DATA_ENABLED:
        data, _ = get_twelve_data_market_data(pair, force=True)
        if data is not None and not data.empty:
            resolved_source = "Twelve Data"

    if data is None or getattr(data, "empty", True):
        update_symbol_cache(yahoo_symbol, force=True)
        with cache_lock:
            cached = market_cache.get(yahoo_symbol)
        if cached:
            data = cached.get("data")
            resolved_source = "Yahoo Finance"

    if (data is None or getattr(data, "empty", True)) and TWELVE_DATA_ENABLED:
        data, _ = get_twelve_data_market_data(pair, force=True)
        if data is not None and not data.empty:
            resolved_source = "Twelve Data"

    rows = dataframe_epoch_rows(data)
    if not rows:
        return False

    entry_epoch = int(item.get("entry_epoch", 0))
    expiry_epoch = int(item.get("expiry_epoch", 0))

    entry_candidates = [(epoch, row) for epoch, row in rows if entry_epoch <= epoch < entry_epoch + 120]
    exit_candidates = [
        (epoch, row) for epoch, row in rows
        if max(entry_epoch, expiry_epoch - 120) <= epoch < expiry_epoch
    ]
    if not entry_candidates or not exit_candidates:
        return False

    entry_epoch_actual, entry_row = entry_candidates[0]
    exit_epoch_actual, exit_row = exit_candidates[-1]
    try:
        entry_price = float(entry_row["Open"])
        exit_price = float(exit_row["Close"])
    except Exception:
        return False

    direction = item.get("signal")
    if exit_price == entry_price:
        result = "DRAW"
    elif direction == "CALL":
        result = "WIN" if exit_price > entry_price else "LOSS"
    elif direction == "PUT":
        result = "WIN" if exit_price < entry_price else "LOSS"
    else:
        return False

    item["entry_price"] = round(entry_price, 8)
    item["exit_price"] = round(exit_price, 8)
    item["entry_candle_epoch"] = entry_epoch_actual
    item["exit_candle_epoch"] = exit_epoch_actual
    item["result"] = result
    item["reference_result"] = result
    item["result_reference_source"] = resolved_source or preferred_source

    if resolved_source == "Yahoo Finance":
        item["yahoo_result"] = result
        item["result_source"] = "yahoo_live"
    else:
        item["backup_result"] = result
        item["result_source"] = "twelve_data_live"

    item["status"] = "COMPLETED"
    item["resolved_at"] = int(time.time())
    return True


def tracked_signal_phase(item, now=None):
    """Return the live lifecycle phase used by the browser Signal Flow UI."""
    item = item or {}
    now = int(now or time.time())
    result = str(item.get("result") or "").upper()
    status = str(item.get("status") or "").upper()

    if result in {"WIN", "LOSS", "DRAW"} or status == "COMPLETED":
        return "COMPLETED"
    if status == "AWAITING_QX":
        return "QX_VERIFY"

    entry_epoch = int(item.get("entry_epoch") or 0)
    expiry_epoch = int(item.get("expiry_epoch") or 0)
    if entry_epoch and now < entry_epoch:
        return "WAITING_ENTRY"
    if entry_epoch and now <= entry_epoch + 4:
        return "ENTRY_OPEN"
    if expiry_epoch and now < expiry_epoch:
        return "TRACKING"
    if expiry_epoch:
        return "RESOLVING"
    return "PENDING"


def resolve_due_signals(items, now=None):
    """Resolve all due outcomes once. Returns True when stored data changed."""
    changed = False
    now = int(now or time.time())

    for item in items:
        item_status = str(item.get("status") or "").upper()
        if item_status not in {"PENDING", "AWAITING_QX"}:
            continue
        if item_status == "AWAITING_QX" and (item.get("reference_result") or item.get("yahoo_result") or item.get("backup_result")):
            continue

        expiry_epoch = int(item.get("expiry_epoch") or 0)
        if not expiry_epoch or now < expiry_epoch + 8:
            continue

        pair = str(item.get("pair", ""))

        # Yahoo is not the Quotex OTC price feed. For OTC we calculate a
        # clearly-labelled Yahoo proxy result, but actual WIN/LOSS remains
        # a manual Quotex confirmation.
        if "(OTC)" in pair:
            proxy_item = dict(item)
            if resolve_tracked_signal(proxy_item):
                for field in (
                    "entry_price", "exit_price", "entry_candle_epoch",
                    "exit_candle_epoch", "yahoo_result", "backup_result",
                    "reference_result", "result_reference_source"
                ):
                    if field in proxy_item:
                        item[field] = proxy_item.get(field)
            item["status"] = "AWAITING_QX"
            item["result"] = None
            item["result_source"] = "awaiting_quotex"
            item["resolved_at"] = now
            changed = True
            continue

        if resolve_tracked_signal(item):
            changed = True

    return changed


def signal_outcome_worker():
    while True:
        try:
            with signals_lock:
                items = load_signals()
                if resolve_due_signals(items):
                    save_signals(items)
        except Exception as e:
            print(f"Signal outcome worker error: {e}")

        time.sleep(10)


def signal_stats(items):
    completed = [x for x in items if x.get("status") == "COMPLETED"]
    wins = sum(1 for x in completed if x.get("result") == "WIN")
    losses = sum(1 for x in completed if x.get("result") == "LOSS")
    draws = sum(1 for x in completed if x.get("result") == "DRAW")
    decided = wins + losses

    return {
        "completed": len(completed),
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "observed_win_rate": round((wins / decided) * 100, 2) if decided else None,
    }


# =========================================================
# YAHOO MARKET DATA
# =========================================================

def fetch_yahoo_1m(symbol):
    """Fetch Yahoo Finance 1-minute OHLCV candles."""
    yf = _get_yfinance()
    ticker = yf.Ticker(symbol)
    df = ticker.history(
        period="5d",
        interval="1m",
        auto_adjust=False,
        actions=False,
        timeout=YAHOO_REQUEST_TIMEOUT_SECONDS,
    )

    if df is None or df.empty:
        return None

    required = ["Open", "High", "Low", "Close"]
    if not all(col in df.columns for col in required):
        return None

    df = df.dropna(subset=required)
    if len(df) < 120:
        return None

    return df


def _get_symbol_fetch_lock(symbol):
    with symbol_fetch_locks_guard:
        lock = symbol_fetch_locks.get(symbol)
        if lock is None:
            lock = threading.Lock()
            symbol_fetch_locks[symbol] = lock
        return lock


def _get_batch_key_lock(key):
    with batch_key_locks_guard:
        lock = batch_key_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            batch_key_locks[key] = lock
        return lock


def _pace_yahoo_request():
    global last_yahoo_fetch_started
    with yahoo_pace_lock:
        now = time.time()
        wait_for = YAHOO_MIN_GAP_SECONDS - (now - last_yahoo_fetch_started)
        if wait_for > 0:
            time.sleep(wait_for)
        last_yahoo_fetch_started = time.time()


def _cached_symbol_is_usable(symbol, max_cache_age):
    now = time.time()
    with cache_lock:
        cached = market_cache.get(symbol)
        if cached and (now - float(cached.get("timestamp") or 0)) <= max_cache_age:
            return True
    return False


def _source_candle_age_seconds(df):
    """Return age of the newest Yahoo 1m candle, not age of our local cache fetch."""
    if df is None or getattr(df, "empty", True):
        return None
    try:
        latest = df.index[-1]
        latest_epoch = float(latest.timestamp())
        # Yahoo timestamps are the candle open time. Give the 1m candle its full minute
        # before counting it as stale.
        return max(0.0, time.time() - latest_epoch - 60.0)
    except Exception:
        return None


def update_symbol_cache(symbol, force=False):
    """Refresh one Yahoo symbol with bounded single-flight waits and failure cooldown."""
    now = time.time()

    with cache_lock:
        cached = market_cache.get(symbol)
        if cached and not force and (now - cached["timestamp"]) <= CACHE_DURATION:
            return True

    with failed_symbol_lock:
        blocked_until = failed_symbol_until.get(symbol, 0)

    if not force and blocked_until > now:
        return _cached_symbol_is_usable(symbol, STALE_CACHE_MAX_AGE)

    symbol_lock = _get_symbol_fetch_lock(symbol)
    acquired_symbol = symbol_lock.acquire(timeout=YAHOO_SYMBOL_LOCK_WAIT_SECONDS)
    if not acquired_symbol:
        # Another request is already fetching this exact symbol. Never let a manual
        # single-pair scan hang indefinitely behind it.
        return _cached_symbol_is_usable(symbol, STALE_CACHE_MAX_AGE)

    try:
        now = time.time()

        # Another request may have refreshed this symbol while this caller waited.
        with cache_lock:
            cached = market_cache.get(symbol)
            if cached and not force and (now - cached["timestamp"]) <= CACHE_DURATION:
                return True

        with failed_symbol_lock:
            blocked_until = failed_symbol_until.get(symbol, 0)

        if not force and blocked_until > now:
            return _cached_symbol_is_usable(symbol, STALE_CACHE_MAX_AGE)

        acquired_slot = yahoo_fetch_semaphore.acquire(timeout=YAHOO_SEMAPHORE_WAIT_SECONDS)
        if not acquired_slot:
            return _cached_symbol_is_usable(symbol, STALE_CACHE_MAX_AGE)

        try:
            _pace_yahoo_request()
            df = fetch_yahoo_1m(symbol)
        finally:
            yahoo_fetch_semaphore.release()

        if df is None:
            with failed_symbol_lock:
                failed_symbol_until[symbol] = time.time() + YAHOO_FAILURE_COOLDOWN
            return False

        with cache_lock:
            market_cache[symbol] = {
                "data": df.copy(),
                "timestamp": time.time(),
            }

        with failed_symbol_lock:
            failed_symbol_until.pop(symbol, None)

        return True

    except Exception as e:
        with failed_symbol_lock:
            failed_symbol_until[symbol] = time.time() + YAHOO_FAILURE_COOLDOWN
        print(f"Yahoo fetch error for {symbol}: {e}")
        return False
    finally:
        symbol_lock.release()



def _get_twelve_data_symbol_lock(provider_symbol):
    with twelve_data_symbol_locks_guard:
        lock = twelve_data_symbol_locks.get(provider_symbol)
        if lock is None:
            lock = threading.Lock()
            twelve_data_symbol_locks[provider_symbol] = lock
        return lock


def _pace_twelve_data_request():
    global last_twelve_data_fetch_started
    with twelve_data_pace_lock:
        now = time.time()
        wait_for = TWELVE_DATA_MIN_GAP_SECONDS - (now - last_twelve_data_fetch_started)
        if wait_for > 0:
            time.sleep(wait_for)
        last_twelve_data_fetch_started = time.time()


def _twelve_data_cache_get(provider_symbol, allow_stale=False):
    now = time.time()
    with twelve_data_cache_lock:
        cached = twelve_data_cache.get(provider_symbol)
        if not cached:
            return None
        cache_age = now - float(cached.get("timestamp") or 0)
        if allow_stale or cache_age <= TWELVE_DATA_CACHE_SECONDS:
            return cached.get("data").copy()
    return None


def _twelve_data_mark_failure(provider_symbol, code=None):
    global twelve_data_global_blocked_until
    now = time.time()
    with twelve_data_failed_lock:
        twelve_data_failed_until[provider_symbol] = now + TWELVE_DATA_FAILURE_COOLDOWN

    try:
        code = int(code or 0)
    except Exception:
        code = 0
    if code in {401, 403, 429}:
        with twelve_data_global_lock:
            twelve_data_global_blocked_until = max(
                twelve_data_global_blocked_until,
                now + TWELVE_DATA_GLOBAL_RATE_LIMIT_COOLDOWN,
            )


def _twelve_data_global_allowed():
    with twelve_data_global_lock:
        return time.time() >= float(twelve_data_global_blocked_until or 0.0)


def _parse_twelve_data_frame(payload):
    values = payload.get("values") if isinstance(payload, dict) else None
    if not isinstance(values, list) or not values:
        return None

    pd = _get_pandas()
    df = pd.DataFrame(values)
    required = {"datetime", "open", "high", "low", "close"}
    if not required.issubset(df.columns):
        return None

    df["Datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    for src, dst in (("open", "Open"), ("high", "High"), ("low", "Low"), ("close", "Close")):
        df[dst] = pd.to_numeric(df[src], errors="coerce")

    if "volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    else:
        df["Volume"] = 0.0

    df = (
        df.dropna(subset=["Datetime", "Open", "High", "Low", "Close"])
          .set_index("Datetime")
          .sort_index()
    )
    df = df[~df.index.duplicated(keep="last")]
    if len(df) < 120:
        return None
    return df[["Open", "High", "Low", "Close", "Volume"]]


def fetch_twelve_data_1m(pair):
    """Fetch Twelve Data 1-minute OHLCV backup candles for one RAJA pair."""
    if not TWELVE_DATA_ENABLED:
        return None, None

    provider_symbol = TWELVE_DATA_SYMBOLS.get(pair)
    if not provider_symbol:
        return None, None

    params = {
        "symbol": provider_symbol,
        "interval": "1min",
        "outputsize": TWELVE_DATA_OUTPUTSIZE,
        "timezone": "UTC",
        "apikey": TWELVE_DATA_API_KEY,
    }
    url = TWELVE_DATA_BASE_URL + "?" + urlencode(params)
    req = UrlRequest(url, headers={"User-Agent": "RAJA-AI/1.0", "Accept": "application/json"})

    try:
        with urlopen(req, timeout=TWELVE_DATA_REQUEST_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
    except HTTPError as exc:
        _twelve_data_mark_failure(provider_symbol, getattr(exc, "code", None))
        print(f"Twelve Data HTTP error for {provider_symbol}: {getattr(exc, 'code', 'unknown')}")
        return None, provider_symbol
    except URLError as exc:
        _twelve_data_mark_failure(provider_symbol)
        print(f"Twelve Data network error for {provider_symbol}: {exc.reason}")
        return None, provider_symbol
    except Exception as exc:
        _twelve_data_mark_failure(provider_symbol)
        print(f"Twelve Data fetch error for {provider_symbol}: {exc}")
        return None, provider_symbol

    if not isinstance(payload, dict) or str(payload.get("status") or "").lower() == "error":
        code = payload.get("code") if isinstance(payload, dict) else None
        _twelve_data_mark_failure(provider_symbol, code)
        message = str(payload.get("message") or "API returned an error") if isinstance(payload, dict) else "Invalid response"
        print(f"Twelve Data API error for {provider_symbol}: {message}")
        return None, provider_symbol

    df = _parse_twelve_data_frame(payload)
    if df is None or df.empty:
        _twelve_data_mark_failure(provider_symbol)
        return None, provider_symbol

    return df, provider_symbol


def get_twelve_data_market_data(pair, force=False):
    """Return cached/fresh Twelve Data candles without exposing the API key."""
    if not TWELVE_DATA_ENABLED:
        return None, None

    provider_symbol = TWELVE_DATA_SYMBOLS.get(pair)
    if not provider_symbol:
        return None, None

    cached = _twelve_data_cache_get(provider_symbol, allow_stale=False)
    if cached is not None and not force:
        return cached, provider_symbol

    if not _twelve_data_global_allowed():
        return _twelve_data_cache_get(provider_symbol, allow_stale=True), provider_symbol

    now = time.time()
    with twelve_data_failed_lock:
        blocked_until = float(twelve_data_failed_until.get(provider_symbol, 0.0) or 0.0)
    if blocked_until > now:
        return _twelve_data_cache_get(provider_symbol, allow_stale=True), provider_symbol

    symbol_lock = _get_twelve_data_symbol_lock(provider_symbol)
    acquired = symbol_lock.acquire(timeout=min(12.0, TWELVE_DATA_REQUEST_TIMEOUT_SECONDS + 2.0))
    if not acquired:
        return _twelve_data_cache_get(provider_symbol, allow_stale=True), provider_symbol

    try:
        cached = _twelve_data_cache_get(provider_symbol, allow_stale=False)
        if cached is not None and not force:
            return cached, provider_symbol

        acquired_slot = twelve_data_fetch_semaphore.acquire(timeout=TWELVE_DATA_REQUEST_TIMEOUT_SECONDS + 2.0)
        if not acquired_slot:
            return _twelve_data_cache_get(provider_symbol, allow_stale=True), provider_symbol

        try:
            _pace_twelve_data_request()
            df, provider_symbol = fetch_twelve_data_1m(pair)
        finally:
            twelve_data_fetch_semaphore.release()

        if df is None or df.empty:
            return _twelve_data_cache_get(provider_symbol, allow_stale=True), provider_symbol

        with twelve_data_cache_lock:
            twelve_data_cache[provider_symbol] = {"data": df.copy(), "timestamp": time.time()}
        with twelve_data_failed_lock:
            twelve_data_failed_until.pop(provider_symbol, None)

        return df.copy(), provider_symbol
    finally:
        symbol_lock.release()


def _market_source_info(pair, yahoo_symbol, source="Yahoo Finance", provider_symbol=None):
    source = str(source or "Yahoo Finance")
    is_backup = source == "Twelve Data"
    if is_backup:
        mode = "underlying_proxy_backup" if "(OTC)" in pair else "live_backup_reference"
    else:
        mode = "underlying_proxy" if "(OTC)" in pair else "live_reference"
    return {
        "source": source,
        "source_mode": mode,
        "provider_symbol": provider_symbol or yahoo_symbol,
        "yahoo_symbol": yahoo_symbol,
        "backup_used": bool(is_backup),
    }


def get_market_data(pair, bridge_user=None, broker=None):
    """
    Priority: Yahoo fresh -> Twelve Data fresh -> freshest stale reference.
    Normal scanning still blocks stale data; OTC fallback may use the stale history.
    """
    yahoo_symbol = YAHOO_SYMBOLS.get(pair)
    if not yahoo_symbol:
        return None, None, None, _market_source_info(pair, None)

    broker_name = str(broker or "").strip().casefold()
    if bridge_user and broker_name == "quotex" and "(OTC)" in str(pair):
        bridge_df, bridge_age, bridge_symbol, bridge_info = get_quotex_bridge_market_data(bridge_user, pair)
        if bridge_df is not None and not bridge_df.empty:
            return bridge_df, bridge_age, bridge_symbol, bridge_info
        if RAJA_REQUIRE_QUOTEX_BRIDGE_FOR_OTC:
            return None, bridge_age, bridge_symbol, bridge_info

    yahoo_data = None
    yahoo_age = None
    now = time.time()
    with cache_lock:
        cached = market_cache.get(yahoo_symbol)

    if cached:
        cache_age = now - float(cached.get("timestamp") or 0.0)
        if cache_age <= CACHE_DURATION:
            yahoo_data = cached["data"].copy()
            yahoo_age = _source_candle_age_seconds(yahoo_data)

    if yahoo_data is None:
        refreshed = update_symbol_cache(yahoo_symbol)
        with cache_lock:
            cached = market_cache.get(yahoo_symbol)
        if cached:
            cache_age = time.time() - float(cached.get("timestamp") or 0.0)
            if refreshed or cache_age <= STALE_CACHE_MAX_AGE:
                yahoo_data = cached["data"].copy()
                yahoo_age = _source_candle_age_seconds(yahoo_data)

    if yahoo_data is not None and not yahoo_data.empty:
        if yahoo_age is None or yahoo_age <= MAX_SOURCE_CANDLE_AGE_SECONDS:
            return yahoo_data, yahoo_age, yahoo_symbol, _market_source_info(
                pair, yahoo_symbol, "Yahoo Finance", yahoo_symbol
            )

    td_data = None
    td_age = None
    td_symbol = TWELVE_DATA_SYMBOLS.get(pair)
    if TWELVE_DATA_ENABLED and td_symbol:
        td_data, td_symbol = get_twelve_data_market_data(pair)
        if td_data is not None and not td_data.empty:
            td_age = _source_candle_age_seconds(td_data)
            if td_age is None or td_age <= MAX_SOURCE_CANDLE_AGE_SECONDS:
                return td_data, td_age, yahoo_symbol, _market_source_info(
                    pair, yahoo_symbol, "Twelve Data", td_symbol
                )

    candidates = []
    if yahoo_data is not None and not yahoo_data.empty:
        candidates.append((
            float(yahoo_age) if yahoo_age is not None else float("inf"),
            yahoo_data, yahoo_age,
            _market_source_info(pair, yahoo_symbol, "Yahoo Finance", yahoo_symbol),
        ))
    if td_data is not None and not td_data.empty:
        candidates.append((
            float(td_age) if td_age is not None else float("inf"),
            td_data, td_age,
            _market_source_info(pair, yahoo_symbol, "Twelve Data", td_symbol),
        ))

    if candidates:
        _, data, age, source_info = min(candidates, key=lambda item: item[0])
        return data.copy(), age, yahoo_symbol, source_info

    return None, None, yahoo_symbol, _market_source_info(pair, yahoo_symbol)


def background_market_poller():
    """Disabled intentionally: full-market polling caused Yahoo rate limits."""
    return


def build_timeframe(base_df, minutes):
    """Create a CLOSED-candle timeframe from Yahoo 1m OHLCV."""
    if base_df is None or base_df.empty:
        return None

    df = base_df.copy()

    if minutes == 1:
        # Last Yahoo minute may still be forming; analyze only closed candles.
        if len(df) > 1:
            df = df.iloc[:-1]
        return df

    rule = f"{minutes}min"

    agg = {
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
    }

    if "Volume" in df.columns:
        agg["Volume"] = "sum"

    try:
        tf = df.resample(
            rule,
            label="left",
            closed="left",
            origin="start_day",
        ).agg(agg)
    except TypeError:
        # Compatibility fallback for older pandas versions.
        tf = df.resample(
            rule,
            label="left",
            closed="left",
        ).agg(agg)

    tf = tf.dropna(subset=["Open", "High", "Low", "Close"])

    # The last resampled bucket can still be forming.
    if len(tf) > 1:
        tf = tf.iloc[:-1]

    return tf


# =========================================================
# INDICATORS
# =========================================================

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period,
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period,
    ).mean()

    rs = avg_gain / avg_loss.replace(0, 1e-12)
    return 100 - (100 / (1 + rs))


def calculate_ema(series, period):
    return series.ewm(
        span=period,
        adjust=False,
        min_periods=period,
    ).mean()


def calculate_macd(series):
    ema12 = calculate_ema(series, 12)
    ema26 = calculate_ema(series, 26)
    macd = ema12 - ema26
    signal = macd.ewm(
        span=9,
        adjust=False,
        min_periods=9,
    ).mean()
    return macd, signal


def calculate_bollinger_bands(series, period=20, std_dev=2):
    middle = series.rolling(period).mean()
    std = series.rolling(period).std()
    upper = middle + (std_dev * std)
    lower = middle - (std_dev * std)
    return upper, middle, lower



def calculate_stochastic_rsi(rsi_series, period=14, smooth_k=3, smooth_d=3):
    """Stochastic RSI for entry timing. Returns smoothed %K and %D series (0..100)."""
    rsi_low = rsi_series.rolling(period).min()
    rsi_high = rsi_series.rolling(period).max()
    spread = (rsi_high - rsi_low).replace(0, 1e-12)
    raw = ((rsi_series - rsi_low) / spread * 100.0).clip(lower=0.0, upper=100.0)
    k = raw.rolling(smooth_k).mean()
    d = k.rolling(smooth_d).mean()
    return k, d


def detect_rsi_divergence(close, rsi_series, lookback=34, pivot_span=2, min_rsi_delta=2.0):
    """Detect simple closed-candle regular RSI divergence from the latest two swing pivots."""
    n = min(len(close), len(rsi_series), int(lookback))
    if n < 12:
        return "NONE"
    prices = close.iloc[-n:]
    rsis = rsi_series.reindex(prices.index)
    p = [safe_float(v) for v in prices.tolist()]
    r = [safe_float(v) for v in rsis.tolist()]
    lows, highs = [], []
    span = max(1, int(pivot_span))
    for i in range(span, n - span):
        if p[i] is None or r[i] is None:
            continue
        local_p = [x for x in p[i-span:i+span+1] if x is not None]
        if len(local_p) < (span * 2 + 1):
            continue
        if p[i] <= min(local_p):
            lows.append(i)
        if p[i] >= max(local_p):
            highs.append(i)

    bullish = False
    bearish = False
    bull_end = -1
    bear_end = -1
    if len(lows) >= 2:
        a, b = lows[-2], lows[-1]
        if r[a] is not None and r[b] is not None:
            bullish = p[b] < p[a] and r[b] >= r[a] + float(min_rsi_delta)
            if bullish:
                bull_end = b
    if len(highs) >= 2:
        a, b = highs[-2], highs[-1]
        if r[a] is not None and r[b] is not None:
            bearish = p[b] > p[a] and r[b] <= r[a] - float(min_rsi_delta)
            if bearish:
                bear_end = b

    if bullish and bearish:
        return "BULLISH" if bull_end >= bear_end else "BEARISH"
    if bullish:
        return "BULLISH"
    if bearish:
        return "BEARISH"
    return "NONE"


def calculate_support_resistance(df, lookback=42, pivot_span=2):
    """Nearest recent closed-candle pivot support/resistance, excluding the current candle."""
    if df is None or len(df) < 12:
        return None, None
    history = df.iloc[:-1].tail(max(12, int(lookback)))
    if history.empty:
        return None, None
    current_price = safe_float(df["Close"].iloc[-1])
    highs = [safe_float(v) for v in history["High"].tolist()]
    lows = [safe_float(v) for v in history["Low"].tolist()]
    span = max(1, int(pivot_span))
    pivot_highs, pivot_lows = [], []
    for i in range(span, len(history) - span):
        if highs[i] is None or lows[i] is None:
            continue
        hwin = [x for x in highs[i-span:i+span+1] if x is not None]
        lwin = [x for x in lows[i-span:i+span+1] if x is not None]
        if len(hwin) == span * 2 + 1 and highs[i] >= max(hwin):
            pivot_highs.append(highs[i])
        if len(lwin) == span * 2 + 1 and lows[i] <= min(lwin):
            pivot_lows.append(lows[i])

    valid_highs = [x for x in pivot_highs if x is not None]
    valid_lows = [x for x in pivot_lows if x is not None]
    fallback_high = max([x for x in highs if x is not None], default=None)
    fallback_low = min([x for x in lows if x is not None], default=None)

    if current_price is None:
        return fallback_low, fallback_high

    lower_levels = [x for x in valid_lows if x <= current_price]
    upper_levels = [x for x in valid_highs if x >= current_price]
    support = max(lower_levels) if lower_levels else (min(valid_lows) if valid_lows else fallback_low)
    resistance = min(upper_levels) if upper_levels else (max(valid_highs) if valid_highs else fallback_high)
    return support, resistance


def bollinger_signal_context(df, bb_upper, bb_middle, bb_lower, atr):
    """Full Bollinger context: rejection, squeeze, breakout and band expansion."""
    close = df["Close"]
    last = df.iloc[-1]
    price = safe_float(close.iloc[-1])
    prev_close = safe_float(close.iloc[-2])
    upper_now = safe_float(bb_upper.iloc[-1])
    lower_now = safe_float(bb_lower.iloc[-1])
    mid_now = safe_float(bb_middle.iloc[-1])
    upper_prev = safe_float(bb_upper.iloc[-2])
    lower_prev = safe_float(bb_lower.iloc[-2])
    mid_prev = safe_float(bb_middle.iloc[-2])
    if None in (price, prev_close, upper_now, lower_now, mid_now, upper_prev, lower_prev, mid_prev):
        return {
            "bull_points": 0.0, "bear_points": 0.0, "context": "UNAVAILABLE",
            "squeeze": False, "expanding": False,
        }

    width = (bb_upper - bb_lower) / bb_middle.abs().replace(0, 1e-12)
    width_now = safe_float(width.iloc[-1], 0.0)
    width_prev = safe_float(width.iloc[-2], width_now)
    width_avg = safe_float(width.rolling(20).mean().iloc[-1], width_now)
    squeeze = bool(width_avg > 0 and width_now <= width_avg * 0.80)
    expanding = bool(width_now > max(width_prev * 1.03, width_avg * 0.92))

    candle_open = safe_float(last["Open"])
    candle_high = safe_float(last["High"])
    candle_low = safe_float(last["Low"])
    candle_close = safe_float(last["Close"])
    bull_candle = candle_close is not None and candle_open is not None and candle_close > candle_open
    bear_candle = candle_close is not None and candle_open is not None and candle_close < candle_open

    breakout_up = bool(price > upper_now and prev_close <= upper_prev and expanding)
    breakout_down = bool(price < lower_now and prev_close >= lower_prev and expanding)
    lower_rejection = bool(candle_low is not None and candle_low <= lower_now and price > lower_now and bull_candle)
    upper_rejection = bool(candle_high is not None and candle_high >= upper_now and price < upper_now and bear_candle)
    mid_up = bool(price > mid_now and mid_now >= mid_prev)
    mid_down = bool(price < mid_now and mid_now <= mid_prev)

    bull_points = 0.0
    bear_points = 0.0
    context = "NEUTRAL"
    if breakout_up:
        bull_points += 1.20
        context = "BULL BREAKOUT + EXPANSION"
    elif breakout_down:
        bear_points += 1.20
        context = "BEAR BREAKOUT + EXPANSION"
    elif lower_rejection:
        bull_points += 1.00
        context = "LOWER BAND REJECTION"
    elif upper_rejection:
        bear_points += 1.00
        context = "UPPER BAND REJECTION"
    elif expanding and mid_up:
        bull_points += 0.65
        context = "BULL BAND EXPANSION"
    elif expanding and mid_down:
        bear_points += 0.65
        context = "BEAR BAND EXPANSION"
    elif not squeeze and mid_up:
        bull_points += 0.35
        context = "ABOVE RISING MID"
    elif not squeeze and mid_down:
        bear_points += 0.35
        context = "BELOW FALLING MID"
    elif squeeze:
        context = "SQUEEZE / WAIT BREAKOUT"

    return {
        "bull_points": bull_points,
        "bear_points": bear_points,
        "context": context,
        "squeeze": squeeze,
        "expanding": expanding,
        "bandwidth": width_now,
    }


def calculate_true_range(df):
    previous_close = df["Close"].shift(1)
    high_low = df["High"] - df["Low"]
    high_close = (df["High"] - previous_close).abs()
    low_close = (df["Low"] - previous_close).abs()
    return high_low.combine(high_close, max).combine(low_close, max)


def calculate_atr(df, period=14):
    tr = calculate_true_range(df)
    return tr.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period,
    ).mean()


def calculate_adx_components(df, period=14):
    high = df["High"]
    low = df["Low"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = up_move.where(
        (up_move > down_move) & (up_move > 0),
        0.0,
    )
    minus_dm = down_move.where(
        (down_move > up_move) & (down_move > 0),
        0.0,
    )

    tr = calculate_true_range(df)
    atr = tr.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period,
    ).mean()

    plus_di = (
        100
        * plus_dm.ewm(
            alpha=1 / period,
            adjust=False,
            min_periods=period,
        ).mean()
        / atr.replace(0, 1e-12)
    )

    minus_di = (
        100
        * minus_dm.ewm(
            alpha=1 / period,
            adjust=False,
            min_periods=period,
        ).mean()
        / atr.replace(0, 1e-12)
    )

    dx = (
        100
        * (plus_di - minus_di).abs()
        / (plus_di + minus_di).replace(0, 1e-12)
    )

    adx = dx.ewm(
        alpha=1 / period,
        adjust=False,
        min_periods=period,
    ).mean()

    return adx, plus_di, minus_di


def safe_float(value, default=None):
    try:
        value = float(value)
        if value != value:
            return default
        return value
    except Exception:
        return default


def analyze_timeframe(df, timeframe):
    """Analyze one CLOSED timeframe with RAJA HYBRID weighted confluence. No random values."""
    if df is None or df.empty or len(df) < 60:
        return {"timeframe": timeframe, "signal": "NO SIGNAL", "score": 0, "reason": "Insufficient closed candles"}

    required = {"Open", "High", "Low", "Close"}
    if not required.issubset(df.columns):
        return {"timeframe": timeframe, "signal": "NO SIGNAL", "score": 0, "reason": "Missing OHLC columns"}

    df = df.copy().dropna(subset=list(required))
    if len(df) < 60:
        return {"timeframe": timeframe, "signal": "NO SIGNAL", "score": 0, "reason": "Insufficient clean candles"}

    close = df["Close"]
    rsi_series = calculate_rsi(close, 14)
    rsi = safe_float(rsi_series.iloc[-1])
    ema9 = safe_float(calculate_ema(close, 9).iloc[-1])
    ema21 = safe_float(calculate_ema(close, 21).iloc[-1])
    ema50 = safe_float(calculate_ema(close, 50).iloc[-1])

    macd, macd_signal = calculate_macd(close)
    macd_now = safe_float(macd.iloc[-1])
    macd_sig_now = safe_float(macd_signal.iloc[-1])

    bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(close)
    bb_mid = safe_float(bb_middle.iloc[-1])
    bb_upper_now = safe_float(bb_upper.iloc[-1])
    bb_lower_now = safe_float(bb_lower.iloc[-1])

    atr = safe_float(calculate_atr(df, 14).iloc[-1])
    adx, plus_di, minus_di = calculate_adx_components(df, 14)
    adx_now = safe_float(adx.iloc[-1])
    plus_di_now = safe_float(plus_di.iloc[-1])
    minus_di_now = safe_float(minus_di.iloc[-1])

    stoch_k_series, stoch_d_series = calculate_stochastic_rsi(rsi_series, 14, 3, 3)
    stoch_k = safe_float(stoch_k_series.iloc[-1])
    stoch_d = safe_float(stoch_d_series.iloc[-1])
    stoch_k_prev = safe_float(stoch_k_series.iloc[-2])
    stoch_d_prev = safe_float(stoch_d_series.iloc[-2])

    price = safe_float(close.iloc[-1])
    previous_close = safe_float(close.iloc[-2])

    values = [
        rsi, ema9, ema21, ema50, macd_now, macd_sig_now,
        bb_mid, bb_upper_now, bb_lower_now, atr, adx_now,
        plus_di_now, minus_di_now, stoch_k, stoch_d,
        stoch_k_prev, stoch_d_prev, price, previous_close,
    ]
    if any(v is None for v in values) or atr <= 0:
        return {"timeframe": timeframe, "signal": "NO SIGNAL", "score": 0, "reason": "Indicators not ready"}

    ema_bullish = price > ema9 > ema21 > ema50
    ema_bearish = price < ema9 < ema21 < ema50
    macd_bullish = macd_now > macd_sig_now
    macd_bearish = macd_now < macd_sig_now
    adx_bullish = adx_now >= 18 and plus_di_now > minus_di_now
    adx_bearish = adx_now >= 18 and minus_di_now > plus_di_now

    last = df.iloc[-1]
    candle_open = safe_float(last["Open"])
    candle_high = safe_float(last["High"])
    candle_low = safe_float(last["Low"])
    candle_close = safe_float(last["Close"])
    if None in (candle_open, candle_high, candle_low, candle_close):
        return {"timeframe": timeframe, "signal": "NO SIGNAL", "score": 0, "reason": "Latest candle incomplete"}

    candle_range = candle_high - candle_low
    if candle_range <= 0:
        return {"timeframe": timeframe, "signal": "NO SIGNAL", "score": 0, "reason": "Invalid candle range"}

    bullish_candle = candle_close > candle_open
    bearish_candle = candle_close < candle_open
    candle_body = abs(candle_close - candle_open)
    upper_wick = candle_high - max(candle_open, candle_close)
    lower_wick = min(candle_open, candle_close) - candle_low

    # UPGRADE 5: Precision Wick Rejection.
    # A valid rejection wick must be meaningful relative to BOTH the candle body
    # and the full candle range. This removes many weak 25%-wick false positives.
    wick_body_reference = max(candle_body, candle_range * 0.05)
    lower_wick_ratio = lower_wick / candle_range
    upper_wick_ratio = upper_wick / candle_range
    lower_wick_body_multiple = lower_wick / max(wick_body_reference, 1e-12)
    upper_wick_body_multiple = upper_wick / max(wick_body_reference, 1e-12)
    bullish_rejection = bool(
        bullish_candle
        and lower_wick_ratio >= 0.35
        and lower_wick_body_multiple >= 1.50
    )
    bearish_rejection = bool(
        bearish_candle
        and upper_wick_ratio >= 0.35
        and upper_wick_body_multiple >= 1.50
    )

    # UPGRADE 1: Full Bollinger logic replaces the old middle-band-only vote.
    bb_ctx = bollinger_signal_context(df, bb_upper, bb_middle, bb_lower, atr)

    # UPGRADE 2: RSI keeps its normal momentum role and adds regular divergence context.
    rsi_divergence = detect_rsi_divergence(close, rsi_series)

    # UPGRADE 3: Stochastic RSI becomes the fast timing layer; one-candle momentum is only a small bonus.
    stoch_bullish_cross = stoch_k_prev <= stoch_d_prev and stoch_k > stoch_d and stoch_k <= 65
    stoch_bearish_cross = stoch_k_prev >= stoch_d_prev and stoch_k < stoch_d and stoch_k >= 35
    stoch_bullish_timing = stoch_k > stoch_d and stoch_k <= 35
    stoch_bearish_timing = stoch_k < stoch_d and stoch_k >= 65
    momentum_bullish = price > previous_close
    momentum_bearish = price < previous_close

    # UPGRADE 4: Wick rejection is strongest only at an important recent S/R level.
    support, resistance = calculate_support_resistance(df, 42, 2)
    level_tolerance = max(float(atr) * 0.40, abs(float(price)) * 0.00020)
    near_support = support is not None and candle_low <= float(support) + level_tolerance and candle_close >= float(support) - level_tolerance * 0.20
    near_resistance = resistance is not None and candle_high >= float(resistance) - level_tolerance and candle_close <= float(resistance) + level_tolerance * 0.20
    support_rejection = bool(bullish_rejection and near_support)
    resistance_rejection = bool(bearish_rejection and near_resistance)

    # Precision confluence: Wick + S/R is mandatory. Bollinger rejection and
    # EMA50 alignment upgrade the setup instead of allowing a random wick to dominate.
    bb_lower_wick_rejection = bool(
        bullish_rejection and candle_low <= bb_lower_now and candle_close > bb_lower_now
    )
    bb_upper_wick_rejection = bool(
        bearish_rejection and candle_high >= bb_upper_now and candle_close < bb_upper_now
    )
    ema50_call_filter = bool(candle_close >= ema50)
    ema50_put_filter = bool(candle_close <= ema50)

    wick_call_confirmations = int(bool(near_support)) + int(bb_lower_wick_rejection) + int(ema50_call_filter)
    wick_put_confirmations = int(bool(near_resistance)) + int(bb_upper_wick_rejection) + int(ema50_put_filter)

    precision_wick_call = bool(
        support_rejection and (bb_lower_wick_rejection or ema50_call_filter)
    )
    precision_wick_put = bool(
        resistance_rejection and (bb_upper_wick_rejection or ema50_put_filter)
    )
    premium_wick_call = bool(
        support_rejection and bb_lower_wick_rejection and ema50_call_filter
    )
    premium_wick_put = bool(
        resistance_rejection and bb_upper_wick_rejection and ema50_put_filter
    )

    if premium_wick_call:
        wick_context = "PREMIUM CALL · WICK + SUPPORT + LOWER BB + EMA50"
    elif premium_wick_put:
        wick_context = "PREMIUM PUT · WICK + RESISTANCE + UPPER BB + EMA50"
    elif precision_wick_call:
        wick_context = "PRECISION CALL · WICK + SUPPORT + CONFIRMATION"
    elif precision_wick_put:
        wick_context = "PRECISION PUT · WICK + RESISTANCE + CONFIRMATION"
    elif support_rejection:
        wick_context = "SUPPORT WICK REJECTION"
    elif resistance_rejection:
        wick_context = "RESISTANCE WICK REJECTION"
    elif bullish_rejection:
        wick_context = "RAW BULLISH WICK"
    elif bearish_rejection:
        wick_context = "RAW BEARISH WICK"
    else:
        wick_context = "NONE"

    resistance_breakout = bool(resistance is not None and candle_close > float(resistance) + atr * 0.12 and previous_close <= float(resistance) + atr * 0.05)
    support_breakdown = bool(support is not None and candle_close < float(support) - atr * 0.12 and previous_close >= float(support) - atr * 0.05)
    if support_rejection:
        sr_context = "SUPPORT REJECTION"
    elif resistance_rejection:
        sr_context = "RESISTANCE REJECTION"
    elif resistance_breakout:
        sr_context = "RESISTANCE BREAKOUT"
    elif support_breakdown:
        sr_context = "SUPPORT BREAKDOWN"
    elif near_support:
        sr_context = "NEAR SUPPORT"
    elif near_resistance:
        sr_context = "NEAR RESISTANCE"
    else:
        sr_context = "MID-RANGE"

    volume_bullish = False
    volume_bearish = False
    if "Volume" in df.columns:
        volume = df["Volume"].fillna(0)
        current_volume = safe_float(volume.iloc[-1], 0.0)
        avg_volume = safe_float(volume.rolling(20).mean().iloc[-1], 0.0)
        if current_volume > 0 and avg_volume > 0:
            volume_bullish = bullish_candle and current_volume > avg_volume
            volume_bearish = bearish_candle and current_volume > avg_volume

    bullish_points = 0.0
    bearish_points = 0.0

    # RSI normal momentum, less dominant than trend/SR.
    if 52 <= rsi <= 70:
        bullish_points += 0.75
    elif 30 <= rsi <= 48:
        bearish_points += 0.75
    if rsi_divergence == "BULLISH":
        bullish_points += 0.65
    elif rsi_divergence == "BEARISH":
        bearish_points += 0.65

    # Trend core retained.
    if ema_bullish:
        bullish_points += 2.0
    elif ema_bearish:
        bearish_points += 2.0

    if macd_bullish:
        bullish_points += 1.0
    elif macd_bearish:
        bearish_points += 1.0

    # Full Bollinger weighted context.
    bullish_points += float(bb_ctx.get("bull_points") or 0.0)
    bearish_points += float(bb_ctx.get("bear_points") or 0.0)

    if adx_bullish:
        bullish_points += 1.5
    elif adx_bearish:
        bearish_points += 1.5

    # StochRSI timing replaces the old 1.0-point single-candle momentum vote.
    if stoch_bullish_cross:
        bullish_points += 1.0
    elif stoch_bearish_cross:
        bearish_points += 1.0
    elif stoch_bullish_timing:
        bullish_points += 0.70
    elif stoch_bearish_timing:
        bearish_points += 0.70

    # Raw momentum stays only as a tiny tie-breaker.
    if momentum_bullish:
        bullish_points += 0.25
    elif momentum_bearish:
        bearish_points += 0.25

    # Precision wick weighting. The strongest score is reserved for a rejection
    # confirmed by S/R + Bollinger Band + EMA50; a naked wick gets very little weight.
    if premium_wick_call:
        bullish_points += 1.75
    elif premium_wick_put:
        bearish_points += 1.75
    elif precision_wick_call:
        bullish_points += 1.45
    elif precision_wick_put:
        bearish_points += 1.45
    elif support_rejection:
        bullish_points += 1.10
    elif resistance_rejection:
        bearish_points += 1.10
    elif resistance_breakout:
        bullish_points += 1.00
    elif support_breakdown:
        bearish_points += 1.00
    elif bullish_rejection:
        bullish_points += 0.10
    elif bearish_rejection:
        bearish_points += 0.10
    elif bullish_candle:
        bullish_points += 0.15
    elif bearish_candle:
        bearish_points += 0.15

    # Being close to a level is useful context, but not a hard veto.
    if near_support and not support_breakdown:
        bullish_points += 0.20
    if near_resistance and not resistance_breakout:
        bearish_points += 0.20

    if volume_bullish:
        bullish_points += 0.75
    elif volume_bearish:
        bearish_points += 0.75

    difference = abs(bullish_points - bearish_points)
    winning_points = max(bullish_points, bearish_points)

    # Keep the existing balanced per-TF gate; upgrades improve vote quality rather than making it stricter.
    diagnostics = {
        "rsi": round(rsi, 2),
        "adx": round(adx_now, 2),
        "atr": round(atr, 8),
        "stoch_rsi_k": round(stoch_k, 2),
        "stoch_rsi_d": round(stoch_d, 2),
        "rsi_divergence": rsi_divergence,
        "bb_context": str(bb_ctx.get("context") or "NEUTRAL"),
        "bb_squeeze": bool(bb_ctx.get("squeeze")),
        "bb_expanding": bool(bb_ctx.get("expanding")),
        "support": round(float(support), 8) if support is not None else None,
        "resistance": round(float(resistance), 8) if resistance is not None else None,
        "sr_context": sr_context,
        "wick_context": wick_context,
        "bullish_rejection": bullish_rejection,
        "bearish_rejection": bearish_rejection,
        "lower_wick_ratio": round(lower_wick_ratio, 3),
        "upper_wick_ratio": round(upper_wick_ratio, 3),
        "lower_wick_body_multiple": round(lower_wick_body_multiple, 2),
        "upper_wick_body_multiple": round(upper_wick_body_multiple, 2),
        "bb_lower_wick_rejection": bb_lower_wick_rejection,
        "bb_upper_wick_rejection": bb_upper_wick_rejection,
        "ema50_call_filter": ema50_call_filter,
        "ema50_put_filter": ema50_put_filter,
        "wick_call_confirmations": wick_call_confirmations,
        "wick_put_confirmations": wick_put_confirmations,
        "precision_wick_call": precision_wick_call,
        "precision_wick_put": precision_wick_put,
        "premium_wick_call": premium_wick_call,
        "premium_wick_put": premium_wick_put,
        "bullish_points": round(bullish_points, 2),
        "bearish_points": round(bearish_points, 2),
    }
    if difference < 1.5 or winning_points < 3.5:
        return {
            "timeframe": timeframe, "signal": "NO SIGNAL", "score": 0,
            "reason": "Weak/conflicting hybrid timeframe", **diagnostics,
            "closed_candle_epoch": int(df.index[-1].timestamp()) if len(df.index) else None,
        }

    signal = "CALL" if bullish_points > bearish_points else "PUT"
    score = 50 + difference * 6
    if adx_now >= 20:
        score += min(adx_now - 20, 15) * 0.5
    score = max(50, min(95, score))

    return {
        "timeframe": timeframe,
        "signal": signal,
        "score": round(score, 2),
        "price": round(price, 8),
        **diagnostics,
        "closed_candle_epoch": int(df.index[-1].timestamp()) if len(df.index) else None,
    }



def should_suppress_duplicate(pair, signal, timeframe_summary):
    """Suppress same pair+direction when the analyzed TF context has not changed."""
    context = tuple(
        (tf, details.get("signal"), details.get("score"))
        for tf, details in sorted((timeframe_summary or {}).items())
    )
    fingerprint = (pair, signal, context)
    now = time.time()

    with recent_signal_lock:
        existing = recent_signals.get(pair)

        if existing:
            same_signal = existing.get("signal") == signal
            same_context = existing.get("fingerprint") == fingerprint
            still_locked = (now - existing.get("timestamp", 0)) < DUPLICATE_SIGNAL_COOLDOWN

            if same_signal and same_context and still_locked:
                return True

        recent_signals[pair] = {
            "signal": signal,
            "fingerprint": fingerprint,
            "timestamp": now,
        }

    return False


def format_market_data_age(seconds):
    """Human-readable market-data age for UI/status responses."""
    try:
        total = max(0, int(round(float(seconds or 0))))
    except Exception:
        return "--"

    if total < 60:
        return f"{total}s"

    minutes = total // 60
    if minutes < 60:
        return f"{minutes}m"

    hours, rem_minutes = divmod(minutes, 60)
    if hours < 48:
        return f"{hours}h {rem_minutes}m" if rem_minutes else f"{hours}h"

    days, rem_hours = divmod(hours, 24)
    return f"{days}d {rem_hours}h" if rem_hours else f"{days}d"


def market_result_is_countable(result):
    """Stale/unavailable feed attempts are operational checks, not trading scans."""
    if not isinstance(result, dict):
        return False
    if result.get("exclude_from_performance") or result.get("exclude_from_history"):
        return False
    if result.get("source_stale"):
        return False
    if result.get("data_delayed") and result.get("scan_paused"):
        return False
    return True


def batch_results_are_countable(results):
    rows = [row for row in (results or []) if isinstance(row, dict)]
    return any(market_result_is_countable(row) for row in rows)


def no_signal_result(pair, reason, symbol=None, data_age=None, timeframes=None, source_info=None):
    source_info = source_info or _market_source_info(pair, symbol)
    return {
        "pair": pair,
        "score": 0,
        "signal": "NO SIGNAL",
        "reason": reason,
        "rsi": None,
        "adx": None,
        "atr": None,
        "price": None,
        "bullish_points": 0,
        "bearish_points": 0,
        "data_age": round(data_age, 2) if data_age is not None else None,
        "source": source_info.get("source") or "Yahoo Finance",
        "source_mode": source_info.get("source_mode") or ("underlying_proxy" if "(OTC)" in pair else "live_reference"),
        "backup_used": bool(source_info.get("backup_used")),
        "provider_symbol": source_info.get("provider_symbol"),
        "otc_proxy_warning": bool("(OTC)" in pair and source_info.get("source_mode") != "broker_otc_exact"),
        "yahoo_symbol": symbol,
        "timeframe_summary": timeframes or {},
        "timeframes_scanned": list(TIMEFRAMES.keys()),
        "no_trade": True,
        "no_trade_reason": str(reason or "Current conditions did not pass the safety gate."),
        "quality_gate": "BLOCKED",
    }


def serialize_candles(df, limit=28):
    if df is None or df.empty:
        return []
    out = []
    for idx, row in df.tail(max(8, min(int(limit), 60))).iterrows():
        try:
            out.append({
                "t": int(idx.timestamp()),
                "o": round(float(row["Open"]), 8),
                "h": round(float(row["High"]), 8),
                "l": round(float(row["Low"]), 8),
                "c": round(float(row["Close"]), 8),
            })
        except Exception:
            continue
    return out


def market_stability_metrics(price, atr, adx, data_age, agreement_pct):
    try:
        price = abs(float(price or 0))
        atr = abs(float(atr or 0))
        vol_pct = (atr / price * 100.0) if price > 0 else 0.0
    except Exception:
        vol_pct = 0.0
    age = max(0.0, float(data_age or 0))
    adx_val = max(0.0, min(60.0, float(adx or 0)))
    agreement = max(0.0, min(100.0, float(agreement_pct or 0)))
    data_score = max(0.0, 100.0 - min(age, 180.0) / 1.8)
    trend_score = min(100.0, 35.0 + adx_val * 1.15)
    if vol_pct < 0.003:
        vol_score = 58.0
    elif vol_pct <= 0.8:
        vol_score = 96.0 - abs(vol_pct - 0.16) * 18.0
    elif vol_pct <= 1.8:
        vol_score = 78.0 - (vol_pct - 0.8) * 28.0
    else:
        vol_score = max(18.0, 50.0 - (vol_pct - 1.8) * 20.0)
    score = max(0.0, min(100.0, data_score * 0.28 + trend_score * 0.24 + agreement * 0.30 + vol_score * 0.18))
    risk = "LOW" if score >= 78 else ("MEDIUM" if score >= 58 else "HIGH")
    return round(score, 1), risk, round(vol_pct, 5)


def normalize_scan_options(raw):
    raw = raw if isinstance(raw, dict) else {}
    mode = str(raw.get("mode") or "BALANCED").strip().upper()
    presets = {
        "SAFE": {"min_tf": 5, "min_agreement": 80.0, "min_score": 80.0, "vol_min": 0.003, "vol_max": 1.20},
        "BALANCED": {"min_tf": 4, "min_agreement": 66.7, "min_score": 65.0, "vol_min": 0.002, "vol_max": 2.00},
        "AGGRESSIVE": {"min_tf": 4, "min_agreement": 66.7, "min_score": 55.0, "vol_min": 0.0, "vol_max": 3.00},
        "CUSTOM": {"min_tf": 4, "min_agreement": 66.7, "min_score": 65.0, "vol_min": 0.0, "vol_max": 3.00},
    }
    base = dict(presets.get(mode, presets["BALANCED"]))
    if mode == "CUSTOM":
        try: base["min_tf"] = max(4, min(6, int(raw.get("min_tf", base["min_tf"]))))
        except Exception: pass
        try: base["min_agreement"] = max(60.0, min(100.0, float(raw.get("min_agreement", base["min_agreement"]))))
        except Exception: pass
        try: base["min_score"] = max(50.0, min(95.0, float(raw.get("min_score", base["min_score"]))))
        except Exception: pass
        try: base["vol_min"] = max(0.0, min(5.0, float(raw.get("vol_min", base["vol_min"]))))
        except Exception: pass
        try: base["vol_max"] = max(base["vol_min"], min(10.0, float(raw.get("vol_max", base["vol_max"]))))
        except Exception: pass
    base["mode"] = mode if mode in presets else "BALANCED"
    return base


def calculate_live_indicators(pair, selected_expiry=None, scan_options=None, bridge_user=None, broker=None):
    """Scan synchronized timeframes using a SAFE/BALANCED/AGGRESSIVE/CUSTOM gate."""
    opts = normalize_scan_options(scan_options)
    if pair not in YAHOO_SYMBOLS:
        result = no_signal_result(pair, "Pair is not configured in Yahoo mapping.")
        result.update({"scan_mode": opts["mode"], "scan_thresholds": opts})
        return result

    base_df, data_age, symbol, source_info = get_market_data(pair, bridge_user=bridge_user, broker=broker)
    if base_df is None or base_df.empty:
        result = no_signal_result(
            pair,
            source_info.get("unavailable_reason") or "Live reference data is temporarily unavailable. Scan safely paused; retry when the source responds.",
            symbol=symbol,
            data_age=data_age,
            source_info=source_info,
        )
        result.update({
            "scan_mode": opts["mode"],
            "scan_thresholds": opts,
            "data_delayed": True,
            "scan_paused": True,
            "market_status": "UNAVAILABLE",
            "data_status": "UNAVAILABLE",
            "data_age_seconds": round(float(data_age), 2) if data_age is not None else None,
            "data_age_label": format_market_data_age(data_age) if data_age is not None else "--",
            "exclude_from_history": True,
            "exclude_from_performance": True,
            "scan_skip_reason": "market_data_unavailable",
        })
        return result

    if data_age is not None and data_age > MAX_SOURCE_CANDLE_AGE_SECONDS:
        age_label = format_market_data_age(data_age)
        provider_name = str(source_info.get("source") or "reference source")
        result = no_signal_result(
            pair,
            f"BAD MARKET / STALE DATA — latest {provider_name} 1m reference candle is {age_label} old. "
            "Yahoo primary and the configured live backup did not provide a fresh usable candle.",
            symbol=symbol,
            data_age=data_age,
            source_info=source_info,
        )
        result.update({
            "scan_mode": opts["mode"],
            "scan_thresholds": opts,
            "data_delayed": True,
            "source_stale": True,
            "bad_market": True,
            "scan_paused": True,
            "market_status": "BAD",
            "data_status": "STALE",
            "data_age_seconds": round(float(data_age), 2),
            "data_age_label": age_label,
            "exclude_from_history": True,
            "exclude_from_performance": True,
            "scan_skip_reason": "stale_market_data",
        })
        return result

    chart_preview = serialize_candles(base_df, 28)
    results = {}
    for tf_name, minutes in TIMEFRAMES.items():
        tf_df = build_timeframe(base_df, minutes)
        results[tf_name] = analyze_timeframe(tf_df, tf_name)

    call_results = [r for r in results.values() if r.get("signal") == "CALL"]
    put_results = [r for r in results.values() if r.get("signal") == "PUT"]
    valid_count = len(call_results) + len(put_results)
    summary = {
        tf: {
            "signal": r.get("signal"), "score": r.get("score", 0), "rsi": r.get("rsi"),
            "adx": r.get("adx"), "atr": r.get("atr"),
            "stoch_rsi_k": r.get("stoch_rsi_k"), "stoch_rsi_d": r.get("stoch_rsi_d"),
            "rsi_divergence": r.get("rsi_divergence"), "bb_context": r.get("bb_context"),
            "support": r.get("support"), "resistance": r.get("resistance"),
            "sr_context": r.get("sr_context"), "wick_context": r.get("wick_context"),
            "lower_wick_ratio": r.get("lower_wick_ratio"), "upper_wick_ratio": r.get("upper_wick_ratio"),
            "wick_call_confirmations": r.get("wick_call_confirmations", 0),
            "wick_put_confirmations": r.get("wick_put_confirmations", 0),
            "precision_wick_call": bool(r.get("precision_wick_call")),
            "precision_wick_put": bool(r.get("precision_wick_put")),
            "premium_wick_call": bool(r.get("premium_wick_call")),
            "premium_wick_put": bool(r.get("premium_wick_put")),
            "bullish_points": r.get("bullish_points", 0), "bearish_points": r.get("bearish_points", 0),
            "closed_candle_epoch": r.get("closed_candle_epoch")
        }
        for tf, r in results.items()
    }

    def rejected(reason):
        result = no_signal_result(pair, reason, symbol=symbol, data_age=data_age, timeframes=summary, source_info=source_info)
        result.update({"scan_mode": opts["mode"], "scan_thresholds": opts, "chart_preview": chart_preview})
        return result

    if valid_count < opts["min_tf"]:
        return rejected(f"Fewer than {opts['min_tf']} timeframes reached valid confluence for {opts['mode']} mode.")

    if len(call_results) > len(put_results):
        signal, supporters, opponents = "CALL", call_results, put_results
    elif len(put_results) > len(call_results):
        signal, supporters, opponents = "PUT", put_results, call_results
    else:
        return rejected("Multi-timeframe direction is tied.")

    if len(supporters) < opts["min_tf"]:
        return rejected(f"Fewer than {opts['min_tf']} timeframes agree with the final direction.")

    agreement_ratio = len(supporters) / valid_count
    if agreement_ratio * 100.0 < opts["min_agreement"]:
        return rejected(f"Multi-timeframe agreement below {opts['min_agreement']:.1f}% for {opts['mode']} mode.")

    required_tf = EXPIRY_CONFIRMATION_TIMEFRAME.get(str(selected_expiry or "").strip())
    if required_tf:
        required_result = results.get(required_tf) or {}
        required_signal = required_result.get("signal")
        if required_signal != signal:
            return rejected(
                f"Selected expiry {selected_expiry} requires {required_tf} confirmation; "
                f"{required_tf} is {required_signal or 'NO SIGNAL'} while final direction is {signal}."
            )

    avg_support_score = sum(r["score"] for r in supporters) / len(supporters)
    multi_tf_score = max(50, min(95, avg_support_score + ((agreement_ratio - 0.5) * 12)))

    representative = None
    for preferred in ("5m", "2m", "1m", "10m", "15m", "30m"):
        r = results.get(preferred)
        if r and r.get("signal") == signal:
            representative = r
            break
    if representative is None:
        representative = max(supporters, key=lambda x: x.get("score", 0))

    stability_score, risk_level, volatility_pct = market_stability_metrics(
        representative.get("price"), representative.get("atr"), representative.get("adx"),
        data_age, agreement_ratio * 100.0,
    )

    def quality_rejected(reason):
        blocked = rejected(reason)
        blocked.update({
            "market_stability_score": stability_score,
            "market_risk_level": risk_level,
            "volatility_pct": volatility_pct,
            "no_trade": True,
            "no_trade_reason": reason,
            "quality_gate": "BLOCKED",
        })
        return blocked

    # Smart NO TRADE gate: do not force a signal through a weak or high-risk regime.
    if str(risk_level or "").upper() == "HIGH":
        return quality_rejected(
            f"Smart NO TRADE: market risk is HIGH (stability {stability_score:.1f}/100)."
        )
    if float(stability_score or 0) < 55.0:
        return quality_rejected(
            f"Smart NO TRADE: market stability {stability_score:.1f}/100 is below the 55/100 safety floor."
        )
    if volatility_pct < opts["vol_min"] or volatility_pct > opts["vol_max"]:
        return quality_rejected(
            f"Volatility {volatility_pct:.4f}% is outside {opts['mode']} range "
            f"({opts['vol_min']:.4f}%–{opts['vol_max']:.2f}%)."
        )
    if multi_tf_score < opts["min_score"]:
        return quality_rejected(
            f"Technical confluence {multi_tf_score:.1f}% is below {opts['mode']} threshold {opts['min_score']:.1f}%."
        )

    # Deep quality ranking (ranking only, NOT an extra rejection gate).
    # This keeps BALANCED signal frequency while comparing every qualified pair more carefully.
    selected_result = results.get(required_tf) if required_tf else None
    if not selected_result or selected_result.get("signal") != signal:
        selected_result = representative
    selected_tf_score = float(selected_result.get("score") or multi_tf_score)

    # Neighbouring timeframes add context quality but do not veto an otherwise valid setup.
    context_map = {
        "1m": ("1m", "2m", "5m"),
        "2m": ("1m", "2m", "5m"),
        "5m": ("2m", "5m", "10m"),
        "10m": ("5m", "10m", "15m"),
        "15m": ("10m", "15m", "30m"),
        "30m": ("15m", "30m"),
    }
    context_tfs = context_map.get(required_tf or "", tuple(TIMEFRAMES.keys()))
    context_rows = [results.get(tf) or {} for tf in context_tfs]
    context_valid = [r for r in context_rows if r.get("signal") in {"CALL", "PUT"}]
    context_same = sum(1 for r in context_valid if r.get("signal") == signal)
    context_alignment_pct = (context_same / len(context_valid) * 100.0) if context_valid else agreement_ratio * 100.0

    supporter_scores = [float(r.get("score") or 0.0) for r in supporters]
    supporter_score_avg = (sum(supporter_scores) / len(supporter_scores)) if supporter_scores else multi_tf_score
    supporter_score_spread = (max(supporter_scores) - min(supporter_scores)) if supporter_scores else 0.0
    consistency_score = max(50.0, min(100.0, 100.0 - supporter_score_spread * 2.5))

    supporter_adx = [float(r.get("adx") or 0.0) for r in supporters if r.get("adx") is not None]
    avg_support_adx = (sum(supporter_adx) / len(supporter_adx)) if supporter_adx else float(representative.get("adx") or 0.0)
    trend_quality = max(45.0, min(100.0, 45.0 + max(0.0, avg_support_adx - 15.0) * 2.2))

    rep_bull = float(representative.get("bullish_points") or 0.0)
    rep_bear = float(representative.get("bearish_points") or 0.0)
    directional_edge = abs(rep_bull - rep_bear)
    edge_quality = max(50.0, min(100.0, 50.0 + directional_edge * 8.0))

    deep_quality_score = (
        float(multi_tf_score) * 0.32
        + float(agreement_ratio * 100.0) * 0.18
        + float(stability_score) * 0.16
        + float(selected_tf_score) * 0.12
        + float(context_alignment_pct) * 0.09
        + float(trend_quality) * 0.06
        + float(consistency_score) * 0.04
        + float(edge_quality) * 0.03
    )
    deep_quality_score = max(0.0, min(100.0, deep_quality_score))

    aligned_tfs = [r["timeframe"] for r in supporters]
    opposing_tfs = [r["timeframe"] for r in opponents]
    return {
        "pair": pair, "score": round(multi_tf_score, 2), "signal": signal,
        "reason": f"{opts['mode']} · Multi-TF agreement: {len(supporters)}/{valid_count} valid timeframes -> {signal}",
        "rsi": representative.get("rsi"), "adx": representative.get("adx"), "atr": representative.get("atr"),
        "stoch_rsi_k": representative.get("stoch_rsi_k"), "stoch_rsi_d": representative.get("stoch_rsi_d"),
        "rsi_divergence": representative.get("rsi_divergence"), "bb_context": representative.get("bb_context"),
        "support": representative.get("support"), "resistance": representative.get("resistance"),
        "sr_context": representative.get("sr_context"),
        "wick_context": representative.get("wick_context"),
        "wick_call_confirmations": representative.get("wick_call_confirmations", 0),
        "wick_put_confirmations": representative.get("wick_put_confirmations", 0),
        "precision_wick_call": bool(representative.get("precision_wick_call")),
        "precision_wick_put": bool(representative.get("precision_wick_put")),
        "premium_wick_call": bool(representative.get("premium_wick_call")),
        "premium_wick_put": bool(representative.get("premium_wick_put")),
        "hybrid_engine": "RAJA_HYBRID_97_WEIGHTED_WICK_V2",
        "price": representative.get("price"), "bullish_points": representative.get("bullish_points", 0),
        "bearish_points": representative.get("bearish_points", 0),
        "data_age": round(data_age, 2) if data_age is not None else None,
        "source": source_info.get("source") or "Yahoo Finance",
        "source_mode": source_info.get("source_mode") or ("underlying_proxy" if "(OTC)" in pair else "live_reference"),
        "backup_used": bool(source_info.get("backup_used")),
        "provider_symbol": source_info.get("provider_symbol"),
        "otc_proxy_warning": bool("(OTC)" in pair and source_info.get("source_mode") != "broker_otc_exact"), "yahoo_symbol": symbol,
        "timeframes_scanned": list(TIMEFRAMES.keys()), "aligned_timeframes": aligned_tfs,
        "opposing_timeframes": opposing_tfs, "timeframe_summary": summary,
        "multi_tf_agreement": round(agreement_ratio * 100, 1), "selected_expiry": selected_expiry,
        "required_expiry_timeframe": required_tf,
        "confirmation_mode": f"{opts['mode']} · {opts['min_tf']}-of-6 + {required_tf or 'TF'} Required",
        "duplicate_protection": False, "scan_mode": opts["mode"], "scan_thresholds": opts,
        "market_stability_score": stability_score, "market_risk_level": risk_level,
        "volatility_pct": volatility_pct, "chart_preview": chart_preview,
        "deep_quality_score": round(deep_quality_score, 2),
        "deep_quality_components": {
            "technical": round(float(multi_tf_score), 2),
            "agreement": round(float(agreement_ratio * 100.0), 1),
            "stability": round(float(stability_score), 1),
            "selected_tf": round(float(selected_tf_score), 2),
            "context_alignment": round(float(context_alignment_pct), 1),
            "avg_adx": round(float(avg_support_adx), 2),
            "consistency": round(float(consistency_score), 1),
            "directional_edge": round(float(directional_edge), 2),
        },
        "deep_scan_mode": "FULL_MARKET_QUALIFIED_RANKING",
        "no_trade": False, "quality_gate": "PASSED",
    }


def calculate_forex_otc_fallback_snapshot(pair, selected_expiry=None):
    """
    Build a REFERENCE-ONLY snapshot from the last Yahoo candles even when the
    normal live freshness gate is closed.

    Important:
    - This is NOT live broker OTC data.
    - This function does not write signal history/performance records.
    - Frontend JavaScript applies the separate fallback confidence gate.
    """
    if pair not in FOREX_OTC_PAIRS:
        return {
            "pair": pair,
            "available": False,
            "live_fresh": False,
            "reason": "Pair is not part of the configured Forex OTC list.",
            "source": "Yahoo Finance",
            "source_mode": "fallback_reference_only",
        }

    base_df, data_age, symbol, source_info = get_market_data(pair)
    if base_df is None or base_df.empty:
        return {
            "pair": pair,
            "available": False,
            "live_fresh": False,
            "reason": "No Yahoo/Twelve Data reference history is currently available for this pair.",
            "source": source_info.get("source") or "Yahoo Finance",
            "source_mode": "fallback_reference_only",
            "backup_used": bool(source_info.get("backup_used")),
            "provider_symbol": source_info.get("provider_symbol"),
            "yahoo_symbol": symbol,
            "data_age_seconds": round(float(data_age), 2) if data_age is not None else None,
            "data_age_label": format_market_data_age(data_age) if data_age is not None else "--",
        }

    age_value = float(data_age or 0.0)
    live_fresh = age_value <= float(MAX_SOURCE_CANDLE_AGE_SECONDS)

    results = {}
    for tf_name, minutes in TIMEFRAMES.items():
        tf_df = build_timeframe(base_df, minutes)
        results[tf_name] = analyze_timeframe(tf_df, tf_name)

    summary = {
        tf: {
            "signal": row.get("signal"),
            "score": row.get("score", 0),
            "rsi": row.get("rsi"),
            "adx": row.get("adx"),
            "atr": row.get("atr"),
            "price": row.get("price"),
            "stoch_rsi_k": row.get("stoch_rsi_k"),
            "stoch_rsi_d": row.get("stoch_rsi_d"),
            "rsi_divergence": row.get("rsi_divergence"),
            "bb_context": row.get("bb_context"),
            "support": row.get("support"),
            "resistance": row.get("resistance"),
            "sr_context": row.get("sr_context"),
            "wick_context": row.get("wick_context"),
            "lower_wick_ratio": row.get("lower_wick_ratio"),
            "upper_wick_ratio": row.get("upper_wick_ratio"),
            "wick_call_confirmations": row.get("wick_call_confirmations", 0),
            "wick_put_confirmations": row.get("wick_put_confirmations", 0),
            "precision_wick_call": bool(row.get("precision_wick_call")),
            "precision_wick_put": bool(row.get("precision_wick_put")),
            "premium_wick_call": bool(row.get("premium_wick_call")),
            "premium_wick_put": bool(row.get("premium_wick_put")),
            "bullish_points": row.get("bullish_points", 0),
            "bearish_points": row.get("bearish_points", 0),
            "closed_candle_epoch": row.get("closed_candle_epoch"),
        }
        for tf, row in results.items()
    }

    return {
        "pair": pair,
        "available": True,
        "live_fresh": bool(live_fresh),
        "normal_scan_required": bool(live_fresh),
        "reference_stale": not bool(live_fresh),
        "source": source_info.get("source") or "Yahoo Finance",
        "source_mode": "fallback_reference_only",
        "backup_used": bool(source_info.get("backup_used")),
        "provider_symbol": source_info.get("provider_symbol"),
        "warning": "REFERENCE-BASED FALLBACK · NOT LIVE BROKER OTC DATA",
        "yahoo_symbol": symbol,
        "data_age_seconds": round(age_value, 2),
        "data_age_label": format_market_data_age(age_value),
        "selected_expiry": str(selected_expiry or ""),
        "timeframe_summary": summary,
        "timeframes_scanned": list(TIMEFRAMES.keys()),
        "chart_preview": serialize_candles(base_df, 60),
    }



# =========================================================
# LIVE ECONOMIC NEWS / CALENDAR
# =========================================================

def fetch_forex_factory_calendar():
    """Fetch and normalize Forex Factory's official weekly JSON calendar export."""
    req = UrlRequest(
        FOREX_FACTORY_CALENDAR_URL,
        headers={
            "User-Agent": "RAJA-AI-PREMIUM/2.5 (+economic-calendar)",
            "Accept": "application/json",
        },
    )
    with urlopen(req, timeout=6) as response:
        payload = response.read().decode("utf-8", errors="replace")

    raw_items = json.loads(payload)
    if not isinstance(raw_items, list):
        raise ValueError("Unexpected Forex Factory calendar response")

    normalized = []
    for item in raw_items[:300]:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        country = str(item.get("country", "")).strip().upper()
        date_value = str(item.get("date", "")).strip()
        if not title or not date_value:
            continue
        impact = str(item.get("impact", "Low")).strip().title() or "Low"
        if impact not in {"High", "Medium", "Low", "Holiday"}:
            impact = "Low"
        normalized.append({
            "title": title[:180],
            "currency": country[:8],
            "date": date_value[:40],
            "impact": impact,
            "forecast": str(item.get("forecast", "")).strip()[:40],
            "previous": str(item.get("previous", "")).strip()[:40],
        })

    normalized.sort(key=lambda x: x.get("date", ""))
    return normalized


def _calendar_for_safety_lock():
    now = time.time()
    with market_news_lock:
        cached_data = list(market_news_cache.get("data") or [])
        cached_at = float(market_news_cache.get("timestamp") or 0.0)

    if cached_data and (now - cached_at) <= MARKET_NEWS_CACHE_SECONDS:
        return cached_data

    try:
        fresh = fetch_forex_factory_calendar()
        with market_news_lock:
            market_news_cache["timestamp"] = time.time()
            market_news_cache["data"] = fresh
        return fresh
    except Exception as exc:
        print(f"News safety calendar warning: {exc}")
        return cached_data


def _forex_pair_currencies(pair):
    clean = str(pair or "").replace(" (OTC)", "").strip().upper()
    if "/" in clean:
        left, right = clean.split("/", 1)
        out = {x for x in (left.strip(), right.strip()) if len(x.strip()) == 3}
        return out
    if clean == "XAUUSD":
        return {"USD"}
    return set()


def evaluate_news_safety_lock(pairs, market, before_minutes=15, after_minutes=15):
    # User requested this safety lock for Forex markets only.
    if "FOREX" not in str(market or "").upper():
        return None

    currencies = set()
    for pair in pairs or []:
        currencies.update(_forex_pair_currencies(pair))
    if not currencies:
        return None

    now_ts = time.time()
    candidates = []
    for item in _calendar_for_safety_lock():
        if str(item.get("impact") or "").title() != "High":
            continue
        currency = str(item.get("currency") or "").upper()
        if currency not in currencies:
            continue
        raw_date = str(item.get("date") or "").strip()
        if not raw_date:
            continue
        try:
            parsed = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            event_ts = parsed.timestamp()
        except Exception:
            continue

        if now_ts - (after_minutes * 60) <= event_ts <= now_ts + (before_minutes * 60):
            candidates.append((abs(event_ts - now_ts), event_ts, item))

    if not candidates:
        return None

    _, event_ts, event = min(candidates, key=lambda row: row[0])
    seconds_to_event = int(event_ts - now_ts)
    if seconds_to_event >= 0:
        timing = f"in about {max(1, round(seconds_to_event / 60))} minute(s)"
    else:
        timing = f"about {max(1, round(abs(seconds_to_event) / 60))} minute(s) ago"

    return {
        "active": True,
        "currency": str(event.get("currency") or ""),
        "title": str(event.get("title") or "High-impact economic event"),
        "date": str(event.get("date") or ""),
        "impact": "High",
        "window_before_minutes": int(before_minutes),
        "window_after_minutes": int(after_minutes),
        "reason": (
            f"News Safety Lock: {event.get('currency','')} high-impact event "
            f"“{event.get('title','Economic event')}” is {timing}. "
            f"Forex scans are paused inside the {before_minutes}-minute before / "
            f"{after_minutes}-minute after safety window."
        ),
    }


def news_locked_no_signal(pair, news_lock):
    reason = str((news_lock or {}).get("reason") or "High-impact news safety lock is active.")
    result = no_signal_result(pair, reason, symbol=YAHOO_SYMBOLS.get(pair))
    result.update({
        "news_safety_lock": news_lock,
        "no_trade": True,
        "no_trade_reason": reason,
        "quality_gate": "NEWS_LOCK",
    })
    return result


# =========================================================
# ROUTES
# =========================================================

@app.route("/")
@app.route("/index.html")
def home():
    index_path = BASE_DIR / "index.html"
    if index_path.exists():
        try:
            html = index_path.read_text(encoding="utf-8")
            html = html.replace(APP_BUILD_TOKEN, APP_BUILD_ID)
            response = app.response_class(html, mimetype="text/html")
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
            return response
        except Exception:
            # Safe fallback if the template token cannot be injected for any reason.
            return send_from_directory(str(BASE_DIR), "index.html")
    return (
        "RAJA AI backend is running. "
        "Place index.html in the same folder as bot.py."
    )


@app.route("/app-version", methods=["GET"])
def app_version():
    response = jsonify({
        "status": "success",
        "build": APP_BUILD_ID,
        "update_policy": "auto",
    })
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/manifest.json", methods=["GET"])
def pwa_manifest():
    # Serve a build-versioned manifest so Android/Chrome sees changed icon URLs
    # after every real deployment. Keep app id/start_url untouched so this updates
    # the existing installation instead of creating a second app.
    manifest_path = BASE_DIR / "manifest.json"
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))

        def _version_icons(rows):
            if not isinstance(rows, list):
                return
            for icon in rows:
                if not isinstance(icon, dict):
                    continue
                src = str(icon.get("src") or "").strip()
                if not src:
                    continue
                base = src.split("?", 1)[0]
                icon["src"] = f"{base}?v={APP_BUILD_ID}"

        _version_icons(payload.get("icons"))
        for shortcut in payload.get("shortcuts") or []:
            if isinstance(shortcut, dict):
                _version_icons(shortcut.get("icons"))

        response = jsonify(payload)
        response.mimetype = "application/manifest+json"
    except Exception:
        response = send_from_directory(str(BASE_DIR), "manifest.json", mimetype="application/manifest+json")

    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/sw.js", methods=["GET"])
def pwa_service_worker():
    response = send_from_directory(str(BASE_DIR), "sw.js", mimetype="application/javascript")
    response.headers["Service-Worker-Allowed"] = "/"
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/raja-ai-icon-<size>.png", methods=["GET"])
@app.route("/raja-ai-icon-<size>-v2.png", methods=["GET"])
@app.route("/raja-ai-icon-<size>-v3.png", methods=["GET"])
def pwa_icon(size):
    # Accept current/future versioned icon filenames (for example 192-v3)
    # while validating the actual icon size.
    raw_size = str(size or "").strip()
    base_size = raw_size.split("-", 1)[0]
    if base_size not in {"192", "512"}:
        return jsonify({"status": "error", "message": "Icon not found."}), 404
    requested = request.path.rsplit("/", 1)[-1]
    candidate = BASE_DIR / requested
    if not candidate.exists():
        requested = f"raja-ai-icon-{base_size}.png"
    response = send_from_directory(str(BASE_DIR), requested, mimetype="image/png")
    # PWA icon identity is versioned by filename/build and must never be pinned by HTTP cache.
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/raja-splash-logo.png", methods=["GET"])
def pwa_splash_logo():
    response = send_from_directory(str(BASE_DIR), "raja-splash-logo.png", mimetype="image/png")
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.after_request
def disable_html_cache(response):
    # Fresh app shell/update metadata must never get pinned by Safari/Chrome HTTP cache.
    # User data/localStorage is untouched; only network cache policy is controlled here.
    no_store_paths = {"/", "/index.html", "/sw.js", "/app-version"}
    if request.path in no_store_paths or request.path.endswith(".html"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    elif request.path == "/manifest.json":
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response


@app.route("/otc-fallback-config", methods=["GET"])
def otc_fallback_config():
    auth, error = _auth_session(request.args)
    if error:
        return error

    companion_url = RAJA_QUOTEX_OTC_COMPANION_URL
    bridge = _get_quotex_bridge_status(auth["user"])
    direct_ready = bool(bridge.get("connected"))
    return jsonify({
        "status": "success",
        "data": {
            "enabled": bool(RAJA_QUOTEX_OTC_URL),
            "market": "ForexOTC",
            "launch_url": RAJA_QUOTEX_OTC_URL,
            "companion_connected": direct_ready or bool(companion_url),
            "companion_url": companion_url,
            "direct_scan_available": direct_ready,
            "bridge": bridge,
            "bridge_required_for_quotex_otc": RAJA_REQUIRE_QUOTEX_BRIDGE_FOR_OTC,
            "message": (
                "RAJA Quotex Bridge is connected and streaming broker OTC data."
                if direct_ready
                else "Quotex can be opened, but exact OTC scanning needs the RAJA Quotex Bridge extension."
            ),
        },
    })


@app.route("/quotex-bridge/pair-code", methods=["POST"])
def quotex_bridge_pair_code():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error
    now = time.time()
    with quotex_bridge_pair_codes_lock:
        for old_code, row in list(quotex_bridge_pair_codes.items()):
            if float(row.get("expires_at") or 0) <= now:
                quotex_bridge_pair_codes.pop(old_code, None)
        code = None
        for _ in range(20):
            candidate = f"{secrets.randbelow(900000) + 100000:06d}"
            if candidate not in quotex_bridge_pair_codes:
                code = candidate
                break
        if not code:
            return jsonify({"status": "error", "message": "Could not create a bridge pairing code. Retry."}), 503
        quotex_bridge_pair_codes[code] = {
            "user": auth["user"],
            "device": auth["device"],
            "expires_at": now + QUOTEX_BRIDGE_PAIR_CODE_TTL_SECONDS,
        }
    return jsonify({
        "status": "success",
        "data": {
            "pair_code": code,
            "expires_in_seconds": QUOTEX_BRIDGE_PAIR_CODE_TTL_SECONDS,
            "server_url": request.host_url.rstrip("/"),
            "instructions": "Open the RAJA Quotex Bridge extension and enter this code once."
        }
    })


@app.route("/quotex-bridge/activate", methods=["POST"])
def quotex_bridge_activate():
    data = request.get_json(silent=True) or {}
    code = str(data.get("pair_code") or "").strip()
    if not code:
        return jsonify({"status": "error", "message": "Pairing code is required."}), 400
    now = time.time()
    with quotex_bridge_pair_codes_lock:
        row = quotex_bridge_pair_codes.pop(code, None)
    if not row or float(row.get("expires_at") or 0) <= now:
        return jsonify({"status": "error", "message": "Pairing code is invalid or expired. Generate a new code in RAJA AI."}), 401
    token = _issue_quotex_bridge_token(row["user"], row["device"])
    return jsonify({
        "status": "success",
        "data": {
            "bridge_token": token,
            "expires_at": int(now) + QUOTEX_BRIDGE_TOKEN_TTL_SECONDS,
            "user": row["user"],
        }
    })


@app.route("/quotex-bridge/status", methods=["POST"])
def quotex_bridge_status_route():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error
    pair = str(data.get("pair") or "").strip()
    if pair and pair not in YAHOO_SYMBOLS:
        pair = ""
    return jsonify({"status": "success", "data": _get_quotex_bridge_status(auth["user"], pair or None)})


@app.route("/quotex-bridge/tick", methods=["POST"])
def quotex_bridge_tick():
    data = request.get_json(silent=True) or {}
    token = request.headers.get("X-RAJA-Bridge-Token") or data.get("bridge_token")
    bridge_auth = _validate_quotex_bridge_token(token)
    if not bridge_auth:
        return jsonify({"status": "error", "message": "Bridge token is invalid or expired. Pair the extension again."}), 401
    pair = str(data.get("pair") or "").strip()
    if pair not in YAHOO_SYMBOLS or "(OTC)" not in pair:
        return jsonify({"status": "error", "message": "Unsupported or non-OTC bridge pair."}), 400

    accepted = 0
    candles = data.get("candles")
    if isinstance(candles, list):
        for candle in candles[-500:]:
            accepted += int(_bridge_upsert_candle(bridge_auth["user"], pair, candle))

    price = data.get("price")
    epoch = data.get("timestamp")
    tick_ok = False
    if price is not None:
        tick_ok = _bridge_upsert_tick(bridge_auth["user"], pair, price, epoch)
        accepted += int(tick_ok)

    if not accepted:
        return jsonify({"status": "error", "message": "No valid Quotex price/candle data was supplied."}), 400

    _set_quotex_bridge_status(
        bridge_auth["user"], bridge_auth["device"], pair=pair,
        price=price if tick_ok else None, source_page=data.get("source_page")
    )
    status = _get_quotex_bridge_status(bridge_auth["user"], pair)
    return jsonify({"status": "success", "data": {"accepted": accepted, **status}})


@app.route("/health", methods=["GET"])
def health():
    with cache_lock:
        cached_symbols = len(market_cache)

    return jsonify({
        "status": "success",
        "service": "RAJA AI multi-timeframe backend",
        "app_build": APP_BUILD_ID,
        "license_store_ready": bool(_license_store_ready.is_set()),
        "yahoo_pairs": len(YAHOO_SYMBOLS),
        "unique_yahoo_symbols": len(UNIQUE_YAHOO_SYMBOLS),
        "cached_symbols": cached_symbols,
        "quotex_bridge_enabled": True,
        "quotex_bridge_required_for_otc": RAJA_REQUIRE_QUOTEX_BRIDGE_FOR_OTC,
        "base_interval": "1m",
        "timeframes_scanned": list(TIMEFRAMES.keys()),
        "cache_duration_seconds": CACHE_DURATION,
        "confirmation_mode": "4-of-6 Strong",
        "duplicate_signal_cooldown_seconds": DUPLICATE_SIGNAL_COOLDOWN,
        "background_full_market_poller": False,
        "yahoo_fetch_concurrency": YAHOO_FETCH_CONCURRENCY,
        "yahoo_failure_cooldown_seconds": YAHOO_FAILURE_COOLDOWN,
        "batch_cache_seconds": BATCH_CACHE_DURATION,
        "yahoo_request_timeout_seconds": YAHOO_REQUEST_TIMEOUT_SECONDS,
        "yahoo_symbol_lock_wait_seconds": YAHOO_SYMBOL_LOCK_WAIT_SECONDS,
        "yahoo_semaphore_wait_seconds": YAHOO_SEMAPHORE_WAIT_SECONDS,
        "twelve_data_backup_configured": TWELVE_DATA_ENABLED,
        "twelve_data_outputsize": TWELVE_DATA_OUTPUTSIZE,
        "twelve_data_cache_seconds": TWELVE_DATA_CACHE_SECONDS,
        "twelve_data_fetch_concurrency": TWELVE_DATA_FETCH_CONCURRENCY,
        "twelve_data_request_timeout_seconds": TWELVE_DATA_REQUEST_TIMEOUT_SECONDS,
        "market_data_priority": ["Yahoo Finance", "Twelve Data", "stale reference fallback"],
        "max_source_candle_age_seconds": MAX_SOURCE_CANDLE_AGE_SECONDS,
        "batch_deadline_seconds": BATCH_SCAN_DEADLINE_SECONDS,
        "forex_otc_fallback_deadline_seconds": FOREX_OTC_FALLBACK_DEADLINE_SECONDS,
        "automatic_outcome_tracking": list(AUTO_TRACK_EXPIRIES.keys()),
        "closed_candle_analysis": True,
        "pocket_option_reference_assets": {
            "forex_otc": len(POCKET_OPTION_FOREX_OTC_PAIRS),
            "crypto_otc": len(POCKET_OPTION_CRYPTO_OTC_PAIRS),
            "stocks_otc": len(POCKET_OPTION_STOCKS_OTC_PAIRS),
        },
        "license_store": LICENSE_STORE_MODE,
        "persistent_license_store": bool(DATABASE_URL),
    })


@app.route("/market-news", methods=["GET"])
def market_news():
    now = time.time()
    with market_news_lock:
        cached_data = list(market_news_cache.get("data") or [])
        cached_at = float(market_news_cache.get("timestamp") or 0.0)

    if cached_data and (now - cached_at) <= MARKET_NEWS_CACHE_SECONDS:
        return jsonify({
            "status": "success",
            "source": "Forex Factory weekly calendar export",
            "source_url": "https://www.forexfactory.com/calendar",
            "data": cached_data,
            "cache_hit": True,
            "stale": False,
            "updated_at": int(cached_at),
        })

    try:
        fresh_data = fetch_forex_factory_calendar()
        with market_news_lock:
            market_news_cache["timestamp"] = time.time()
            market_news_cache["data"] = fresh_data
            updated_at = market_news_cache["timestamp"]

        return jsonify({
            "status": "success",
            "source": "Forex Factory weekly calendar export",
            "source_url": "https://www.forexfactory.com/calendar",
            "data": fresh_data,
            "cache_hit": False,
            "stale": False,
            "updated_at": int(updated_at),
        })
    except Exception as exc:
        print(f"Forex Factory calendar fetch error: {exc}")
        # If the upstream feed is briefly unavailable, serve the last successful
        # snapshot instead of leaving the News panel blank.
        if cached_data:
            return jsonify({
                "status": "success",
                "source": "Forex Factory weekly calendar export",
                "source_url": "https://www.forexfactory.com/calendar",
                "data": cached_data,
                "cache_hit": True,
                "stale": True,
                "updated_at": int(cached_at),
                "warning": "Live source temporarily unavailable; showing last cached calendar.",
            })
        return jsonify({
            "status": "error",
            "message": "Live economic calendar is temporarily unavailable.",
        }), 502


@app.route("/app-state", methods=["GET"])
def app_state():
    state = load_app_control_state()
    # Only public control information is exposed here.
    return jsonify({
        "status": "success",
        "data": {
            "maintenance": bool(state.get("maintenance")),
            "maintenance_message": str(state.get("maintenance_message") or ""),
            "broadcast": state.get("broadcast") or {},
            "updated_at": int(state.get("updated_at") or 0),
            "server_time": int(time.time()),
        },
    })


@app.route("/admin/app-control", methods=["POST"])
def admin_app_control():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error

    action = str(data.get("action") or "get").strip().lower()
    state = load_app_control_state()

    if action == "get":
        return jsonify({"status": "success", "data": state})

    if action == "broadcast":
        message = str(data.get("message") or "").strip()[:500]
        if not message:
            return jsonify({"status": "error", "message": "Broadcast message is required."}), 400
        level = str(data.get("level") or "info").lower()
        if level not in {"info", "warning", "critical"}:
            level = "info"
        now = int(time.time())
        state["broadcast"] = {
            "active": True,
            "id": f"broadcast-{now}-{secrets.token_hex(3)}",
            "message": message,
            "level": level,
            "created_at": now,
        }

    elif action == "clear_broadcast":
        state["broadcast"] = default_app_control_state()["broadcast"]

    elif action == "maintenance":
        enabled = bool(data.get("maintenance", False))
        message = str(data.get("maintenance_message") or "").strip()[:300]
        state["maintenance"] = enabled
        if message:
            state["maintenance_message"] = message
        elif enabled:
            state["maintenance_message"] = "RAJA AI scanning is temporarily paused for maintenance."

    else:
        return jsonify({"status": "error", "message": "Unknown app-control action."}), 400

    state = save_app_control_state(state)
    return jsonify({"status": "success", "data": state})


@app.route("/verify-license", methods=["POST"])
def verify_license():
    data = request.get_json(silent=True) or {}
    key = str(data.get("key", "")).strip()
    user = normalize_user_id(data.get("user", ""))
    device = str(data.get("device", "")).strip()
    device_label = str(data.get("device_label", "")).strip()[:160]
    heartbeat = bool(data.get("heartbeat", False))
    supplied_token = str(data.get("session_token", "")).strip()
    if not key or not user or not device:
        return jsonify({"status": "error", "message": "Key, user and device are required."}), 400

    now = int(time.time())

    if DATABASE_URL:
        # Heartbeats/saved-session checks use one atomic UPDATE ... RETURNING query.
        # This removes the previous SELECT + UPDATE round trip and avoids an
        # unnecessary FOR UPDATE lock for the common already-logged-in path.
        if heartbeat:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE raja_licenses
                        SET last_verified_at=%s
                        WHERE license_key=%s
                          AND active=TRUE
                          AND lower(COALESCE(user_id,''))=%s
                          AND device_id=%s
                          AND session_token=%s
                          AND (expires_at IS NULL OR expires_at > %s)
                        RETURNING device_label, session_token, COALESCE(plan,%s), expires_at
                    """, (now, key, user, device, supplied_token, now, DEFAULT_LICENSE_PLAN))
                    row = cur.fetchone()
                    if not row:
                        return jsonify({
                            "status": "error",
                            "message": "This session is invalid, expired, revoked or was replaced by a newer device login."
                        }), 409
                    device_label_db, session_token_db, plan_db, expires_at_db = row
                    return jsonify({
                        "status": "success", "message": "Session active.", "user": user,
                        "device_bound": True, "device_label": device_label_db,
                        "session_token": session_token_db,
                        "plan": plan_db or DEFAULT_LICENSE_PLAN,
                        "expires_at": expires_at_db,
                    })

        # Full login/takeover still uses a row lock so simultaneous device logins
        # cannot issue competing session tokens. It remains one DB transaction.
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT active, user_id, device_id, device_label, created_at,
                           last_verified_at, session_token, plan, expires_at, last_login_at
                    FROM raja_licenses
                    WHERE license_key = %s
                    LIMIT 1
                    FOR UPDATE
                """, (key,))
                row = cur.fetchone()

                if not row:
                    return jsonify({"status": "error", "message": "Invalid or revoked license key."}), 401

                (active, bound_user_raw, previous_device_raw, previous_label_raw, created_at,
                 last_verified_at, stored_token_raw, plan_raw, expires_at, last_login_at) = row
                record = {
                    "active": bool(active),
                    "user": bound_user_raw,
                    "device": previous_device_raw,
                    "device_label": previous_label_raw,
                    "created_at": created_at,
                    "last_verified_at": last_verified_at,
                    "session_token": stored_token_raw,
                    "plan": plan_raw or DEFAULT_LICENSE_PLAN,
                    "expires_at": expires_at,
                    "last_login_at": last_login_at,
                }

                if not record["active"]:
                    return jsonify({"status": "error", "message": "Invalid or revoked license key."}), 401
                if license_is_expired(record, now):
                    return jsonify({"status": "error", "message": "This license has expired. Contact admin to renew access."}), 401

                bound_user = normalize_user_id(record.get("user", ""))
                if bound_user and bound_user != user:
                    return jsonify({"status": "error", "message": "This key is assigned to another user/UID."}), 403

                is_free_trial = str(record.get("plan") or "").strip().upper() == "FREE TRIAL"
                if is_free_trial:
                    cur.execute(
                        "SELECT license_key FROM raja_trial_claims WHERE claim_type=%s AND claim_value=%s",
                        ("device", device),
                    )
                    claim_row = cur.fetchone()
                    if claim_row and str(claim_row[0] or "") != key:
                        return jsonify({
                            "status": "error",
                            "message": "This device has already used a free trial. Contact admin for VIP access."
                        }), 409

                previous_device = str(record.get("device") or "")
                previous_label = str(record.get("device_label") or "")
                new_token = secrets.token_urlsafe(32)
                next_label = device_label or device
                next_plan = record.get("plan") or DEFAULT_LICENSE_PLAN

                cur.execute("""
                    UPDATE raja_licenses
                    SET device_id=%s, device_label=%s, user_id=%s, session_token=%s,
                        last_verified_at=%s, last_login_at=%s, plan=%s
                    WHERE license_key=%s
                """, (device, next_label, user, new_token, now, now, next_plan, key))

                if is_free_trial:
                    cur.execute("""
                        INSERT INTO raja_trial_claims(claim_type, claim_value, license_key, created_at)
                        VALUES(%s,%s,%s,%s)
                        ON CONFLICT(claim_type, claim_value) DO NOTHING
                    """, ("device", device, key, now))

                return jsonify({
                    "status": "success", "message": "License verified successfully.", "user": user,
                    "device_bound": True, "device_label": next_label, "session_token": new_token,
                    "replaced_previous_device": bool(previous_device and previous_device != device),
                    "previous_device_label": previous_label if previous_device and previous_device != device else None,
                    "plan": next_plan, "expires_at": record.get("expires_at"),
                })

    # File-storage fallback keeps the existing behavior for local development/testing.
    record = load_license_record(key)
    if not record or not record.get("active", False):
        return jsonify({"status": "error", "message": "Invalid or revoked license key."}), 401
    if license_is_expired(record, now):
        return jsonify({"status": "error", "message": "This license has expired. Contact admin to renew access."}), 401

    bound_user = normalize_user_id(record.get("user", ""))
    if bound_user and bound_user != user:
        return jsonify({"status": "error", "message": "This key is assigned to another user/UID."}), 403

    is_free_trial = str(record.get("plan") or "").strip().upper() == "FREE TRIAL"
    if is_free_trial and not heartbeat:
        device_claim = get_trial_claim("device", device)
        if device_claim and str(device_claim.get("license_key") or "") != key:
            return jsonify({
                "status": "error",
                "message": "This device has already used a free trial. Contact admin for VIP access."
            }), 409

    if heartbeat:
        if str(record.get("device") or "") != device or not supplied_token or str(record.get("session_token") or "") != supplied_token:
            return jsonify({"status": "error", "message": "This session was replaced by a newer device login."}), 409
        record["last_verified_at"] = now
        save_license_record(key, record)
        return jsonify({
            "status": "success", "message": "Session active.", "user": user,
            "device_bound": True, "device_label": record.get("device_label"),
            "session_token": record.get("session_token"), "plan": record.get("plan") or DEFAULT_LICENSE_PLAN,
            "expires_at": record.get("expires_at"),
        })

    previous_device = str(record.get("device") or "")
    previous_label = str(record.get("device_label") or "")
    new_token = secrets.token_urlsafe(32)
    record["device"] = device
    record["device_label"] = device_label or device
    record["user"] = user
    record["session_token"] = new_token
    record["last_verified_at"] = now
    record["last_login_at"] = now
    record["plan"] = record.get("plan") or DEFAULT_LICENSE_PLAN
    save_license_record(key, record)
    if is_free_trial:
        record_trial_claim("device", device, key)
    return jsonify({
        "status": "success", "message": "License verified successfully.", "user": user,
        "device_bound": True, "device_label": record.get("device_label"), "session_token": new_token,
        "replaced_previous_device": bool(previous_device and previous_device != device),
        "previous_device_label": previous_label if previous_device and previous_device != device else None,
        "plan": record.get("plan"), "expires_at": record.get("expires_at"),
    })


@app.route("/logout-license", methods=["POST"])
def logout_license():
    data = request.get_json(silent=True) or {}
    key = str(data.get("key", "")).strip()
    user = normalize_user_id(data.get("user", ""))
    device = str(data.get("device", "")).strip()
    token = str(data.get("session_token", "")).strip()
    if not key or not user or not device:
        return jsonify({"status": "error", "message": "Key, user and device are required."}), 400

    if DATABASE_URL:
        with _db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE raja_licenses
                    SET device_id=NULL, device_label=NULL, last_verified_at=NULL, session_token=NULL
                    WHERE license_key=%s
                      AND lower(COALESCE(user_id,''))=%s
                      AND device_id=%s
                      AND (session_token IS NULL OR session_token=%s)
                """, (key, user, device, token))
        return jsonify({"status": "success", "message": "Device session released."})

    record = load_license_record(key)
    if not record:
        return jsonify({"status": "success", "message": "Session already cleared."})
    if (normalize_user_id(record.get("user", "")) == user and record.get("device") == device
            and (not record.get("session_token") or record.get("session_token") == token)):
        record["device"] = None
        record["device_label"] = None
        record["last_verified_at"] = None
        record["session_token"] = None
        save_license_record(key, record)
    return jsonify({"status": "success", "message": "Device session released."})


@app.route("/user/profile", methods=["POST"])
def user_profile():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error
    record = auth["record"]
    user = auth["user"]
    now = int(time.time())
    day_start = now - (now % 86400)
    events = _load_scan_events(5000)
    user_events = [e for e in events if normalize_user_id(e.get("user", "")) == user]
    scans_today = sum(1 for e in user_events if int(e.get("created_at") or 0) >= day_start)
    return jsonify({
        "status": "success",
        "data": {
            "user": user, "plan": record.get("plan") or DEFAULT_LICENSE_PLAN,
            "expires_at": record.get("expires_at"), "created_at": record.get("created_at"),
            "last_login_at": record.get("last_login_at"), "last_active_at": record.get("last_verified_at"),
            "device": record.get("device"), "device_label": record.get("device_label"),
            "scans_today": scans_today, "total_scans": len(user_events),
        }
    })


@app.route("/admin/generate-key", methods=["POST"])
def admin_generate_key():
    data = request.get_json(silent=True) or {}
    password = str(data.get("password", ""))
    user = normalize_user_id(data.get("user", ""))
    plan = str(data.get("plan") or DEFAULT_LICENSE_PLAN).strip().upper()[:40]
    try:
        # Supports short trials too:
        # 0.5 day = 12 hours, 1 day = 24 hours.
        duration_days = max(0.0, min(3650.0, float(data.get("duration_days") or 0)))
    except Exception:
        duration_days = 0.0
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    if not user:
        return jsonify({"status": "error", "message": "User Telegram ID / UID is required."}), 400

    is_free_trial = plan == "FREE TRIAL"
    if is_free_trial:
        previous_claim = get_trial_claim("user", user)
        if previous_claim:
            return jsonify({
                "status": "error",
                "message": "Free trial already used for this Telegram ID / UID. Create a VIP license instead."
            }), 409

    while True:
        key = "RAJA-VIP-" + secrets.token_hex(4).upper() + "-2026"
        if load_license_record(key) is None:
            break
    now = int(time.time())
    record = {
        "active": True, "user": user, "device": None, "device_label": None,
        "session_token": None, "created_at": now, "last_verified_at": None,
        "last_login_at": None, "plan": plan, "expires_at": now + int(duration_days * 86400) if duration_days else None,
    }
    save_license_record(key, record)
    if is_free_trial:
        record_trial_claim("user", user, key)
    return jsonify({"status": "success", "message": "License created.", "key": key, "user": user,
                    "plan": plan, "expires_at": record.get("expires_at")})


@app.route("/admin/licenses", methods=["POST"])
def admin_list_licenses():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    now = int(time.time())
    licenses = load_licenses()
    rows = []
    for key, record in licenses.items():
        record = record if isinstance(record, dict) else {}
        last_verified_at = int(record.get("last_verified_at") or 0)
        session_active = bool(record.get("device") and record.get("session_token") and last_verified_at and (now - last_verified_at) < 90)
        rows.append({
            "key": key, "active": bool(record.get("active", False)) and not license_is_expired(record, now),
            "expired": license_is_expired(record, now), "user": record.get("user"), "device": record.get("device"),
            "device_label": record.get("device_label"), "session_active": session_active,
            "created_at": record.get("created_at"), "last_verified_at": record.get("last_verified_at"),
            "last_login_at": record.get("last_login_at"), "plan": record.get("plan") or DEFAULT_LICENSE_PLAN,
            "expires_at": record.get("expires_at"),
        })
    rows.sort(key=lambda x: int(x.get("created_at") or 0), reverse=True)
    bound_count = sum(1 for row in rows if row.get("device"))
    online_count = sum(1 for row in rows if row.get("session_active"))
    return jsonify({"status": "success", "data": rows, "count": len(rows), "summary": {
        "total": len(rows), "active": sum(1 for row in rows if row.get("active")), "bound": bound_count,
        "online": online_count, "unbound": max(0, len(rows) - bound_count),
        "expired": sum(1 for row in rows if row.get("expired")),
    }})


def _validate_admin_password(data):
    # Fail closed: admin routes are disabled unless Render provides a password.
    if not ADMIN_PASSWORD:
        return jsonify({
            "status": "error",
            "message": "Admin access is disabled because RAJA_ADMIN_PASSWORD is not configured."
        }), 503

    supplied = str((data or {}).get("password", ""))
    if not hmac.compare_digest(supplied, ADMIN_PASSWORD):
        return jsonify({"status": "error", "message": "Incorrect admin password."}), 403
    return None


@app.route("/admin/analytics", methods=["POST"])
def admin_analytics():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    now = int(time.time())
    day_start = now - (now % 86400)
    events = [e for e in _load_scan_events(10000) if int(e.get("created_at") or 0) >= day_start]
    user_counts = Counter(normalize_user_id(e.get("user", "")) for e in events if e.get("user"))
    market_counts = Counter(str(e.get("market") or "Unknown") for e in events)
    licenses = load_licenses()
    online = sum(1 for r in licenses.values() if r.get("device") and r.get("session_token") and int(r.get("last_verified_at") or 0) >= now - 90)
    return jsonify({"status": "success", "data": {
        "scans_today": len(events), "signals_today": sum(1 for e in events if e.get("signal_found")),
        "most_active_customer": user_counts.most_common(1)[0][0] if user_counts else "--",
        "most_active_customer_scans": user_counts.most_common(1)[0][1] if user_counts else 0,
        "most_scanned_market": market_counts.most_common(1)[0][0] if market_counts else "--",
        "most_scanned_market_count": market_counts.most_common(1)[0][1] if market_counts else 0,
        "active_licenses": sum(1 for r in licenses.values() if r.get("active") and not license_is_expired(r, now)),
        "online_users": online,
    }})


@app.route("/admin/reset-device", methods=["POST"])
def admin_reset_device():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    key = str(data.get("key", "")).strip()
    if not key:
        return jsonify({"status": "error", "message": "License key is required."}), 400
    record = load_license_record(key)
    if not record:
        return jsonify({"status": "error", "message": "License key not found."}), 404
    record["device"] = None
    record["device_label"] = None
    record["last_verified_at"] = None
    record["session_token"] = None
    save_license_record(key, record)
    return jsonify({"status": "success", "message": "Device binding reset.", "key": key})


@app.route("/admin/reset-all-devices", methods=["POST"])
def admin_reset_all_devices():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    updated, total = reset_all_license_devices()
    return jsonify({"status": "success", "message": "All device bindings cleared.", "updated": updated, "total": total})


def _delete_license_from_request(data):
    key = str(data.get("key", "")).strip()
    auth_error = _validate_admin_password(data)
    if auth_error:
        return None, auth_error
    if not key:
        return None, (jsonify({"status": "error", "message": "License key is required."}), 400)
    if not delete_license_record(key):
        return None, (jsonify({"status": "error", "message": "License key not found."}), 404)
    return key, None


@app.route("/admin/delete-key", methods=["POST"])
def admin_delete_key():
    data = request.get_json(silent=True) or {}; key, error = _delete_license_from_request(data)
    if error: return error
    return jsonify({"status": "success", "message": "License removed permanently.", "key": key})


@app.route("/admin/revoke-key", methods=["POST"])
def admin_revoke_key():
    data = request.get_json(silent=True) or {}; key, error = _delete_license_from_request(data)
    if error: return error
    return jsonify({"status": "success", "message": "License removed permanently.", "key": key})


@app.route("/admin/clear-keys", methods=["POST"])
def admin_clear_keys():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    removed = clear_all_license_records()
    return jsonify({"status": "success", "message": "All license keys removed.", "removed": removed})




@app.route("/track-signal", methods=["POST"])
def track_signal():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error
    pair = str(data.get("pair", "")).strip(); direction = str(data.get("signal", "")).strip().upper()
    expiry = str(data.get("expiry", "")).strip(); score = data.get("score")
    timeframe_summary = data.get("timeframe_summary") or {}; client_id = str(data.get("client_id", "")).strip()
    if pair not in YAHOO_SYMBOLS:
        return jsonify({"status": "error", "message": "Unsupported pair."}), 400
    if direction not in {"CALL", "PUT"}:
        return jsonify({"status": "error", "message": "Signal must be CALL or PUT."}), 400
    if expiry not in AUTO_TRACK_EXPIRIES:
        return jsonify({"status": "success", "auto_tracking": False,
                        "message": "15s/30s outcome tracking is disabled because the Yahoo base feed is 1-minute."})
    now = int(time.time()); duration = AUTO_TRACK_EXPIRIES[expiry]
    entry_epoch = ((now // duration) + 1) * duration; expiry_epoch = entry_epoch + duration
    signal_id = "sig_" + secrets.token_hex(8)
    item = {
        "id": signal_id, "client_id": client_id, "user": auth["user"], "pair": pair, "signal": direction,
        "score": float(score or 0), "expiry": expiry, "created_at": now, "entry_epoch": entry_epoch,
        "expiry_epoch": expiry_epoch, "entry_price": None, "exit_price": None, "result": None,
        "status": "PENDING", "result_source": "pending",
        "source": str(data.get("source") or "Yahoo Finance"),
        "source_mode": str(data.get("source_mode") or ("underlying_proxy" if "(OTC)" in pair else "live_reference")),
        "provider_symbol": data.get("provider_symbol"),
        "timeframe_summary": timeframe_summary, "chart_preview": data.get("chart_preview") or [],
        "market_stability_score": data.get("market_stability_score"), "market_risk_level": data.get("market_risk_level"),
        "volatility_pct": data.get("volatility_pct"), "scan_mode": data.get("scan_mode"),
        "snapshot": data.get("snapshot") or {}, "market": data.get("market"),
    }
    with signals_lock:
        items = load_signals(); items.insert(0, item); save_signals(items[:2000])
    return jsonify({"status": "success", "auto_tracking": True, "signal_id": signal_id,
                    "entry_epoch": entry_epoch, "expiry_epoch": expiry_epoch,
                    "message": f"Signal registered. Enter on the next {expiry} candle open."})


@app.route("/signals/result", methods=["POST"])
def set_signal_result():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error: return error
    signal_id = str(data.get("signal_id", "")).strip(); result = str(data.get("result", "")).strip().upper()
    if not signal_id or result not in {"WIN", "LOSS", "DRAW"}:
        return jsonify({"status": "error", "message": "Valid signal_id and WIN/LOSS/DRAW result are required."}), 400
    with signals_lock:
        items = load_signals(); target = next((x for x in items if x.get("id") == signal_id), None)
        if target is None: return jsonify({"status": "error", "message": "Signal not found."}), 404
        if target.get("user") and normalize_user_id(target.get("user")) != auth["user"]:
            return jsonify({"status": "error", "message": "This signal belongs to another user."}), 403
        if "(OTC)" not in str(target.get("pair", "")):
            return jsonify({"status": "error", "message": "Manual Quotex result is only used for OTC signals."}), 400
        target["result"] = result; target["status"] = "COMPLETED"; target["result_source"] = "quotex_manual"; target["resolved_at"] = int(time.time())
        save_signals(items)
    return jsonify({
        "status": "success",
        "signal_id": signal_id,
        "result": result,
        "result_source": "quotex_manual",
        "yahoo_proxy_result": target.get("yahoo_result"),
        "reference_proxy_result": target.get("reference_result") or target.get("yahoo_result") or target.get("backup_result"),
        "reference_proxy_source": target.get("result_reference_source") or target.get("source"),
    })


@app.route("/signals/history", methods=["GET"])
def signals_history():
    try:
        limit = max(1, min(int(request.args.get("limit", 30)), 100))
    except Exception:
        limit = 30

    auth_data = {k: request.args.get(k, "") for k in ("key", "user", "device", "session_token")}
    auth, error = _auth_session(auth_data)
    if error:
        return error

    # Do one on-demand resolution pass before returning history. This prevents
    # WIN/LOSS from appearing stuck when the background worker has not ticked yet.
    with signals_lock:
        items = load_signals()
        if resolve_due_signals(items):
            save_signals(items)

    user_items = [x for x in items if normalize_user_id(x.get("user", "")) == auth["user"]]
    # Backward compatibility: include old same-device history records that predate user tagging.
    user_items.extend([
        x for x in items
        if not x.get("user")
        and str(x.get("client_id", "")) == auth["device"]
        and x not in user_items
    ])
    user_items.sort(key=lambda x: int(x.get("created_at") or 0), reverse=True)

    now = int(time.time())
    view_items = []
    for raw in user_items[:limit]:
        item = dict(raw)
        item["phase"] = tracked_signal_phase(item, now)
        entry_epoch = int(item.get("entry_epoch") or 0)
        expiry_epoch = int(item.get("expiry_epoch") or 0)
        item["seconds_to_entry"] = max(0, entry_epoch - now) if entry_epoch else None
        item["seconds_to_expiry"] = max(0, expiry_epoch - now) if expiry_epoch else None
        view_items.append(item)

    return jsonify({
        "status": "success",
        "data": view_items,
        "stats": signal_stats(user_items),
        "server_epoch": now,
    })


@app.route("/signals/stats", methods=["GET"])
def signals_stats():
    auth_data = {k: request.args.get(k, "") for k in ("key", "user", "device", "session_token")}
    auth, error = _auth_session(auth_data)
    if error:
        return error

    with signals_lock:
        all_items = load_signals()
        if resolve_due_signals(all_items):
            save_signals(all_items)

    items = [x for x in all_items if normalize_user_id(x.get("user", "")) == auth["user"]]
    return jsonify({"status": "success", "stats": signal_stats(items)})


@app.route("/forex-otc-fallback-data", methods=["POST"])
def forex_otc_fallback_data():
    """
    Authenticated data endpoint used only by the in-app Forex OTC fallback UI.
    Normal RAJA AI scans continue to use /scan and /scan-batch.
    """
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error:
        return error

    maintenance = scan_maintenance_state()
    if maintenance:
        return jsonify({
            "status": "error",
            "maintenance": True,
            "message": maintenance.get("maintenance_message") or "RAJA AI scans are temporarily paused.",
        }), 503

    action = str(data.get("action") or "scan").strip().lower()
    selected_expiry = str(data.get("expiry") or "").strip()

    if action == "status":
        pair = str(data.get("pair") or "").strip()
        if pair not in FOREX_OTC_PAIRS:
            pair = FOREX_OTC_PAIRS[0] if FOREX_OTC_PAIRS else ""
        if not pair:
            return jsonify({"status": "error", "message": "No Forex OTC pairs are configured."}), 400

        snapshot = calculate_forex_otc_fallback_snapshot(pair, selected_expiry)
        return jsonify({
            "status": "success",
            "mode": "forex_otc_reference_status",
            "data": snapshot,
            "max_fresh_age_seconds": MAX_SOURCE_CANDLE_AGE_SECONDS,
        })

    requested = data.get("pairs")
    if not isinstance(requested, list):
        requested = [data.get("pair")] if data.get("pair") else []

    pairs = []
    seen = set()
    for raw in requested[:30]:
        pair = str(raw or "").strip()
        if pair in FOREX_OTC_PAIRS and pair not in seen:
            pairs.append(pair)
            seen.add(pair)

    if not pairs:
        return jsonify({"status": "error", "message": "No supported Forex OTC pairs were supplied."}), 400

    results_by_pair = {}
    workers = min(3, len(pairs))
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="raja-forex-otc-fallback")
    future_map = {
        pool.submit(calculate_forex_otc_fallback_snapshot, pair, selected_expiry): pair
        for pair in pairs
    }

    done, pending = wait(future_map.keys(), timeout=FOREX_OTC_FALLBACK_DEADLINE_SECONDS)
    for future in done:
        pair = future_map[future]
        try:
            results_by_pair[pair] = future.result()
        except Exception as exc:
            print(f"Forex OTC fallback snapshot error for {pair}: {exc}")
            results_by_pair[pair] = {
                "pair": pair,
                "available": False,
                "live_fresh": False,
                "reason": "Fallback reference analysis failed for this pair.",
                "source": "Yahoo Finance",
                "source_mode": "fallback_reference_only",
            }

    for future in pending:
        pair = future_map[future]
        future.cancel()
        results_by_pair[pair] = {
            "pair": pair,
            "available": False,
            "live_fresh": False,
            "reason": "Fallback reference analysis timed out for this pair.",
            "source": "Yahoo Finance",
            "source_mode": "fallback_reference_only",
        }

    pool.shutdown(wait=False, cancel_futures=True)
    rows = [results_by_pair[pair] for pair in pairs]

    return jsonify({
        "status": "success",
        "mode": "forex_otc_reference_fallback",
        "warning": "REFERENCE-BASED FALLBACK · NOT LIVE BROKER OTC DATA",
        "data": rows,
        "live_restored": any(bool(row.get("live_fresh")) for row in rows),
        "max_fresh_age_seconds": MAX_SOURCE_CANDLE_AGE_SECONDS,
    })


@app.route("/scan-batch", methods=["POST"])
def scan_batch():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error: return error

    maintenance = scan_maintenance_state()
    if maintenance:
        return jsonify({
            "status": "error",
            "maintenance": True,
            "message": maintenance.get("maintenance_message") or "RAJA AI scans are temporarily paused.",
        }), 503

    requested_pairs = data.get("pairs") or []; selected_expiry = str(data.get("expiry", "")).strip()
    market = str(data.get("market") or "Unknown")[:80]; broker = str(data.get("broker") or "").strip()[:80]; opts = normalize_scan_options(data.get("scan_options"))
    if not isinstance(requested_pairs, list):
        return jsonify({"status": "error", "message": "pairs must be an array."}), 400
    pairs, seen = [], set()
    for raw in requested_pairs[:40]:
        pair = str(raw).strip()
        if pair in YAHOO_SYMBOLS and pair not in seen: pairs.append(pair); seen.add(pair)
    if not pairs:
        return jsonify({"status": "error", "message": "No supported pairs were supplied."}), 400

    news_lock = evaluate_news_safety_lock(pairs, market)
    if news_lock:
        results = [news_locked_no_signal(pair, news_lock) for pair in pairs]
        diagnostics = {
            "total_pairs": len(results), "completed_pairs": len(results),
            "timed_out_pairs": [], "timed_out_pairs_count": 0,
            "partial_response": False, "data_available": 0, "data_unavailable": len(results),
            "signals_found": 0, "elapsed_seconds": 0,
            "batch_deadline_seconds": BATCH_SCAN_DEADLINE_SECONDS,
            "yahoo_request_timeout_seconds": YAHOO_REQUEST_TIMEOUT_SECONDS,
            "yahoo_fetch_concurrency": YAHOO_FETCH_CONCURRENCY,
            "batch_workers": 0, "scan_mode": opts["mode"],
            "news_safety_lock": news_lock,
        }
        _append_scan_event(auth["user"], market, "AUTO", opts["mode"], False)
        return jsonify({
            "status": "success", "data": results, "diagnostics": diagnostics,
            "cache_hit": False, "news_safety_lock": news_lock,
        })

    options_key = (opts["mode"], opts["min_tf"], opts["min_agreement"], opts["min_score"], opts["vol_min"], opts["vol_max"])
    bridge_cache_user = auth["user"] if broker.casefold() == "quotex" and any("(OTC)" in p for p in pairs) else ""
    key = (broker, bridge_cache_user, selected_expiry, tuple(pairs), options_key); now = time.time()
    with batch_cache_lock:
        cached = batch_cache.get(key)
        if cached and (now - cached["timestamp"]) <= BATCH_CACHE_DURATION:
            payload = cached["payload"]
            found = any(r.get("signal") in {"CALL", "PUT"} for r in payload["data"])
            if batch_results_are_countable(payload["data"]):
                _append_scan_event(auth["user"], market, "AUTO", opts["mode"], found)
            return jsonify({"status": "success", "data": payload["data"], "diagnostics": payload["diagnostics"], "cache_hit": True})
    key_lock = _get_batch_key_lock(key)
    with key_lock:
        now = time.time()
        with batch_cache_lock:
            cached = batch_cache.get(key)
            if cached and (now - cached["timestamp"]) <= BATCH_CACHE_DURATION:
                payload = cached["payload"]
                found = any(r.get("signal") in {"CALL", "PUT"} for r in payload["data"])
                _append_scan_event(auth["user"], market, "AUTO", opts["mode"], found)
                return jsonify({"status": "success", "data": payload["data"], "diagnostics": payload["diagnostics"], "cache_hit": True})

        results_by_pair, timed_out_pairs = {}, []
        workers = min(BATCH_SCAN_WORKERS, len(pairs)); batch_started = time.time()
        pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="raja-batch")
        future_map = {pool.submit(calculate_live_indicators, pair, selected_expiry, opts, auth["user"], broker): pair for pair in pairs}
        done, pending = wait(future_map.keys(), timeout=BATCH_SCAN_DEADLINE_SECONDS)
        for future in done:
            pair = future_map[future]
            try: results_by_pair[pair] = future.result()
            except Exception as exc:
                print(f"Batch scan error for {pair}: {exc}"); results_by_pair[pair] = no_signal_result(pair, "Scan worker failed for this pair.", symbol=YAHOO_SYMBOLS.get(pair))
        for future in pending:
            pair = future_map[future]; timed_out_pairs.append(pair); future.cancel()
            results_by_pair[pair] = no_signal_result(pair, "Skipped because the shared scan deadline was reached; next Auto Re-Scan will retry.", symbol=YAHOO_SYMBOLS.get(pair))
        pool.shutdown(wait=False, cancel_futures=True)
        results = [results_by_pair[pair] for pair in pairs]
        stale_pairs = [r for r in results if r.get("source_stale")]
        delayed_pairs = [r for r in results if r.get("data_delayed")]
        usable_rows = [r for r in results if not r.get("source_stale") and not r.get("data_delayed")]
        data_available = sum(1 for r in usable_rows if r.get("data_age") is not None)
        data_unavailable = len(results) - data_available
        signals_found = sum(1 for r in results if r.get("signal") in {"CALL", "PUT"})
        elapsed = round(time.time() - batch_started, 2)
        all_market_data_blocked = bool(results) and not batch_results_are_countable(results)
        diagnostics = {
            "total_pairs": len(results),
            "completed_pairs": len(done),
            "timed_out_pairs": timed_out_pairs,
            "timed_out_pairs_count": len(timed_out_pairs),
            "partial_response": bool(timed_out_pairs),
            "data_available": data_available,
            "data_unavailable": data_unavailable,
            "stale_pairs_count": len(stale_pairs),
            "delayed_pairs_count": len(delayed_pairs),
            "all_market_data_blocked": all_market_data_blocked,
            "signals_found": signals_found,
            "elapsed_seconds": elapsed,
            "batch_deadline_seconds": BATCH_SCAN_DEADLINE_SECONDS,
            "yahoo_request_timeout_seconds": YAHOO_REQUEST_TIMEOUT_SECONDS,
            "yahoo_fetch_concurrency": YAHOO_FETCH_CONCURRENCY,
            "batch_workers": workers,
            "scan_mode": opts["mode"],
        }
        payload = {"data": results, "diagnostics": diagnostics}
        with batch_cache_lock:
            batch_cache[key] = {"timestamp": time.time(), "payload": payload}
            if len(batch_cache) > 40:
                for old_key, _ in sorted(batch_cache.items(), key=lambda kv: kv[1]["timestamp"])[:10]: batch_cache.pop(old_key, None)
        if batch_results_are_countable(results):
            _append_scan_event(auth["user"], market, "AUTO", opts["mode"], signals_found > 0)
        return jsonify({"status": "success", "data": results, "diagnostics": diagnostics, "cache_hit": False})


@app.route("/scan", methods=["POST"])
def scan_markets():
    data = request.get_json(silent=True) or {}
    auth, error = _auth_session(data)
    if error: return error

    maintenance = scan_maintenance_state()
    if maintenance:
        return jsonify({
            "status": "error",
            "maintenance": True,
            "message": maintenance.get("maintenance_message") or "RAJA AI scans are temporarily paused.",
        }), 503

    selected_pair = str(data.get("pair", "")).strip(); selected_expiry = str(data.get("expiry", "")).strip()
    market = str(data.get("market") or "Unknown")[:80]; broker = str(data.get("broker") or "").strip()[:80]; opts = normalize_scan_options(data.get("scan_options"))
    if not selected_pair or "Auto Scan Best Pair" in selected_pair:
        return jsonify({"status": "error", "message": "Auto Scan must use /scan-batch with the selected market pair list."}), 400
    if selected_pair not in YAHOO_SYMBOLS:
        return jsonify({"status": "error", "message": f"Unsupported pair: {selected_pair}",
                        "data": no_signal_result(selected_pair, "Pair is not configured in Yahoo mapping.")}), 400

    news_lock = evaluate_news_safety_lock([selected_pair], market)
    if news_lock:
        result = news_locked_no_signal(selected_pair, news_lock)
        _append_scan_event(auth["user"], market, selected_pair, opts["mode"], False)
        return jsonify({"status": "success", "data": result, "news_safety_lock": news_lock})

    result = calculate_live_indicators(selected_pair, selected_expiry, opts, auth["user"], broker)
    if market_result_is_countable(result):
        _append_scan_event(
            auth["user"],
            market,
            selected_pair,
            opts["mode"],
            result.get("signal") in {"CALL", "PUT"},
        )
    return jsonify({"status": "success", "data": result})



# =========================================================
# TELEGRAM INTEGRATION SERVICES
# =========================================================

def issue_telegram_license(user_ref):
    """Issue or reuse an active web-compatible VIP key for an admin-approved Telegram user."""
    user = normalize_user_id(user_ref)
    if not user:
        raise ValueError("Telegram/Quotex user reference is required.")

    existing_key, _ = find_active_license_for_user(user)
    if existing_key:
        return existing_key

    while True:
        key = "RAJA-VIP-" + secrets.token_hex(4).upper() + "-2026"
        if load_license_record(key) is None:
            break

    record = {
        "active": True,
        "user": user,
        "device": None,
        "device_label": None,
        "session_token": None,
        "created_at": int(time.time()),
        "last_verified_at": None,
        "last_login_at": None,
        "plan": DEFAULT_LICENSE_PLAN,
        "expires_at": None,
    }
    save_license_record(key, record)
    return key


def validate_telegram_license(key, user_ref):
    """Validate an existing VIP key without consuming/binding a web device session."""
    key = str(key or "").strip()
    user = normalize_user_id(user_ref)
    if not key or not user:
        return False
    record = load_license_record(key)
    if not record or not record.get("active", False):
        return False
    # Important for short trials: Telegram access must stop when the license expires.
    if license_is_expired(record):
        return False
    bound_user = normalize_user_id(record.get("user", ""))
    return (not bound_user) or bound_user == user



def telegram_license_info(key, user_ref):
    """Return plan/expiry details for Telegram reminder and expiry automation."""
    key = str(key or "").strip()
    user = normalize_user_id(user_ref)
    record = load_license_record(key) if key else None
    if not isinstance(record, dict):
        return {"exists": False, "valid": False, "plan": None, "expires_at": None}
    bound_user = normalize_user_id(record.get("user", ""))
    user_matches = (not bound_user) or (bound_user == user)
    now = int(time.time())
    return {
        "exists": True,
        "valid": bool(record.get("active", False)) and user_matches and not license_is_expired(record, now),
        "plan": record.get("plan") or DEFAULT_LICENSE_PLAN,
        "expires_at": record.get("expires_at"),
        "active": bool(record.get("active", False)),
        "user_matches": user_matches,
    }


def telegram_scan_pair(pair, selected_expiry):
    return calculate_live_indicators(str(pair), str(selected_expiry))


def telegram_scan_auto(pairs, selected_expiry):
    """Run the same strict multi-TF analysis for a Telegram Auto Best Pair scan."""
    pairs = [str(p).strip() for p in (pairs or []) if str(p).strip() in YAHOO_SYMBOLS][:40]
    if not pairs:
        return {"best": None, "diagnostics": {"total_pairs": 0, "data_available": 0}}

    workers = min(BATCH_SCAN_WORKERS, len(pairs))
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="raja-tg-scan")
    future_map = {pool.submit(calculate_live_indicators, pair, selected_expiry): pair for pair in pairs}
    done, pending = wait(future_map.keys(), timeout=BATCH_SCAN_DEADLINE_SECONDS)
    results = []
    for future in done:
        try:
            results.append(future.result())
        except Exception as exc:
            print(f"Telegram auto scan error for {future_map[future]}: {exc}")
    for future in pending:
        future.cancel()
    pool.shutdown(wait=False, cancel_futures=True)

    valid = [r for r in results if isinstance(r, dict) and r.get("signal") in {"CALL", "PUT"}]
    best = max(valid, key=lambda r: float(r.get("score") or 0), default=None)
    diagnostics = {
        "total_pairs": len(pairs),
        "completed_pairs": len(done),
        "timed_out_pairs_count": len(pending),
        "data_available": sum(1 for r in results if isinstance(r, dict) and r.get("data_age") is not None),
        "signals_found": len(valid),
    }
    return {"best": best, "diagnostics": diagnostics}


try:
    from telegram_bot import register_telegram_routes
    register_telegram_routes(app, {
        "issue_license": issue_telegram_license,
        "validate_license": validate_telegram_license,
        "license_info": telegram_license_info,
        "scan_pair": telegram_scan_pair,
        "scan_auto": telegram_scan_auto,
    })
except Exception as telegram_integration_error:
    # Telegram must never prevent the existing web bot from starting.
    print(f"Telegram integration disabled due to startup error: {telegram_integration_error}")


signal_worker_thread = threading.Thread(
    target=signal_outcome_worker,
    daemon=True,
)
signal_worker_thread.start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        threaded=True,
    )
