from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import yfinance as yf
import os
import time
import json
import secrets
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from collections import Counter
from datetime import datetime, timezone
from urllib.request import Request as UrlRequest, urlopen

try:
    import psycopg
except Exception:
    psycopg = None

app = Flask(__name__, static_folder=".", template_folder=".")
CORS(app)

# =========================================================
# RAJA AI MULTI-TIMEFRAME BACKEND
# Yahoo Finance 1-minute OHLCV is the base/reference feed.
# 2m, 5m, 10m, 15m and 30m are built from the same Yahoo
# 1-minute candles so every timeframe stays synchronized.
#
# IMPORTANT: "(OTC)" assets are underlying-market proxies.
# They are NOT exact Quotex OTC quotes.
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
    "Bitcoin (OTC)": "BTC-USD",
    "Ethereum (OTC)": "ETH-USD",
    "Litecoin (OTC)": "LTC-USD",
    "Ripple (OTC)": "XRP-USD",
    "Solana (OTC)": "SOL-USD",
    "Toncoin (OTC)": "TON-USD",
    "Ethereum Classic (OTC)": "ETC-USD",
    "Axie Infinity (OTC)": "AXS-USD",
    "Binance Coin (OTC)": "BNB-USD",
    "Polkadot (OTC)": "DOT-USD",
    "Avalanche (OTC)": "AVAX-USD",
    "Chainlink (OTC)": "LINK-USD",
    "Bitcoin Cash (OTC)": "BCH-USD",
    "Zcash (OTC)": "ZEC-USD",
    "Cosmos (OTC)": "ATOM-USD",

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
}

ALL_PAIRS = list(YAHOO_SYMBOLS.keys())
UNIQUE_YAHOO_SYMBOLS = list(dict.fromkeys(YAHOO_SYMBOLS.values()))

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
BATCH_SCAN_WORKERS = max(1, min(4, int(os.environ.get("RAJA_BATCH_WORKERS", "3"))))
YAHOO_REQUEST_TIMEOUT_SECONDS = max(3.0, min(15.0, float(os.environ.get("RAJA_YAHOO_REQUEST_TIMEOUT", "7"))))
# Browser batch timeout is 90s; 78s gives the server more room than the old 68s while
# still returning before the browser aborts.
BATCH_SCAN_DEADLINE_SECONDS = max(25.0, min(85.0, float(os.environ.get("RAJA_BATCH_DEADLINE_SECONDS", "78"))))

market_cache = {}
cache_lock = threading.RLock()

symbol_fetch_locks = {}
symbol_fetch_locks_guard = threading.Lock()
failed_symbol_until = {}
failed_symbol_lock = threading.Lock()

yahoo_fetch_semaphore = threading.BoundedSemaphore(YAHOO_FETCH_CONCURRENCY)
yahoo_pace_lock = threading.Lock()
last_yahoo_fetch_started = 0.0

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
DATA_DIR = Path(os.environ.get("RAJA_DATA_DIR", str(BASE_DIR))).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)

LICENSE_FILE = DATA_DIR / "licenses.json"
license_lock = threading.RLock()

ADMIN_PASSWORD = os.environ.get("RAJA_ADMIN_PASSWORD", "786")

# Permanent license storage:
# - Recommended on Render Free: set DATABASE_URL (for example a Neon/Supabase PostgreSQL URL).
# - If DATABASE_URL is absent, the app falls back to licenses.json. Render Free local files
#   are ephemeral, so the fallback is for local development/testing only.
DATABASE_URL = (os.environ.get("DATABASE_URL") or os.environ.get("RAJA_DATABASE_URL") or "").strip()
LICENSE_STORE_MODE = "postgres" if DATABASE_URL else "file"
DEVICE_SESSION_TTL_SECONDS = max(120, int(os.environ.get("RAJA_DEVICE_SESSION_TTL", "300")))
# User requested a one-time reset of all previously generated keys. This marker makes
# the reset run only once per persistent database. Change/empty the env var to control it.
LICENSE_RESET_VERSION = os.environ.get("RAJA_LICENSE_RESET_VERSION", "2026-08-12-v8-clean-slate").strip()

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
    "15m": 900,
    "30m": 1800,
}


def normalize_user_id(value):
    """Canonicalize Telegram/user IDs so @Name and name resolve to the same customer."""
    user = str(value or "").strip()
    if user.startswith("@"):
        user = user[1:].strip()
    return user.casefold()


def _db_connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    if psycopg is None:
        raise RuntimeError("psycopg is not installed; install requirements.txt")
    return psycopg.connect(DATABASE_URL, connect_timeout=10)


def initialize_license_store():
    if DATABASE_URL:
        # Persistent license + scan analytics storage.
        with _db_connect() as conn:
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
    licenses = load_licenses()
    record = licenses.get(key)
    now = int(time.time())
    if not record or not record.get("active", False) or license_is_expired(record, now):
        return None, (jsonify({"status": "error", "message": "Invalid, expired or revoked license key."}), 401)
    if normalize_user_id(record.get("user", "")) != user:
        return None, (jsonify({"status": "error", "message": "License is assigned to a different user."}), 403)
    if str(record.get("device") or "") != device or str(record.get("session_token") or "") != session_token:
        return None, (jsonify({"status": "error", "message": "This session was replaced by another device. Please login again."}), 409)
    return {"key": key, "user": user, "device": device, "record": record, "licenses": licenses}, None


initialize_license_store()


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
                                "VALUES(%s,%s,%s,%s)",
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
    """Resolve a pending theoretical signal from Yahoo 1m candles.

    Entry = Open of the next 1-minute candle after the signal was displayed.
    Exit  = Close of the final 1-minute candle inside the selected expiry.
    """
    pair = item.get("pair")
    symbol = YAHOO_SYMBOLS.get(pair)
    if not symbol:
        return False

    update_symbol_cache(symbol)

    with cache_lock:
        cached = market_cache.get(symbol)

    if not cached:
        return False

    rows = dataframe_epoch_rows(cached.get("data"))
    if not rows:
        return False

    entry_epoch = int(item.get("entry_epoch", 0))
    expiry_epoch = int(item.get("expiry_epoch", 0))

    # Allow a small tolerance for delayed/missing minute bars.
    entry_candidates = [
        (epoch, row) for epoch, row in rows
        if entry_epoch <= epoch < entry_epoch + 120
    ]
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
    item["yahoo_result"] = result
    item["result_source"] = "yahoo_live"
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
        if item_status == "AWAITING_QX" and item.get("yahoo_result"):
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
                    "exit_candle_epoch", "yahoo_result"
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


def update_symbol_cache(symbol, force=False):
    """Refresh one Yahoo symbol with single-flight, pacing and failure cooldown."""
    now = time.time()

    with cache_lock:
        cached = market_cache.get(symbol)
        if cached and not force and (now - cached["timestamp"]) <= CACHE_DURATION:
            return True

    with failed_symbol_lock:
        blocked_until = failed_symbol_until.get(symbol, 0)

    if not force and blocked_until > now:
        with cache_lock:
            cached = market_cache.get(symbol)
            if cached and (now - cached["timestamp"]) <= STALE_CACHE_MAX_AGE:
                return True
        return False

    symbol_lock = _get_symbol_fetch_lock(symbol)
    with symbol_lock:
        now = time.time()

        # Another request may have refreshed this symbol while this caller waited.
        with cache_lock:
            cached = market_cache.get(symbol)
            if cached and not force and (now - cached["timestamp"]) <= CACHE_DURATION:
                return True

        with failed_symbol_lock:
            blocked_until = failed_symbol_until.get(symbol, 0)

        if not force and blocked_until > now:
            with cache_lock:
                cached = market_cache.get(symbol)
                if cached and (now - cached["timestamp"]) <= STALE_CACHE_MAX_AGE:
                    return True
            return False

        try:
            with yahoo_fetch_semaphore:
                _pace_yahoo_request()
                df = fetch_yahoo_1m(symbol)

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


def get_market_data(pair):
    symbol = YAHOO_SYMBOLS.get(pair)
    if not symbol:
        return None, None, None

    now = time.time()
    with cache_lock:
        cached = market_cache.get(symbol)

    if cached:
        age = now - cached["timestamp"]
        if age <= CACHE_DURATION:
            return cached["data"].copy(), age, symbol

    refreshed = update_symbol_cache(symbol)

    with cache_lock:
        cached = market_cache.get(symbol)

    if cached:
        age = time.time() - cached["timestamp"]
        if refreshed or age <= STALE_CACHE_MAX_AGE:
            return cached["data"].copy(), age, symbol

    return None, None, symbol


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
    """Analyze one CLOSED timeframe. No random values are used."""
    if df is None or df.empty or len(df) < 60:
        return {
            "timeframe": timeframe,
            "signal": "NO SIGNAL",
            "score": 0,
            "reason": "Insufficient closed candles",
        }

    required = {"Open", "High", "Low", "Close"}
    if not required.issubset(df.columns):
        return {
            "timeframe": timeframe,
            "signal": "NO SIGNAL",
            "score": 0,
            "reason": "Missing OHLC columns",
        }

    df = df.copy().dropna(subset=list(required))
    if len(df) < 60:
        return {
            "timeframe": timeframe,
            "signal": "NO SIGNAL",
            "score": 0,
            "reason": "Insufficient clean candles",
        }

    close = df["Close"]

    rsi = safe_float(calculate_rsi(close, 14).iloc[-1])
    ema9 = safe_float(calculate_ema(close, 9).iloc[-1])
    ema21 = safe_float(calculate_ema(close, 21).iloc[-1])
    ema50 = safe_float(calculate_ema(close, 50).iloc[-1])

    macd, macd_signal = calculate_macd(close)
    macd_now = safe_float(macd.iloc[-1])
    macd_sig_now = safe_float(macd_signal.iloc[-1])

    bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(close)
    bb_mid = safe_float(bb_middle.iloc[-1])

    atr = safe_float(calculate_atr(df, 14).iloc[-1])

    adx, plus_di, minus_di = calculate_adx_components(df, 14)
    adx_now = safe_float(adx.iloc[-1])
    plus_di_now = safe_float(plus_di.iloc[-1])
    minus_di_now = safe_float(minus_di.iloc[-1])

    price = safe_float(close.iloc[-1])
    previous_close = safe_float(close.iloc[-2])

    values = [
        rsi, ema9, ema21, ema50,
        macd_now, macd_sig_now, bb_mid,
        atr, adx_now, plus_di_now, minus_di_now,
        price, previous_close,
    ]

    if any(v is None for v in values) or atr <= 0:
        return {
            "timeframe": timeframe,
            "signal": "NO SIGNAL",
            "score": 0,
            "reason": "Indicators not ready",
        }

    ema_bullish = price > ema9 > ema21 > ema50
    ema_bearish = price < ema9 < ema21 < ema50

    macd_bullish = macd_now > macd_sig_now
    macd_bearish = macd_now < macd_sig_now

    bb_bullish = price > bb_mid
    bb_bearish = price < bb_mid

    adx_bullish = adx_now >= 18 and plus_di_now > minus_di_now
    adx_bearish = adx_now >= 18 and minus_di_now > plus_di_now

    momentum_bullish = price > previous_close
    momentum_bearish = price < previous_close

    last = df.iloc[-1]
    candle_open = safe_float(last["Open"])
    candle_high = safe_float(last["High"])
    candle_low = safe_float(last["Low"])
    candle_close = safe_float(last["Close"])

    if None in (candle_open, candle_high, candle_low, candle_close):
        return {
            "timeframe": timeframe,
            "signal": "NO SIGNAL",
            "score": 0,
            "reason": "Latest candle incomplete",
        }

    candle_range = candle_high - candle_low
    if candle_range <= 0:
        return {
            "timeframe": timeframe,
            "signal": "NO SIGNAL",
            "score": 0,
            "reason": "Invalid candle range",
        }

    bullish_candle = candle_close > candle_open
    bearish_candle = candle_close < candle_open

    upper_wick = candle_high - max(candle_open, candle_close)
    lower_wick = min(candle_open, candle_close) - candle_low

    bullish_rejection = (
        lower_wick / candle_range >= 0.25
        and bullish_candle
    )
    bearish_rejection = (
        upper_wick / candle_range >= 0.25
        and bearish_candle
    )

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

    if 52 <= rsi <= 70:
        bullish_points += 1.0
    elif 30 <= rsi <= 48:
        bearish_points += 1.0

    if ema_bullish:
        bullish_points += 2.0
    elif ema_bearish:
        bearish_points += 2.0

    if macd_bullish:
        bullish_points += 1.0
    elif macd_bearish:
        bearish_points += 1.0

    if bb_bullish:
        bullish_points += 1.0
    elif bb_bearish:
        bearish_points += 1.0

    if adx_bullish:
        bullish_points += 1.5
    elif adx_bearish:
        bearish_points += 1.5

    if momentum_bullish:
        bullish_points += 1.0
    elif momentum_bearish:
        bearish_points += 1.0

    if bullish_rejection:
        bullish_points += 1.0
    elif bearish_rejection:
        bearish_points += 1.0
    elif bullish_candle:
        bullish_points += 0.5
    elif bearish_candle:
        bearish_points += 0.5

    if volume_bullish:
        bullish_points += 1.0
    elif volume_bearish:
        bearish_points += 1.0

    difference = abs(bullish_points - bearish_points)
    winning_points = max(bullish_points, bearish_points)

    # Slightly relaxed per-TF gate because final decision also requires
    # multi-timeframe agreement.
    if difference < 1.5 or winning_points < 3.5:
        return {
            "timeframe": timeframe,
            "signal": "NO SIGNAL",
            "score": 0,
            "reason": "Weak/conflicting timeframe",
            "rsi": round(rsi, 2),
            "adx": round(adx_now, 2),
            "bullish_points": round(bullish_points, 2),
            "bearish_points": round(bearish_points, 2),
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
        "rsi": round(rsi, 2),
        "adx": round(adx_now, 2),
        "atr": round(atr, 8),
        "price": round(price, 8),
        "bullish_points": round(bullish_points, 2),
        "bearish_points": round(bearish_points, 2),
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


def no_signal_result(pair, reason, symbol=None, data_age=None, timeframes=None):
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
        "source": "Yahoo Finance",
        "source_mode": "underlying_proxy" if "(OTC)" in pair else "live_reference",
        "otc_proxy_warning": "(OTC)" in pair,
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


def calculate_live_indicators(pair, selected_expiry=None, scan_options=None):
    """Scan synchronized timeframes using a SAFE/BALANCED/AGGRESSIVE/CUSTOM gate."""
    opts = normalize_scan_options(scan_options)
    if pair not in YAHOO_SYMBOLS:
        result = no_signal_result(pair, "Pair is not configured in Yahoo mapping.")
        result.update({"scan_mode": opts["mode"], "scan_thresholds": opts})
        return result

    base_df, data_age, symbol = get_market_data(pair)
    if base_df is None or base_df.empty:
        result = no_signal_result(pair, "Yahoo market data unavailable.", symbol=symbol, data_age=data_age)
        result.update({"scan_mode": opts["mode"], "scan_thresholds": opts})
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
        tf: {"signal": r.get("signal"), "score": r.get("score", 0), "rsi": r.get("rsi"),
             "adx": r.get("adx"), "closed_candle_epoch": r.get("closed_candle_epoch")}
        for tf, r in results.items()
    }

    def rejected(reason):
        result = no_signal_result(pair, reason, symbol=symbol, data_age=data_age, timeframes=summary)
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

    aligned_tfs = [r["timeframe"] for r in supporters]
    opposing_tfs = [r["timeframe"] for r in opponents]
    return {
        "pair": pair, "score": round(multi_tf_score, 2), "signal": signal,
        "reason": f"{opts['mode']} · Multi-TF agreement: {len(supporters)}/{valid_count} valid timeframes -> {signal}",
        "rsi": representative.get("rsi"), "adx": representative.get("adx"), "atr": representative.get("atr"),
        "price": representative.get("price"), "bullish_points": representative.get("bullish_points", 0),
        "bearish_points": representative.get("bearish_points", 0),
        "data_age": round(data_age, 2) if data_age is not None else None,
        "source": "Yahoo Finance", "source_mode": "underlying_proxy" if "(OTC)" in pair else "live_reference",
        "otc_proxy_warning": "(OTC)" in pair, "yahoo_symbol": symbol,
        "timeframes_scanned": list(TIMEFRAMES.keys()), "aligned_timeframes": aligned_tfs,
        "opposing_timeframes": opposing_tfs, "timeframe_summary": summary,
        "multi_tf_agreement": round(agreement_ratio * 100, 1), "selected_expiry": selected_expiry,
        "required_expiry_timeframe": required_tf,
        "confirmation_mode": f"{opts['mode']} · {opts['min_tf']}-of-6 + {required_tf or 'TF'} Required",
        "duplicate_protection": False, "scan_mode": opts["mode"], "scan_thresholds": opts,
        "market_stability_score": stability_score, "market_risk_level": risk_level,
        "volatility_pct": volatility_pct, "chart_preview": chart_preview,
        "no_trade": False, "quality_gate": "PASSED",
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
    with urlopen(req, timeout=10) as response:
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
def home():
    if os.path.exists(BASE_DIR / "index.html"):
        return send_from_directory(str(BASE_DIR), "index.html")
    return (
        "RAJA AI backend is running. "
        "Place index.html in the same folder as bot.py."
    )


@app.route("/manifest.json", methods=["GET"])
def pwa_manifest():
    return send_from_directory(str(BASE_DIR), "manifest.json", mimetype="application/manifest+json")


@app.route("/sw.js", methods=["GET"])
def pwa_service_worker():
    response = send_from_directory(str(BASE_DIR), "sw.js", mimetype="application/javascript")
    response.headers["Service-Worker-Allowed"] = "/"
    response.headers["Cache-Control"] = "no-cache"
    return response


@app.route("/raja-ai-icon-<size>.png", methods=["GET"])
def pwa_icon(size):
    if size not in {"192", "512"}:
        return jsonify({"status": "error", "message": "Icon not found."}), 404
    return send_from_directory(str(BASE_DIR), f"raja-ai-icon-{size}.png", mimetype="image/png")


@app.after_request
def disable_html_cache(response):
    # Prevent stale index.html/inline JS from surviving a new Render deploy.
    if request.path == "/" or request.path.endswith(".html"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


@app.route("/health", methods=["GET"])
def health():
    with cache_lock:
        cached_symbols = len(market_cache)

    return jsonify({
        "status": "success",
        "service": "RAJA AI multi-timeframe backend",
        "yahoo_pairs": len(YAHOO_SYMBOLS),
        "unique_yahoo_symbols": len(UNIQUE_YAHOO_SYMBOLS),
        "cached_symbols": cached_symbols,
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
        "batch_deadline_seconds": BATCH_SCAN_DEADLINE_SECONDS,
        "automatic_outcome_tracking": list(AUTO_TRACK_EXPIRIES.keys()),
        "closed_candle_analysis": True,
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

    licenses = load_licenses()
    record = licenses.get(key)
    now = int(time.time())
    if not record or not record.get("active", False):
        return jsonify({"status": "error", "message": "Invalid or revoked license key."}), 401
    if license_is_expired(record, now):
        return jsonify({"status": "error", "message": "This license has expired. Contact admin to renew access."}), 401

    bound_user = normalize_user_id(record.get("user", ""))
    # @username and username are treated as the same identity. A genuinely different
    # customer still cannot use someone else's key.
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
        licenses[key] = record
        save_licenses(licenses)
        return jsonify({
            "status": "success", "message": "Session active.", "user": user,
            "device_bound": True, "device_label": record.get("device_label"),
            "session_token": record.get("session_token"), "plan": record.get("plan") or DEFAULT_LICENSE_PLAN,
            "expires_at": record.get("expires_at"),
        })

    # NEW DEVICE WINS: a valid login immediately replaces the previous browser session.
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
    licenses[key] = record
    save_licenses(licenses)
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
    licenses = load_licenses()
    record = licenses.get(key)
    if not record:
        return jsonify({"status": "success", "message": "Session already cleared."})
    if (normalize_user_id(record.get("user", "")) == user and record.get("device") == device
            and (not record.get("session_token") or record.get("session_token") == token)):
        record["device"] = None
        record["device_label"] = None
        record["last_verified_at"] = None
        record["session_token"] = None
        licenses[key] = record
        save_licenses(licenses)
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
    if password != ADMIN_PASSWORD:
        return jsonify({"status": "error", "message": "Incorrect admin password."}), 403
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

    licenses = load_licenses()
    while True:
        key = "RAJA-VIP-" + secrets.token_hex(4).upper() + "-2026"
        if key not in licenses:
            break
    now = int(time.time())
    licenses[key] = {
        "active": True, "user": user, "device": None, "device_label": None,
        "session_token": None, "created_at": now, "last_verified_at": None,
        "last_login_at": None, "plan": plan, "expires_at": now + int(duration_days * 86400) if duration_days else None,
    }
    save_licenses(licenses)
    if is_free_trial:
        record_trial_claim("user", user, key)
    return jsonify({"status": "success", "message": "License created.", "key": key, "user": user,
                    "plan": plan, "expires_at": licenses[key].get("expires_at")})


@app.route("/admin/licenses", methods=["POST"])
def admin_list_licenses():
    data = request.get_json(silent=True) or {}
    password = str(data.get("password", ""))
    if password != ADMIN_PASSWORD:
        return jsonify({"status": "error", "message": "Incorrect admin password."}), 403
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
    if str((data or {}).get("password", "")) != ADMIN_PASSWORD:
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
    licenses = load_licenses()
    if key not in licenses:
        return jsonify({"status": "error", "message": "License key not found."}), 404
    record = licenses.get(key) if isinstance(licenses.get(key), dict) else {}
    record["device"] = None; record["device_label"] = None; record["last_verified_at"] = None; record["session_token"] = None
    licenses[key] = record
    save_licenses(licenses)
    return jsonify({"status": "success", "message": "Device binding reset.", "key": key})


@app.route("/admin/reset-all-devices", methods=["POST"])
def admin_reset_all_devices():
    data = request.get_json(silent=True) or {}
    auth_error = _validate_admin_password(data)
    if auth_error:
        return auth_error
    licenses = load_licenses(); updated = 0
    for key, record in list(licenses.items()):
        record = record if isinstance(record, dict) else {}
        if record.get("device") or record.get("session_token"): updated += 1
        record["device"] = None; record["device_label"] = None; record["last_verified_at"] = None; record["session_token"] = None
        licenses[key] = record
    save_licenses(licenses)
    return jsonify({"status": "success", "message": "All device bindings cleared.", "updated": updated, "total": len(licenses)})


def _delete_license_from_request(data):
    password = str(data.get("password", "")); key = str(data.get("key", "")).strip()
    if password != ADMIN_PASSWORD:
        return None, (jsonify({"status": "error", "message": "Incorrect admin password."}), 403)
    if not key:
        return None, (jsonify({"status": "error", "message": "License key is required."}), 400)
    licenses = load_licenses()
    if key not in licenses:
        return None, (jsonify({"status": "error", "message": "License key not found."}), 404)
    licenses.pop(key, None); save_licenses(licenses)
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
    data = request.get_json(silent=True) or {}; password = str(data.get("password", ""))
    if password != ADMIN_PASSWORD:
        return jsonify({"status": "error", "message": "Incorrect admin password."}), 403
    licenses = load_licenses(); removed = len(licenses); save_licenses({})
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
        "status": "PENDING", "result_source": "pending", "source": "Yahoo Finance",
        "source_mode": "underlying_proxy" if "(OTC)" in pair else "live_reference",
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
    market = str(data.get("market") or "Unknown")[:80]; opts = normalize_scan_options(data.get("scan_options"))
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
    key = (selected_expiry, tuple(pairs), options_key); now = time.time()
    with batch_cache_lock:
        cached = batch_cache.get(key)
        if cached and (now - cached["timestamp"]) <= BATCH_CACHE_DURATION:
            payload = cached["payload"]
            found = any(r.get("signal") in {"CALL", "PUT"} for r in payload["data"])
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
        future_map = {pool.submit(calculate_live_indicators, pair, selected_expiry, opts): pair for pair in pairs}
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
        data_available = sum(1 for r in results if r.get("data_age") is not None); data_unavailable = len(results) - data_available
        signals_found = sum(1 for r in results if r.get("signal") in {"CALL", "PUT"}); elapsed = round(time.time() - batch_started, 2)
        diagnostics = {"total_pairs": len(results), "completed_pairs": len(done), "timed_out_pairs": timed_out_pairs,
                       "timed_out_pairs_count": len(timed_out_pairs), "partial_response": bool(timed_out_pairs),
                       "data_available": data_available, "data_unavailable": data_unavailable, "signals_found": signals_found,
                       "elapsed_seconds": elapsed, "batch_deadline_seconds": BATCH_SCAN_DEADLINE_SECONDS,
                       "yahoo_request_timeout_seconds": YAHOO_REQUEST_TIMEOUT_SECONDS, "yahoo_fetch_concurrency": YAHOO_FETCH_CONCURRENCY,
                       "batch_workers": workers, "scan_mode": opts["mode"]}
        payload = {"data": results, "diagnostics": diagnostics}
        with batch_cache_lock:
            batch_cache[key] = {"timestamp": time.time(), "payload": payload}
            if len(batch_cache) > 40:
                for old_key, _ in sorted(batch_cache.items(), key=lambda kv: kv[1]["timestamp"])[:10]: batch_cache.pop(old_key, None)
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
    market = str(data.get("market") or "Unknown")[:80]; opts = normalize_scan_options(data.get("scan_options"))
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

    result = calculate_live_indicators(selected_pair, selected_expiry, opts)
    _append_scan_event(auth["user"], market, selected_pair, opts["mode"], result.get("signal") in {"CALL", "PUT"})
    return jsonify({"status": "success", "data": result})



# =========================================================
# TELEGRAM INTEGRATION SERVICES
# =========================================================

def issue_telegram_license(user_ref):
    """Issue or reuse an active web-compatible VIP key for an admin-approved Telegram user."""
    user = normalize_user_id(user_ref)
    if not user:
        raise ValueError("Telegram/Quotex user reference is required.")

    licenses = load_licenses()
    for key, record in licenses.items():
        if record.get("active", False) and normalize_user_id(record.get("user", "")) == user:
            return key

    while True:
        key = "RAJA-VIP-" + secrets.token_hex(4).upper() + "-2026"
        if key not in licenses:
            break

    licenses[key] = {
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
    save_licenses(licenses)
    return key


def validate_telegram_license(key, user_ref):
    """Validate an existing VIP key without consuming/binding a web device session."""
    key = str(key or "").strip()
    user = normalize_user_id(user_ref)
    if not key or not user:
        return False
    record = load_licenses().get(key)
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
    record = load_licenses().get(key) if key else None
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
