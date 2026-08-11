from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import yfinance as yf
import os
import time
import json
import secrets
import threading
from pathlib import Path

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
    "Bitcoin (OTC)": "BTC-USD",
    "Ethereum (OTC)": "ETH-USD",
    "Litecoin (OTC)": "LTC-USD",
    "Ripple (OTC)": "XRP-USD",
    "Solana (OTC)": "SOL-USD",
    "Toncoin (OTC)": "TON-USD",
    "Ethereum Classic (OTC)": "ETC-USD",
    "Axie Infinity (OTC)": "AXS-USD",
    "Binance Coin (OTC)": "BNB-USD",
    "Trump (OTC)": "TRUMP-USD",
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
    "XAUUSD": "XAUUSD=X",

    # ---------------- Forex OTC proxies ----------------
    "EUR/USD (OTC)": "EURUSD=X",
    "GBP/USD (OTC)": "GBPUSD=X",
    "USD/JPY (OTC)": "USDJPY=X",
    "AUD/USD (OTC)": "AUDUSD=X",
    "USD/CAD (OTC)": "USDCAD=X",
    "USD/CHF (OTC)": "USDCHF=X",
    "NZD/USD (OTC)": "NZDUSD=X",
    "EUR/GBP (OTC)": "EURGBP=X",
    "EUR/JPY (OTC)": "EURJPY=X",
    "GBP/JPY (OTC)": "GBPJPY=X",
    "AUD/JPY (OTC)": "AUDJPY=X",
    "EUR/AUD (OTC)": "EURAUD=X",
    "GBP/AUD (OTC)": "GBPAUD=X",
    "CAD/JPY (OTC)": "CADJPY=X",
    "EUR/CAD (OTC)": "EURCAD=X",
    "GBP/CAD (OTC)": "GBPCAD=X",
    "NZD/JPY (OTC)": "NZDJPY=X",
    "AUD/NZD (OTC)": "AUDNZD=X",
    "EUR/CHF (OTC)": "EURCHF=X",
    "GBP/CHF (OTC)": "GBPCHF=X",
    "NZD/CAD (OTC)": "NZDCAD=X",
    "NZD/CHF (OTC)": "NZDCHF=X",
    "USD/BRL (OTC)": "USDBRL=X",
    "USD/ARS (OTC)": "USDARS=X",
    "USD/INR (OTC)": "USDINR=X",
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
CACHE_DURATION = 90
market_cache = {}
cache_lock = threading.Lock()

# Duplicate-signal protection.
# Prevents the same pair/direction from being re-issued from the same
# multi-timeframe candle context within a short lock window.
recent_signal_lock = threading.Lock()
recent_signals = {}
DUPLICATE_SIGNAL_COOLDOWN = 120  # seconds

# =========================================================
# LICENSE STORE
# =========================================================

BASE_DIR = Path(__file__).resolve().parent
LICENSE_FILE = BASE_DIR / "licenses.json"
license_lock = threading.Lock()

ADMIN_PASSWORD = os.environ.get("RAJA_ADMIN_PASSWORD", "786")

SIGNALS_FILE = BASE_DIR / "signals.json"
signals_lock = threading.Lock()
AUTO_TRACK_EXPIRIES = {
    "1m": 60,
    "2m": 120,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
}

DEFAULT_LICENSES = {
    "RAJA-VIP-2026-X99": {
        "active": True, "user": None, "device": None, "created_at": None
    },
    "RAJA-VIP-PRO-777": {
        "active": True, "user": None, "device": None, "created_at": None
    },
    "RAJA-AI-MASTERKEY": {
        "active": True, "user": None, "device": None, "created_at": None
    },
}


def load_licenses():
    with license_lock:
        if not LICENSE_FILE.exists():
            LICENSE_FILE.write_text(
                json.dumps(DEFAULT_LICENSES, indent=2),
                encoding="utf-8",
            )
            return {k: dict(v) for k, v in DEFAULT_LICENSES.items()}

        try:
            data = json.loads(LICENSE_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("Invalid license database")
        except Exception:
            data = {k: dict(v) for k, v in DEFAULT_LICENSES.items()}

        changed = False
        for key, value in DEFAULT_LICENSES.items():
            if key not in data:
                data[key] = dict(value)
                changed = True

        if changed:
            LICENSE_FILE.write_text(
                json.dumps(data, indent=2),
                encoding="utf-8",
            )
        return data


def save_licenses(data):
    with license_lock:
        temp = LICENSE_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        temp.replace(LICENSE_FILE)



# =========================================================
# SIGNAL TRACKING
# =========================================================

def load_signals():
    with signals_lock:
        if not SIGNALS_FILE.exists():
            SIGNALS_FILE.write_text("[]", encoding="utf-8")
            return []

        try:
            data = json.loads(SIGNALS_FILE.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            return []


def save_signals(items):
    with signals_lock:
        temp = SIGNALS_FILE.with_suffix(".tmp")
        temp.write_text(json.dumps(items, indent=2), encoding="utf-8")
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
    item["status"] = "COMPLETED"
    item["resolved_at"] = int(time.time())
    return True


def signal_outcome_worker():
    while True:
        try:
            items = load_signals()
            changed = False
            now = int(time.time())

            for item in items:
                if item.get("status") != "PENDING":
                    continue

                expiry_epoch = int(item.get("expiry_epoch", 0))

                # Give Yahoo a few seconds after the expiry boundary.
                if now < expiry_epoch + 8:
                    continue

                if resolve_tracked_signal(item):
                    changed = True

            if changed:
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


def update_symbol_cache(symbol):
    try:
        df = fetch_yahoo_1m(symbol)
        if df is None:
            return False

        with cache_lock:
            market_cache[symbol] = {
                "data": df.copy(),
                "timestamp": time.time(),
            }
        return True

    except Exception as e:
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

    update_symbol_cache(symbol)

    with cache_lock:
        cached = market_cache.get(symbol)

    if not cached:
        return None, None, symbol

    age = time.time() - cached["timestamp"]
    return cached["data"].copy(), age, symbol


def background_market_poller():
    """Pre-warm each unique Yahoo symbol without six separate TF requests."""
    while True:
        for symbol in UNIQUE_YAHOO_SYMBOLS:
            update_symbol_cache(symbol)
            time.sleep(0.75)
        time.sleep(5)


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
    }


def calculate_live_indicators(pair, selected_expiry=None):
    """Scan 1m,2m,5m,10m,15m,30m and require selected-expiry confirmation."""
    if pair not in YAHOO_SYMBOLS:
        return no_signal_result(
            pair,
            "Pair is not configured in Yahoo mapping.",
        )

    base_df, data_age, symbol = get_market_data(pair)

    if base_df is None or base_df.empty:
        return no_signal_result(
            pair,
            "Yahoo market data unavailable.",
            symbol=symbol,
            data_age=data_age,
        )

    results = {}
    for tf_name, minutes in TIMEFRAMES.items():
        tf_df = build_timeframe(base_df, minutes)
        results[tf_name] = analyze_timeframe(tf_df, tf_name)

    call_results = [
        r for r in results.values()
        if r.get("signal") == "CALL"
    ]
    put_results = [
        r for r in results.values()
        if r.get("signal") == "PUT"
    ]

    valid_count = len(call_results) + len(put_results)

    summary = {
        tf: {
            "signal": r.get("signal"),
            "score": r.get("score", 0),
            "rsi": r.get("rsi"),
            "adx": r.get("adx"),
            "closed_candle_epoch": r.get("closed_candle_epoch"),
        }
        for tf, r in results.items()
    }

    # Require at least 4 directional timeframes out of 6.
    if valid_count < 4:
        return no_signal_result(
            pair,
            "Fewer than 4 timeframes reached valid confluence.",
            symbol=symbol,
            data_age=data_age,
            timeframes=summary,
        )

    if len(call_results) > len(put_results):
        signal = "CALL"
        supporters = call_results
        opponents = put_results
    elif len(put_results) > len(call_results):
        signal = "PUT"
        supporters = put_results
        opponents = call_results
    else:
        return no_signal_result(
            pair,
            "Multi-timeframe direction is tied.",
            symbol=symbol,
            data_age=data_age,
            timeframes=summary,
        )

    # At least 4 timeframes must agree with the final direction.
    if len(supporters) < 4:
        return no_signal_result(
            pair,
            "Fewer than 4 timeframes agree with the final direction.",
            symbol=symbol,
            data_age=data_age,
            timeframes=summary,
        )

    agreement_ratio = len(supporters) / valid_count

    # Require at least two-thirds directional agreement among valid TFs.
    if agreement_ratio < (2 / 3):
        return no_signal_result(
            pair,
            "Multi-timeframe agreement below 66.7%.",
            symbol=symbol,
            data_age=data_age,
            timeframes=summary,
        )

    # The user's selected expiry is a hard confirmation gate.
    # Example: 1m expiry requires the 1m analysis itself to confirm the final direction.
    required_tf = EXPIRY_CONFIRMATION_TIMEFRAME.get(str(selected_expiry or "").strip())
    if required_tf:
        required_result = results.get(required_tf) or {}
        required_signal = required_result.get("signal")
        if required_signal != signal:
            return no_signal_result(
                pair,
                (
                    f"Selected expiry {selected_expiry} requires {required_tf} confirmation; "
                    f"{required_tf} is {required_signal or 'NO SIGNAL'} while final direction is {signal}."
                ),
                symbol=symbol,
                data_age=data_age,
                timeframes=summary,
            )

    avg_support_score = sum(r["score"] for r in supporters) / len(supporters)

    # Reward agreement without pretending this is a win probability.
    multi_tf_score = avg_support_score + ((agreement_ratio - 0.5) * 12)
    multi_tf_score = max(50, min(95, multi_tf_score))

    # Prefer representative diagnostics from 5m, then 2m, then 1m,
    # otherwise use the strongest supporting timeframe.
    representative = None
    for preferred in ("5m", "2m", "1m", "10m", "15m", "30m"):
        r = results.get(preferred)
        if r and r.get("signal") == signal:
            representative = r
            break

    if representative is None:
        representative = max(supporters, key=lambda x: x.get("score", 0))

    aligned_tfs = [r["timeframe"] for r in supporters]
    opposing_tfs = [r["timeframe"] for r in opponents]

    if should_suppress_duplicate(pair, signal, summary):
        return no_signal_result(
            pair,
            "Duplicate signal suppressed; wait for a fresh timeframe context.",
            symbol=symbol,
            data_age=data_age,
            timeframes=summary,
        )

    return {
        "pair": pair,
        "score": round(multi_tf_score, 2),
        "signal": signal,
        "reason": (
            f"Multi-TF agreement: {len(supporters)}/{valid_count} valid "
            f"timeframes -> {signal}"
        ),
        "rsi": representative.get("rsi"),
        "adx": representative.get("adx"),
        "atr": representative.get("atr"),
        "price": representative.get("price"),
        "bullish_points": representative.get("bullish_points", 0),
        "bearish_points": representative.get("bearish_points", 0),
        "data_age": round(data_age, 2) if data_age is not None else None,
        "source": "Yahoo Finance",
        "source_mode": "underlying_proxy" if "(OTC)" in pair else "live_reference",
        "otc_proxy_warning": "(OTC)" in pair,
        "yahoo_symbol": symbol,
        "timeframes_scanned": list(TIMEFRAMES.keys()),
        "aligned_timeframes": aligned_tfs,
        "opposing_timeframes": opposing_tfs,
        "timeframe_summary": summary,
        "multi_tf_agreement": round(agreement_ratio * 100, 1),
        "selected_expiry": selected_expiry,
        "required_expiry_timeframe": required_tf,
        "confirmation_mode": (
            f"4-of-6 Strong + {required_tf} Required" if required_tf else "4-of-6 Strong"
        ),
        "duplicate_protection": True,
    }


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
        "automatic_outcome_tracking": list(AUTO_TRACK_EXPIRIES.keys()),
        "closed_candle_analysis": True,
    })


@app.route("/verify-license", methods=["POST"])
def verify_license():
    data = request.get_json(silent=True) or {}

    key = str(data.get("key", "")).strip()
    user = str(data.get("user", "")).strip()
    device = str(data.get("device", "")).strip()

    if not key or not user or not device:
        return jsonify({
            "status": "error",
            "message": "Key, user and device are required.",
        }), 400

    licenses = load_licenses()
    record = licenses.get(key)

    if not record or not record.get("active", False):
        return jsonify({
            "status": "error",
            "message": "Invalid or revoked license key.",
        }), 401

    bound_device = record.get("device")
    bound_user = record.get("user")

    if bound_device and bound_device != device:
        return jsonify({
            "status": "error",
            "message": "This key is already bound to another device.",
        }), 403

    if bound_user and bound_user != user:
        return jsonify({
            "status": "error",
            "message": "This key is already assigned to another user.",
        }), 403

    record["device"] = device
    record["user"] = user
    record["last_verified_at"] = int(time.time())
    licenses[key] = record
    save_licenses(licenses)

    return jsonify({
        "status": "success",
        "message": "License verified successfully.",
        "user": user,
        "device_bound": True,
    })


@app.route("/admin/generate-key", methods=["POST"])
def admin_generate_key():
    data = request.get_json(silent=True) or {}

    password = str(data.get("password", ""))
    user = str(data.get("user", "")).strip()

    if password != ADMIN_PASSWORD:
        return jsonify({
            "status": "error",
            "message": "Incorrect admin password.",
        }), 403

    if not user:
        return jsonify({
            "status": "error",
            "message": "User Telegram ID / UID is required.",
        }), 400

    licenses = load_licenses()

    while True:
        key = "RAJA-VIP-" + secrets.token_hex(4).upper() + "-2026"
        if key not in licenses:
            break

    licenses[key] = {
        "active": True,
        "user": user,
        "device": None,
        "created_at": int(time.time()),
    }
    save_licenses(licenses)

    return jsonify({
        "status": "success",
        "message": "License created.",
        "key": key,
        "user": user,
    })


@app.route("/admin/revoke-key", methods=["POST"])
def admin_revoke_key():
    data = request.get_json(silent=True) or {}

    password = str(data.get("password", ""))
    key = str(data.get("key", "")).strip()

    if password != ADMIN_PASSWORD:
        return jsonify({
            "status": "error",
            "message": "Incorrect admin password.",
        }), 403

    licenses = load_licenses()

    if key not in licenses:
        return jsonify({
            "status": "error",
            "message": "License key not found.",
        }), 404

    licenses[key]["active"] = False
    licenses[key]["revoked_at"] = int(time.time())
    save_licenses(licenses)

    return jsonify({
        "status": "success",
        "message": "License revoked.",
        "key": key,
    })



@app.route("/track-signal", methods=["POST"])
def track_signal():
    data = request.get_json(silent=True) or {}

    pair = str(data.get("pair", "")).strip()
    direction = str(data.get("signal", "")).strip().upper()
    expiry = str(data.get("expiry", "")).strip()
    score = data.get("score")
    timeframe_summary = data.get("timeframe_summary") or {}

    if pair not in YAHOO_SYMBOLS:
        return jsonify({
            "status": "error",
            "message": "Unsupported pair.",
        }), 400

    if direction not in {"CALL", "PUT"}:
        return jsonify({
            "status": "error",
            "message": "Signal must be CALL or PUT.",
        }), 400

    # Yahoo 1m cannot reliably verify 15s/30s outcomes.
    if expiry not in AUTO_TRACK_EXPIRIES:
        return jsonify({
            "status": "success",
            "auto_tracking": False,
            "message": "15s/30s outcome tracking is disabled because the Yahoo base feed is 1-minute.",
        })

    now = int(time.time())
    entry_epoch = ((now // 60) + 1) * 60
    duration = AUTO_TRACK_EXPIRIES[expiry]
    expiry_epoch = entry_epoch + duration

    signal_id = "sig_" + secrets.token_hex(8)

    item = {
        "id": signal_id,
        "pair": pair,
        "signal": direction,
        "score": float(score or 0),
        "expiry": expiry,
        "created_at": now,
        "entry_epoch": entry_epoch,
        "expiry_epoch": expiry_epoch,
        "entry_price": None,
        "exit_price": None,
        "result": None,
        "status": "PENDING",
        "source": "Yahoo Finance",
        "source_mode": "underlying_proxy" if "(OTC)" in pair else "live_reference",
        "timeframe_summary": timeframe_summary,
    }

    items = load_signals()
    items.insert(0, item)
    items = items[:500]
    save_signals(items)

    return jsonify({
        "status": "success",
        "auto_tracking": True,
        "signal_id": signal_id,
        "entry_epoch": entry_epoch,
        "expiry_epoch": expiry_epoch,
        "message": "Signal registered. Enter on the next 1-minute candle open.",
    })


@app.route("/signals/history", methods=["GET"])
def signals_history():
    try:
        limit = max(1, min(int(request.args.get("limit", 30)), 100))
    except Exception:
        limit = 30

    items = load_signals()
    return jsonify({
        "status": "success",
        "data": items[:limit],
        "stats": signal_stats(items),
    })


@app.route("/signals/stats", methods=["GET"])
def signals_stats():
    items = load_signals()
    return jsonify({
        "status": "success",
        "stats": signal_stats(items),
    })


@app.route("/scan", methods=["POST"])
def scan_markets():
    data = request.get_json(silent=True) or {}
    selected_pair = str(data.get("pair", "")).strip()
    selected_expiry = str(data.get("expiry", "")).strip()

    if not selected_pair or "Auto Scan Best Pair" in selected_pair:
        best = None

        for pair in ALL_PAIRS:
            result = calculate_live_indicators(pair, selected_expiry)
            if result.get("signal") == "NO SIGNAL":
                continue
            if best is None or result.get("score", 0) > best.get("score", 0):
                best = result

        if best is None:
            return jsonify({
                "status": "success",
                "data": {
                    "pair": None,
                    "score": 0,
                    "signal": "NO SIGNAL",
                    "reason": "No configured pair reached multi-timeframe confluence.",
                    "timeframes_scanned": list(TIMEFRAMES.keys()),
                },
            })

        return jsonify({"status": "success", "data": best})

    if selected_pair not in YAHOO_SYMBOLS:
        return jsonify({
            "status": "error",
            "message": f"Unsupported pair: {selected_pair}",
            "data": no_signal_result(
                selected_pair,
                "Pair is not configured in Yahoo mapping.",
            ),
        }), 400

    result = calculate_live_indicators(selected_pair, selected_expiry)
    return jsonify({"status": "success", "data": result})


poller_thread = threading.Thread(
    target=background_market_poller,
    daemon=True,
)
poller_thread.start()

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
