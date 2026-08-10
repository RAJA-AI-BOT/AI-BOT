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
# RAJA AI BACKEND
# Yahoo Finance = live/reference underlying market source.
# IMPORTANT: "(OTC)" instruments are NOT Quotex OTC quotes.
# They use the corresponding Yahoo underlying-market proxy.
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

# 1-minute Yahoo candles update around minute boundaries, so a cache window
# slightly above one minute avoids unnecessary repeated downloads.
CACHE_DURATION = 90
market_cache = {}          # keyed by Yahoo symbol
cache_lock = threading.Lock()

# =========================================================
# LICENSE STORE
# =========================================================

BASE_DIR = Path(__file__).resolve().parent
LICENSE_FILE = BASE_DIR / "licenses.json"
license_lock = threading.Lock()
ADMIN_PASSWORD = os.environ.get("RAJA_ADMIN_PASSWORD", "786")

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
            return dict(DEFAULT_LICENSES)

        try:
            data = json.loads(LICENSE_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("Invalid license database")
        except Exception:
            data = dict(DEFAULT_LICENSES)

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
# MARKET DATA
# =========================================================

def fetch_yahoo_data(symbol):
    """Fetch 1-minute OHLCV candles from Yahoo Finance."""
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
    if len(df) < 60:
        return None

    return df


def update_symbol_cache(symbol):
    try:
        df = fetch_yahoo_data(symbol)
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

    # On-demand refresh prevents a pair from silently using stale/wrong data.
    update_symbol_cache(symbol)

    with cache_lock:
        cached = market_cache.get(symbol)

    if not cached:
        return None, None, symbol

    age = time.time() - cached["timestamp"]
    return cached["data"].copy(), age, symbol


def background_market_poller():
    """Continuously pre-warm unique Yahoo symbols.

    OTC/live aliases sharing the same Yahoo symbol reuse one cached DataFrame.
    """
    while True:
        for symbol in UNIQUE_YAHOO_SYMBOLS:
            update_symbol_cache(symbol)
            time.sleep(0.75)

        time.sleep(5)


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

    # Avoid a direct pandas import; Series.combine is sufficient here.
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
        if value != value:  # NaN
            return default
        return value
    except Exception:
        return default


def no_signal_result(pair, reason, symbol=None, data_age=None):
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
        "source_mode": (
            "underlying_proxy" if "(OTC)" in pair else "live_reference"
        ),
        "otc_proxy_warning": "(OTC)" in pair,
        "yahoo_symbol": symbol,
    }


def calculate_live_indicators(pair):
    if pair not in YAHOO_SYMBOLS:
        return no_signal_result(
            pair,
            "Pair is not configured in Yahoo mapping.",
        )

    df, data_age, symbol = get_market_data(pair)

    if df is None or df.empty:
        return no_signal_result(
            pair,
            "Yahoo market data unavailable.",
            symbol=symbol,
            data_age=data_age,
        )

    if data_age is not None and data_age > CACHE_DURATION * 2:
        return no_signal_result(
            pair,
            "Yahoo market data is stale.",
            symbol=symbol,
            data_age=data_age,
        )

    required = {"Open", "High", "Low", "Close"}
    if not required.issubset(df.columns) or len(df) < 60:
        return no_signal_result(
            pair,
            "Insufficient OHLC candle history.",
            symbol=symbol,
            data_age=data_age,
        )

    df = df.copy().dropna(subset=list(required))
    if len(df) < 60:
        return no_signal_result(
            pair,
            "Insufficient clean candle history.",
            symbol=symbol,
            data_age=data_age,
        )

    close = df["Close"]

    # 1) RSI
    rsi_series = calculate_rsi(close, 14)
    current_rsi = safe_float(rsi_series.iloc[-1])

    # 2) EMA trend
    ema9 = calculate_ema(close, 9)
    ema21 = calculate_ema(close, 21)
    ema50 = calculate_ema(close, 50)

    current_price = safe_float(close.iloc[-1])
    e9 = safe_float(ema9.iloc[-1])
    e21 = safe_float(ema21.iloc[-1])
    e50 = safe_float(ema50.iloc[-1])

    # 3) MACD
    macd, macd_signal = calculate_macd(close)
    current_macd = safe_float(macd.iloc[-1])
    current_macd_signal = safe_float(macd_signal.iloc[-1])

    # 4) Bollinger Bands
    bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(close)
    current_upper = safe_float(bb_upper.iloc[-1])
    current_middle = safe_float(bb_middle.iloc[-1])
    current_lower = safe_float(bb_lower.iloc[-1])

    # 5) ATR
    atr_series = calculate_atr(df, 14)
    current_atr = safe_float(atr_series.iloc[-1])

    # 6) ADX + DI direction
    adx_series, plus_di_series, minus_di_series = calculate_adx_components(df, 14)
    current_adx = safe_float(adx_series.iloc[-1])
    current_plus_di = safe_float(plus_di_series.iloc[-1])
    current_minus_di = safe_float(minus_di_series.iloc[-1])

    required_values = [
        current_rsi, current_price, e9, e21, e50,
        current_macd, current_macd_signal,
        current_upper, current_middle, current_lower,
        current_atr, current_adx,
        current_plus_di, current_minus_di,
    ]

    if any(v is None for v in required_values):
        return no_signal_result(
            pair,
            "Indicator values are not ready yet.",
            symbol=symbol,
            data_age=data_age,
        )

    if current_atr <= 0:
        return no_signal_result(
            pair,
            "Invalid volatility data.",
            symbol=symbol,
            data_age=data_age,
        )

    ema_bullish = current_price > e9 > e21 > e50
    ema_bearish = current_price < e9 < e21 < e50

    macd_bullish = current_macd > current_macd_signal
    macd_bearish = current_macd < current_macd_signal

    bb_bullish = current_price > current_middle
    bb_bearish = current_price < current_middle

    adx_bullish = current_adx >= 20 and current_plus_di > current_minus_di
    adx_bearish = current_adx >= 20 and current_minus_di > current_plus_di

    # 7) Momentum + candle/wicks
    previous_close = safe_float(close.iloc[-2])
    last = df.iloc[-1]

    candle_open = safe_float(last["Open"])
    candle_high = safe_float(last["High"])
    candle_low = safe_float(last["Low"])
    candle_close = safe_float(last["Close"])

    if None in (
        previous_close, candle_open, candle_high,
        candle_low, candle_close
    ):
        return no_signal_result(
            pair,
            "Latest candle is incomplete.",
            symbol=symbol,
            data_age=data_age,
        )

    momentum_bullish = current_price > previous_close
    momentum_bearish = current_price < previous_close

    candle_range = candle_high - candle_low
    if candle_range <= 0:
        return no_signal_result(
            pair,
            "Latest candle has invalid range.",
            symbol=symbol,
            data_age=data_age,
        )

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

    # 8) Volume confirmation when Yahoo supplies usable volume.
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
    reasons_bull = []
    reasons_bear = []

    # RSI is used as momentum context, not a guaranteed reversal signal.
    if 52 <= current_rsi <= 70:
        bullish_points += 1.0
        reasons_bull.append("RSI bullish momentum")
    elif 30 <= current_rsi <= 48:
        bearish_points += 1.0
        reasons_bear.append("RSI bearish momentum")

    if ema_bullish:
        bullish_points += 2.0
        reasons_bull.append("EMA bullish alignment")
    elif ema_bearish:
        bearish_points += 2.0
        reasons_bear.append("EMA bearish alignment")

    if macd_bullish:
        bullish_points += 1.0
        reasons_bull.append("MACD bullish")
    elif macd_bearish:
        bearish_points += 1.0
        reasons_bear.append("MACD bearish")

    if bb_bullish:
        bullish_points += 1.0
    elif bb_bearish:
        bearish_points += 1.0

    if adx_bullish:
        bullish_points += 1.5
        reasons_bull.append("ADX/+DI trend")
    elif adx_bearish:
        bearish_points += 1.5
        reasons_bear.append("ADX/-DI trend")

    if momentum_bullish:
        bullish_points += 1.0
    elif momentum_bearish:
        bearish_points += 1.0

    if bullish_rejection:
        bullish_points += 1.0
        reasons_bull.append("bullish wick rejection")
    elif bearish_rejection:
        bearish_points += 1.0
        reasons_bear.append("bearish wick rejection")
    elif bullish_candle:
        bullish_points += 0.5
    elif bearish_candle:
        bearish_points += 0.5

    if volume_bullish:
        bullish_points += 1.0
        reasons_bull.append("volume confirmation")
    elif volume_bearish:
        bearish_points += 1.0
        reasons_bear.append("volume confirmation")

    point_difference = abs(bullish_points - bearish_points)
    winning_points = max(bullish_points, bearish_points)

    # Strict no-trade filter.
    if point_difference < 2.0 or winning_points < 4.0:
        result = no_signal_result(
            pair,
            "Indicators are conflicting or confluence is too weak.",
            symbol=symbol,
            data_age=data_age,
        )
        result.update({
            "rsi": round(current_rsi, 2),
            "adx": round(current_adx, 2),
            "atr": round(current_atr, 8),
            "price": round(current_price, 8),
            "bullish_points": round(bullish_points, 2),
            "bearish_points": round(bearish_points, 2),
        })
        return result

    if bullish_points > bearish_points:
        signal = "CALL"
        reason_parts = reasons_bull[:4]
    else:
        signal = "PUT"
        reason_parts = reasons_bear[:4]

    # Technical confluence score — NOT historical win-rate/accuracy.
    score = 50 + (point_difference * 6)

    if current_adx >= 20:
        score += min(current_adx - 20, 15) * 0.5

    score = max(50, min(95, score))

    if current_adx < 15:
        score = min(score, 68)

    source_mode = "underlying_proxy" if "(OTC)" in pair else "live_reference"

    return {
        "pair": pair,
        "score": round(score, 2),
        "signal": signal,
        "reason": ", ".join(reason_parts) if reason_parts else "Technical confluence",
        "rsi": round(current_rsi, 2),
        "ema9": round(e9, 8),
        "ema21": round(e21, 8),
        "ema50": round(e50, 8),
        "macd": round(current_macd, 8),
        "macd_signal": round(current_macd_signal, 8),
        "bb_upper": round(current_upper, 8),
        "bb_middle": round(current_middle, 8),
        "bb_lower": round(current_lower, 8),
        "adx": round(current_adx, 2),
        "plus_di": round(current_plus_di, 2),
        "minus_di": round(current_minus_di, 2),
        "atr": round(current_atr, 8),
        "price": round(current_price, 8),
        "bullish_points": round(bullish_points, 2),
        "bearish_points": round(bearish_points, 2),
        "data_age": round(data_age, 2) if data_age is not None else None,
        "source": "Yahoo Finance",
        "source_mode": source_mode,
        "otc_proxy_warning": "(OTC)" in pair,
        "yahoo_symbol": symbol,
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
        "service": "RAJA AI backend",
        "yahoo_pairs": len(YAHOO_SYMBOLS),
        "unique_yahoo_symbols": len(UNIQUE_YAHOO_SYMBOLS),
        "cached_symbols": cached_symbols,
        "cache_duration_seconds": CACHE_DURATION,
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


@app.route("/scan", methods=["POST"])
def scan_markets():
    data = request.get_json(silent=True) or {}
    selected_pair = str(data.get("pair", "")).strip()

    # The current frontend normally scans each pair itself.
    # This branch is retained for direct API use.
    if (
        not selected_pair
        or "Auto Scan Best Pair" in selected_pair
    ):
        best = None

        for pair in ALL_PAIRS:
            result = calculate_live_indicators(pair)
            if result["signal"] == "NO SIGNAL":
                continue

            if best is None or result["score"] > best["score"]:
                best = result

        if best is None:
            return jsonify({
                "status": "success",
                "data": {
                    "pair": None,
                    "score": 0,
                    "signal": "NO SIGNAL",
                    "reason": "No configured pair reached valid confluence.",
                },
            })

        return jsonify({
            "status": "success",
            "data": best,
        })

    if selected_pair not in YAHOO_SYMBOLS:
        return jsonify({
            "status": "error",
            "message": f"Unsupported pair: {selected_pair}",
            "data": no_signal_result(
                selected_pair,
                "Pair is not configured in Yahoo mapping.",
            ),
        }), 400

    result = calculate_live_indicators(selected_pair)

    return jsonify({
        "status": "success",
        "data": result,
    })


# Start background cache warmer only after all functions/routes exist.
poller_thread = threading.Thread(
    target=background_market_poller,
    daemon=True,
)
poller_thread.start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        threaded=True,
    )
