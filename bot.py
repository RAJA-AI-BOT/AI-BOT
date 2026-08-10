from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import yfinance as yf
import pandas as pd
import os
import time
import threading
import json
import secrets
from pathlib import Path

app = Flask(__name__, static_folder='.', template_folder='.')
CORS(app)

# =========================================================
# CONFIG
# =========================================================
CACHE_DURATION = 45
POLL_DELAY_PER_SYMBOL = 1.2
POLL_ROUND_DELAY = 5
LICENSE_FILE = Path(os.environ.get("LICENSE_FILE", "licenses.json"))
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "786")

# =========================================================
# YAHOO FINANCE SYMBOL MAPPING
# 69 frontend assets preserved exactly.
# OTC entries use the underlying Yahoo market as a PROXY,
# not the broker's private OTC quote stream.
# =========================================================
YAHOO_SYMBOLS = {
    # --- Crypto Live (7) ---
    "BTC-USD": "BTC-USD",
    "ETH-USD": "ETH-USD",
    "SOL-USD": "SOL-USD",
    "LTC-USD": "LTC-USD",
    "XRP-USD": "XRP-USD",
    "ADA-USD": "ADA-USD",
    "DOGE-USD": "DOGE-USD",

    # --- Crypto OTC (16) : underlying Yahoo proxy ---
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

    # --- Forex Live (21) ---
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
    # Yahoo spot-gold coverage is inconsistent. Gold futures are used as a Yahoo proxy.
    "XAUUSD": "GC=F",

    # --- Forex OTC (25) : underlying Yahoo proxy ---
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

# =========================================================
# LICENSE STORAGE / DEVICE BINDING
# =========================================================
license_lock = threading.Lock()

DEFAULT_KEYS = {
    "RAJA-VIP-2026-X99": {"active": True, "user": None, "device": None},
    "RAJA-VIP-PRO-777": {"active": True, "user": None, "device": None},
    "RAJA-AI-MASTERKEY": {"active": True, "user": None, "device": None},
}


def load_licenses():
    with license_lock:
        if not LICENSE_FILE.exists():
            LICENSE_FILE.write_text(json.dumps(DEFAULT_KEYS, indent=2), encoding="utf-8")
            return dict(DEFAULT_KEYS)
        try:
            data = json.loads(LICENSE_FILE.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                raise ValueError("Invalid license database")
            return data
        except Exception:
            return dict(DEFAULT_KEYS)


def save_licenses(data):
    with license_lock:
        LICENSE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


@app.route('/verify-license', methods=['POST'])
def verify_license():
    data = request.get_json(silent=True) or {}
    key = str(data.get('key', '')).strip()
    user = str(data.get('user', '')).strip()
    device = str(data.get('device', '')).strip()

    if not key or not user or not device:
        return jsonify({"status": "error", "message": "Key, user and device are required."}), 400

    licenses = load_licenses()
    record = licenses.get(key)

    if not record or not record.get("active", False):
        return jsonify({"status": "error", "message": "Invalid or inactive license key."}), 401

    bound_device = record.get("device")
    if bound_device and bound_device != device:
        return jsonify({"status": "error", "message": "License is already bound to another device."}), 403

    # First successful verification binds key to this device.
    if not bound_device:
        record["device"] = device
        record["user"] = user
        record["bound_at"] = int(time.time())
        licenses[key] = record
        save_licenses(licenses)

    return jsonify({
        "status": "success",
        "message": "License verified successfully.",
        "device_bound": True
    })


@app.route('/admin/generate-key', methods=['POST'])
def admin_generate_key():
    data = request.get_json(silent=True) or {}
    password = str(data.get("password", ""))
    user = str(data.get("user", "")).strip()

    if password != ADMIN_PASSWORD:
        return jsonify({"status": "error", "message": "Invalid admin password."}), 403
    if not user:
        return jsonify({"status": "error", "message": "User/Telegram ID is required."}), 400

    key = "RAJA-VIP-" + secrets.token_hex(4).upper() + "-2026"
    licenses = load_licenses()
    licenses[key] = {
        "active": True,
        "user": user,
        "device": None,
        "created_at": int(time.time())
    }
    save_licenses(licenses)

    return jsonify({"status": "success", "key": key, "user": user})


@app.route('/admin/revoke-key', methods=['POST'])
def admin_revoke_key():
    data = request.get_json(silent=True) or {}
    password = str(data.get("password", ""))
    key = str(data.get("key", "")).strip()

    if password != ADMIN_PASSWORD:
        return jsonify({"status": "error", "message": "Invalid admin password."}), 403

    licenses = load_licenses()
    if key not in licenses:
        return jsonify({"status": "error", "message": "License key not found."}), 404

    licenses[key]["active"] = False
    save_licenses(licenses)
    return jsonify({"status": "success", "message": "License revoked."})


# =========================================================
# MARKET CACHE
# =========================================================
market_cache = {}
market_cache_lock = threading.Lock()


def background_market_poller():
    """Poll unique Yahoo symbols and copy results to every frontend alias."""
    reverse_map = {}
    for pair, symbol in YAHOO_SYMBOLS.items():
        reverse_map.setdefault(symbol, []).append(pair)

    while True:
        for symbol, aliases in reverse_map.items():
            try:
                ticker = yf.Ticker(symbol)
                df = ticker.history(period="5d", interval="1m", auto_adjust=False)

                if df is not None and not df.empty:
                    df = df.dropna(subset=["Open", "High", "Low", "Close"])
                    if not df.empty:
                        now = time.time()
                        with market_cache_lock:
                            for pair in aliases:
                                market_cache[pair] = {
                                    "data": df.copy(),
                                    "timestamp": now,
                                    "yahoo_symbol": symbol,
                                }
            except Exception as exc:
                print(f"Yahoo fetch error for {symbol}: {exc}")

            time.sleep(POLL_DELAY_PER_SYMBOL)

        time.sleep(POLL_ROUND_DELAY)


poller_thread = threading.Thread(target=background_market_poller, daemon=True)
poller_thread.start()


# =========================================================
# TECHNICAL INDICATORS
# =========================================================
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, 1e-12)
    return 100 - (100 / (1 + rs))


def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False, min_periods=period).mean()


def calculate_macd(series):
    ema12 = calculate_ema(series, 12)
    ema26 = calculate_ema(series, 26)
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False, min_periods=9).mean()
    return macd, signal


def calculate_bollinger_bands(series, period=20, std_dev=2):
    middle = series.rolling(period).mean()
    std = series.rolling(period).std()
    return middle + std_dev * std, middle, middle - std_dev * std


def calculate_atr(df, period=14):
    previous_close = df["Close"].shift(1)
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - previous_close).abs(),
        (df["Low"] - previous_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def calculate_adx(df, period=14):
    high, low, close = df["High"], df["Low"], df["Close"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    previous_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - previous_close).abs(),
        (low - previous_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0, 1e-12)
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0, 1e-12)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1e-12)
    return dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def finite(value):
    return pd.notna(value) and value not in (float("inf"), float("-inf"))


def analyze_pair(pair):
    if pair not in YAHOO_SYMBOLS:
        return None, "Pair is not configured in Yahoo mapping."

    with market_cache_lock:
        cached = market_cache.get(pair)

    if not cached:
        return None, "Yahoo data is not cached yet."

    data_age = time.time() - cached["timestamp"]
    if data_age > CACHE_DURATION:
        return None, "Yahoo data is stale."

    df = cached.get("data")
    if df is None or df.empty or len(df) < 60:
        return None, "Insufficient 1-minute candle history."

    required = {"Open", "High", "Low", "Close"}
    if not required.issubset(df.columns):
        return None, "Required OHLC columns are missing."

    df = df.copy().dropna(subset=list(required))
    if len(df) < 60:
        return None, "Insufficient clean candle history."

    close = df["Close"]
    rsi = calculate_rsi(close, 14)
    ema9 = calculate_ema(close, 9)
    ema21 = calculate_ema(close, 21)
    ema50 = calculate_ema(close, 50)
    macd, macd_signal = calculate_macd(close)
    bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(close)
    atr = calculate_atr(df, 14)
    adx = calculate_adx(df, 14)

    current_price = float(close.iloc[-1])
    current_rsi = float(rsi.iloc[-1])
    current_macd = float(macd.iloc[-1])
    current_macd_signal = float(macd_signal.iloc[-1])
    current_middle = float(bb_middle.iloc[-1])
    current_atr = float(atr.iloc[-1])
    current_adx = float(adx.iloc[-1])

    essential = [current_price, current_rsi, current_macd, current_macd_signal, current_middle, current_atr, current_adx]
    if not all(finite(x) for x in essential):
        return None, "Indicator calculation is not ready."
    if current_atr <= 0:
        return None, "Invalid volatility reading."

    e9, e21, e50 = float(ema9.iloc[-1]), float(ema21.iloc[-1]), float(ema50.iloc[-1])
    ema_bullish = current_price > e9 > e21 > e50
    ema_bearish = current_price < e9 < e21 < e50
    macd_bullish = current_macd > current_macd_signal
    macd_bearish = current_macd < current_macd_signal
    bb_bullish = current_price > current_middle
    bb_bearish = current_price < current_middle

    previous_close = float(close.iloc[-2])
    momentum_bullish = current_price > previous_close
    momentum_bearish = current_price < previous_close

    last = df.iloc[-1]
    candle_open = float(last["Open"])
    candle_high = float(last["High"])
    candle_low = float(last["Low"])
    candle_close = float(last["Close"])
    candle_range = candle_high - candle_low
    if candle_range <= 0:
        return None, "Invalid candle range."

    bullish_candle = candle_close > candle_open
    bearish_candle = candle_close < candle_open
    upper_wick = candle_high - max(candle_open, candle_close)
    lower_wick = min(candle_open, candle_close) - candle_low
    bullish_rejection = lower_wick / candle_range >= 0.25 and bullish_candle
    bearish_rejection = upper_wick / candle_range >= 0.25 and bearish_candle

    volume_bullish = False
    volume_bearish = False
    volume_available = False
    if "Volume" in df.columns:
        volume = df["Volume"].fillna(0)
        current_volume = float(volume.iloc[-1])
        average_volume = float(volume.rolling(20).mean().iloc[-1]) if len(volume) >= 20 else 0
        if current_volume > 0 and average_volume > 0:
            volume_available = True
            volume_bullish = bullish_candle and current_volume > average_volume
            volume_bearish = bearish_candle and current_volume > average_volume

    bullish_points = 0.0
    bearish_points = 0.0

    if 30 <= current_rsi <= 45:
        bullish_points += 1.0
    elif 55 <= current_rsi <= 70:
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

    if current_adx >= 20:
        if ema_bullish:
            bullish_points += 1.0
        elif ema_bearish:
            bearish_points += 1.0

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

    point_difference = abs(bullish_points - bearish_points)
    if point_difference < 2.0:
        signal = "NO SIGNAL"
        score = 0.0
        reason = "Indicators are conflicting or too weak."
    else:
        signal = "CALL" if bullish_points > bearish_points else "PUT"
        score = 50 + point_difference * 6
        score += min(max(current_adx - 20, 0), 15) * 0.5
        score = max(50, min(95, score))
        if current_adx < 15:
            score = min(score, 68)
        reason = "Bullish confluence" if signal == "CALL" else "Bearish confluence"

    is_otc = "(OTC)" in pair
    return {
        "pair": pair,
        "yahoo_symbol": cached.get("yahoo_symbol", YAHOO_SYMBOLS[pair]),
        "source": "Yahoo Finance",
        "source_mode": "underlying_proxy" if is_otc else "live_reference",
        "otc_proxy_warning": is_otc,
        "signal": signal,
        "score": round(float(score), 2),
        "score_label": "Technical Confluence",
        "reason": reason,
        "price": round(current_price, 8),
        "rsi": round(current_rsi, 2),
        "ema9": round(e9, 8),
        "ema21": round(e21, 8),
        "ema50": round(e50, 8),
        "macd": round(current_macd, 8),
        "macd_signal": round(current_macd_signal, 8),
        "bb_middle": round(current_middle, 8),
        "atr": round(current_atr, 8),
        "adx": round(current_adx, 2),
        "bullish_points": bullish_points,
        "bearish_points": bearish_points,
        "volume_available": volume_available,
        "data_age": round(data_age, 2),
    }, None


# =========================================================
# API ROUTES
# =========================================================
@app.route('/')
def home():
    if os.path.exists('index.html'):
        return send_from_directory('.', 'index.html')
    return "Raja AI backend is running. Put index.html in the same folder."


@app.route('/health', methods=['GET'])
def health():
    with market_cache_lock:
        cached_count = len(market_cache)
    return jsonify({
        "status": "ok",
        "configured_pairs": len(YAHOO_SYMBOLS),
        "cached_pairs": cached_count,
        "cache_duration_seconds": CACHE_DURATION,
        "source": "Yahoo Finance"
    })


@app.route('/pairs', methods=['GET'])
def pairs():
    return jsonify({"status": "success", "pairs": ALL_PAIRS})


@app.route('/scan', methods=['POST'])
def scan_markets():
    data = request.get_json(silent=True) or {}
    selected_pair = str(data.get('pair', '')).strip()

    if not selected_pair:
        return jsonify({"status": "error", "message": "Pair is required."}), 400

    # Frontend normally performs category Auto Scan itself by calling /scan for each pair.
    # This endpoint still supports a global auto-scan for compatibility.
    if "Auto Scan" in selected_pair:
        best = None
        failures = 0
        for pair in ALL_PAIRS:
            result, error = analyze_pair(pair)
            if error or not result or result["signal"] == "NO SIGNAL":
                failures += 1
                continue
            if best is None or result["score"] > best["score"]:
                best = result

        if best is None:
            return jsonify({
                "status": "success",
                "data": None,
                "message": "No valid signal found.",
                "failed_or_no_signal": failures
            })
        return jsonify({"status": "success", "data": best})

    if selected_pair not in YAHOO_SYMBOLS:
        return jsonify({
            "status": "error",
            "message": f"Unsupported pair: {selected_pair}. No fallback pair was used."
        }), 400

    result, error = analyze_pair(selected_pair)
    if error:
        return jsonify({
            "status": "success",
            "data": {
                "pair": selected_pair,
                "signal": "NO SIGNAL",
                "score": 0,
                "score_label": "Technical Confluence",
                "reason": error,
                "source": "Yahoo Finance"
            }
        })

    return jsonify({"status": "success", "data": result})


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
