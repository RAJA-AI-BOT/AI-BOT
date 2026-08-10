from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import yfinance as yf
import pandas as pd
import os
import time
import threading

app = Flask(__name__, static_folder='.', template_folder='.')
CORS(app)  # Frontend se connection error hatane ke liye zaroori hai

# Live Yahoo Finance supported pairs mapping
YAHOO_SYMBOLS = {
    'BTC-USD': 'BTC-USD',
    'ETH-USD': 'ETH-USD',
    'SOL-USD': 'SOL-USD',
    'LTC-USD': 'LTC-USD',
    'XRP-USD': 'XRP-USD',
    'ADA-USD': 'ADA-USD',
    'DOGE-USD': 'DOGE-USD',
    'Bitcoin (OTC)': 'BTC-USD',
    'Ethereum (OTC)': 'ETH-USD',
    'Litecoin (OTC)': 'LTC-USD',
    'Ripple (OTC)': 'XRP-USD',
    'Solana (OTC)': 'SOL-USD',
    'Toncoin (OTC)': 'TON-USD',
    'Ethereum Classic (OTC)': 'ETC-USD',
    'Axie Infinity (OTC)': 'AXS-USD',
    'Binance Coin (OTC)': 'BNB-USD',
    'Trump (OTC)': 'TRUMP-USD',
    'Polkadot (OTC)': 'DOT-USD',
    'Avalanche (OTC)': 'AVAX-USD',
    'Chainlink (OTC)': 'LINK-USD',
    'Bitcoin Cash (OTC)': 'BCH-USD',
    'Zcash (OTC)': 'ZEC-USD',
    'Cosmos (OTC)': 'ATOM-USD',
    'EUR/USD': 'EURUSD=X',
    'GBP/USD': 'GBPUSD=X',
    'USD/JPY': 'USDJPY=X',
    'AUD/USD': 'AUDUSD=X',
    'USD/CAD': 'USDCAD=X',
    'USD/CHF': 'USDCHF=X',
    'NZD/USD': 'NZDUSD=X',
    'EUR/GBP': 'EURGBP=X',
    'EUR/JPY': 'EURJPY=X',
    'GBP/JPY': 'GBPJPY=X',
    'AUD/JPY': 'AUDJPY=X',
    'EUR/AUD': 'EURAUD=X',
    'GBP/AUD': 'GBPAUD=X',
    'CAD/JPY': 'CADJPY=X',
    'EUR/CAD': 'EURCAD=X',
    'GBP/CAD': 'GBPCAD=X',
    'NZD/JPY': 'NZDJPY=X',
    'AUD/NZD': 'AUDNZD=X',
    'EUR/CHF': 'EURCHF=X',
    'GBP/CHF': 'GBPCHF=X',
    'XAUUSD': 'XAUUSD=X',
    'EUR/USD (OTC)': 'EURUSD=X',
    'GBP/USD (OTC)': 'GBPUSD=X',
    'USD/JPY (OTC)': 'USDJPY=X',
    'AUD/USD (OTC)': 'AUDUSD=X',
    'USD/CAD (OTC)': 'USDCAD=X',
    'USD/CHF (OTC)': 'USDCHF=X',
    'NZD/USD (OTC)': 'NZDUSD=X',
    'EUR/GBP (OTC)': 'EURGBP=X',
    'EUR/JPY (OTC)': 'EURJPY=X',
    'GBP/JPY (OTC)': 'GBPJPY=X',
    'AUD/JPY (OTC)': 'AUDJPY=X',
    'EUR/AUD (OTC)': 'EURAUD=X',
    'GBP/AUD (OTC)': 'GBPAUD=X',
    'CAD/JPY (OTC)': 'CADJPY=X',
    'EUR/CAD (OTC)': 'EURCAD=X',
    'GBP/CAD (OTC)': 'GBPCAD=X',
    'NZD/JPY (OTC)': 'NZDJPY=X',
    'AUD/NZD (OTC)': 'AUDNZD=X',
    'EUR/CHF (OTC)': 'EURCHF=X',
    'GBP/CHF (OTC)': 'GBPCHF=X',
    'NZD/CAD (OTC)': 'NZDCAD=X',
    'NZD/CHF (OTC)': 'NZDCHF=X',
    'USD/BRL (OTC)': 'USDBRL=X',
    'USD/ARS (OTC)': 'USDARS=X',
    'USD/INR (OTC)': 'USDINR=X',
}

# Saare pairs ki list jo scan honge
OTC_PAIRS = list(YAHOO_SYMBOLS.keys())

# --- CENTRALIZED CACHING & BACKGROUND POLLING ---
market_cache = {}
CACHE_DURATION = 30  # Data freshness duration in seconds

def background_market_poller():
    """Continuously fetch 1-minute OHLCV candles from Yahoo Finance and cache them.
    Yahoo Finance remains the live/reference market-data source.
    """
    while True:
        for pair, symbol in YAHOO_SYMBOLS.items():
            try:
                ticker = yf.Ticker(symbol)
                df = ticker.history(
                    period="5d",
                    interval="1m",
                    auto_adjust=False
                )

                if df is not None and not df.empty:
                    df = df.dropna(subset=["Open", "High", "Low", "Close"])

                    if not df.empty:
                        market_cache[pair] = {
                            "data": df.copy(),
                            "timestamp": time.time()
                        }

            except Exception as e:
                print(f"Background fetch error for {pair}: {e}")

            time.sleep(2)  # Request spacing to reduce Yahoo rate-limit pressure

        time.sleep(5)

# Background polling thread start karein
poller_thread = threading.Thread(target=background_market_poller, daemon=True)
poller_thread.start()

# --- SERVER-SIDE LICENSE VERIFICATION ---
VALID_VIP_KEYS = ["RAJA-VIP-2026-X99", "RAJA-VIP-PRO-777", "RAJA-AI-MASTERKEY"]

@app.route('/verify-license', methods=['POST'])
def verify_license():
    data = request.json or {}
    user_key = data.get('key', '').strip()
    
    if user_key in VALID_VIP_KEYS:
        return jsonify({
            "status": "success",
            "message": "License verified successfully!"
        })
    else:
        return jsonify({
            "status": "error",
            "message": "Invalid or expired license key!"
        }), 401

@app.route('/')
def home():
    # Ab yeh root URL aapki index.html file serve karega agar woh root directory me hai
    if os.path.exists('index.html'):
        return send_from_directory('.', 'index.html')
    return "Raja AI Bot Backend with Centralized Caching and Secure License Verification is Running Successfully!"

@app.route('/admin')
def admin_panel():
    # Admin page serve karne ke liye route (admin.html file hona zaroori hai)
    if os.path.exists('admin.html'):
        return send_from_directory('.', 'admin.html')
    return "Admin Panel HTML file not found in directory!"

@app.route('/scan', methods=['POST'])
def scan_markets():
    data = request.json or {}
    selected_pair = data.get('pair', 'Auto Scan Best Pair (AI)')
    
    best_signal = None
    highest_score = 0
    
    # Agar Auto Scan selected hai toh saari market/pairs par loop chalega
    if selected_pair == "Auto Scan Best Pair (AI)" or not selected_pair:
        for pair in OTC_PAIRS:
            score, signal_type = calculate_live_8_indicators_cached(pair) 
            if score > highest_score:
                highest_score = score
                best_signal = {
                    "pair": pair,
                    "score": score,
                    "signal": signal_type
                }
    else:
        # Clean pair name matching
        clean_pair = selected_pair.replace(" (OTC)", "").strip()
        if clean_pair not in YAHOO_SYMBOLS:
            clean_pair = "EUR/USD" # Default fallback
            
        score, signal_type = calculate_live_8_indicators_cached(clean_pair)
        best_signal = {
            "pair": selected_pair,
            "score": score,
            "signal": signal_type
        }

    return jsonify({
        "status": "success",
        "data": best_signal
    })

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
    upper = middle + std_dev * std
    lower = middle - std_dev * std
    return upper, middle, lower


def calculate_atr(df, period=14):
    previous_close = df["Close"].shift(1)

    tr = pd.concat(
        [
            df["High"] - df["Low"],
            (df["High"] - previous_close).abs(),
            (df["Low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def calculate_adx(df, period=14):
    high = df["High"]
    low = df["Low"]
    close = df["Close"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = up_move.where(
        (up_move > down_move) & (up_move > 0), 0.0
    )
    minus_dm = down_move.where(
        (down_move > up_move) & (down_move > 0), 0.0
    )

    previous_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    plus_di = 100 * plus_dm.ewm(
        alpha=1 / period, adjust=False, min_periods=period
    ).mean() / atr.replace(0, 1e-12)

    minus_di = 100 * minus_dm.ewm(
        alpha=1 / period, adjust=False, min_periods=period
    ).mean() / atr.replace(0, 1e-12)

    dx = (
        100
        * (plus_di - minus_di).abs()
        / (plus_di + minus_di).replace(0, 1e-12)
    )

    return dx.ewm(
        alpha=1 / period, adjust=False, min_periods=period
    ).mean()


def calculate_live_8_indicators_cached(pair):
    """Real indicator engine using Yahoo Finance OHLCV data from the live cache.

    The returned score is a technical confluence score, NOT a proven historical
    win-rate. Historical accuracy will be added separately through backtesting.
    """
    cached_data = market_cache.get(pair)

    if not cached_data:
        return 0, "NO SIGNAL"

    symbol = YAHOO_SYMBOLS.get(pair)
    if not symbol:
        return 0, "NO SIGNAL"

    data_age = time.time() - cached_data["timestamp"]

    if data_age > CACHE_DURATION:
        return 0, "NO SIGNAL"

    df = cached_data.get("data")

    if df is None or df.empty or len(df) < 60:
        return 0, "NO SIGNAL"

    required_columns = {"Open", "High", "Low", "Close"}
    if not required_columns.issubset(df.columns):
        return 0, "NO SIGNAL"

    df = df.copy().dropna(subset=list(required_columns))

    if len(df) < 60:
        return 0, "NO SIGNAL"

    close = df["Close"]

    # 1) RSI
    rsi = calculate_rsi(close, 14)
    current_rsi = float(rsi.iloc[-1])

    # 2) EMA trend
    ema9 = calculate_ema(close, 9)
    ema21 = calculate_ema(close, 21)
    ema50 = calculate_ema(close, 50)

    current_price = float(close.iloc[-1])

    ema_bullish = (
        current_price > float(ema9.iloc[-1])
        and float(ema9.iloc[-1]) > float(ema21.iloc[-1])
        and float(ema21.iloc[-1]) > float(ema50.iloc[-1])
    )

    ema_bearish = (
        current_price < float(ema9.iloc[-1])
        and float(ema9.iloc[-1]) < float(ema21.iloc[-1])
        and float(ema21.iloc[-1]) < float(ema50.iloc[-1])
    )

    # 3) MACD
    macd, macd_signal = calculate_macd(close)
    current_macd = float(macd.iloc[-1])
    current_macd_signal = float(macd_signal.iloc[-1])

    macd_bullish = current_macd > current_macd_signal
    macd_bearish = current_macd < current_macd_signal

    # 4) Bollinger Bands
    bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(close)
    current_upper = float(bb_upper.iloc[-1])
    current_middle = float(bb_middle.iloc[-1])
    current_lower = float(bb_lower.iloc[-1])

    bb_bullish = current_price > current_middle
    bb_bearish = current_price < current_middle

    # 5) ATR volatility
    atr = calculate_atr(df, 14)
    current_atr = float(atr.iloc[-1])

    if current_atr <= 0:
        return 0, "NO SIGNAL"

    # 6) ADX trend strength
    adx = calculate_adx(df, 14)
    current_adx = float(adx.iloc[-1])

    # 7) Price momentum / candle structure
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
        return 0, "NO SIGNAL"

    bullish_candle = candle_close > candle_open
    bearish_candle = candle_close < candle_open

    upper_wick = candle_high - max(candle_open, candle_close)
    lower_wick = min(candle_open, candle_close) - candle_low

    bullish_rejection = (
        lower_wick / candle_range >= 0.25
        and candle_close > candle_open
    )

    bearish_rejection = (
        upper_wick / candle_range >= 0.25
        and candle_close < candle_open
    )

    # 8) Volume confirmation where the Yahoo feed provides usable volume.
    volume_bullish = False
    volume_bearish = False

    if "Volume" in df.columns:
        volume = df["Volume"].fillna(0)

        if float(volume.iloc[-1]) > 0:
            average_volume = float(volume.rolling(20).mean().iloc[-1])

            if average_volume > 0:
                volume_bullish = (
                    bullish_candle
                    and float(volume.iloc[-1]) > average_volume
                )
                volume_bearish = (
                    bearish_candle
                    and float(volume.iloc[-1]) > average_volume
                )

    bullish_points = 0.0
    bearish_points = 0.0

    # RSI: avoid blindly treating every high/low as a reversal.
    if 30 <= current_rsi <= 45:
        bullish_points += 1.0
    elif 55 <= current_rsi <= 70:
        bearish_points += 1.0

    # EMA alignment
    if ema_bullish:
        bullish_points += 2.0
    elif ema_bearish:
        bearish_points += 2.0

    # MACD
    if macd_bullish:
        bullish_points += 1.0
    elif macd_bearish:
        bearish_points += 1.0

    # Bollinger position
    if bb_bullish:
        bullish_points += 1.0
    elif bb_bearish:
        bearish_points += 1.0

    # ADX confirms strength, while DI direction is approximated by EMA alignment.
    if current_adx >= 20:
        if ema_bullish:
            bullish_points += 1.0
        elif ema_bearish:
            bearish_points += 1.0

    # Momentum
    if momentum_bullish:
        bullish_points += 1.0
    elif momentum_bearish:
        bearish_points += 1.0

    # Candle/rejection
    if bullish_rejection:
        bullish_points += 1.0
    elif bearish_rejection:
        bearish_points += 1.0
    elif bullish_candle:
        bullish_points += 0.5
    elif bearish_candle:
        bearish_points += 0.5

    # Volume
    if volume_bullish:
        bullish_points += 1.0
    elif volume_bearish:
        bearish_points += 1.0

    # Strongly conflicting market = no trade.
    point_difference = abs(bullish_points - bearish_points)

    if point_difference < 2.0:
        return 0, "NO SIGNAL"

    if bullish_points > bearish_points:
        signal = "CALL"
        winning_points = bullish_points
        losing_points = bearish_points
    else:
        signal = "PUT"
        winning_points = bearish_points
        losing_points = bullish_points

    # Technical confluence score only. Do NOT label this as historical accuracy.
    score = 50 + (point_difference * 6)
    score += min(max(current_adx - 20, 0), 15) * 0.5
    score = max(50, min(95, score))

    # Don't issue a high score when the trend is weak.
    if current_adx < 15:
        score = min(score, 68)

    return round(score, 2), signal

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
