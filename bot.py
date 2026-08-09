import os
import asyncio
import time
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
import yfinance as yf

# ----------------- RENDER WEB SERVER SETUP -----------------
def run_server():
    server = HTTPServer(('0.0.0.0', 10000), SimpleHTTPRequestHandler)
    server.serve_forever()

# ----------------- LIVE MARKET SCANNER SETUP (FOREX & CRYPTO) -----------------
PAIRS = [
    # Forex Live Pairs
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", 
    "USDCHF=X", "NZDUSD=X", "EURGBP=X", "EURJPY=X", "GBPJPY=X", 
    "AUDJPY=X", "EURAUD=X", "GBPAUD=X", "CADJPY=X", "EURCAD=X", 
    "GBPCAD=X", "NZDJPY=X", "AUDNZD=X", "EURCHF=X", "GBPCHF=X",
    # Crypto Live Pairs
    "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "ADA-USD"
]

def fetch_latest_candle(symbol):
    try:
        data = yf.download(symbol, period="1d", interval="1m", progress=False)
        if not data.empty:
            latest = data.iloc[-1]
            return {
                "symbol": symbol,
                "close": float(latest["Close"]),
                "open": float(latest["Open"])
            }
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
    return None

def evaluate_strategy(candle):
    if candle["close"] > candle["open"]:
        return "CALL (UP)"
    else:
        return "PUT (DOWN)"

async def scan_all_pairs():
    print("Starting full dropdown pairs live market scan (Forex & Crypto)...")
    while True:
        print(f"\n--- Scan Cycle Started at {time.strftime('%H:%M:%S')} ---")
        
        for symbol in PAIRS:
            candle = fetch_latest_candle(symbol)
            if candle:
                clean_name = symbol.replace("=X", "").replace("-USD", "/USD")
                current_price = candle["close"]
                print(f"[{clean_name}] Live Price: {current_price}")
                
                signal = evaluate_strategy(candle)
                if signal:
                    print(f"⚡ SIGNAL FOUND on {clean_name}: {signal} at {current_price}")
            
            await asyncio.sleep(4)
            
        print("--- Cycle completed. Waiting for next scan... ---")
        await asyncio.sleep(60)

# ----------------- MAIN PROGRAM EXECUTION -----------------
if __name__ == "__main__":
    # 1. Render web server thread start karein taake service active rahe
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # 2. Async market scanner main thread par chalayein
    try:
        asyncio.run(scan_all_pairs())
    except KeyboardInterrupt:
        print("Bot stopped.")
