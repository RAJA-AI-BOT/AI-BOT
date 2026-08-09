import asyncio
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
import threading
import time
from fastapi import FastAPI
import uvicorn
import yfinance as yf

# ----------------- RENDER WEB SERVER SETUP -----------------
app = FastAPI()


def run_http_server():
  server = HTTPServer(("0.0.0.0", 10000), SimpleHTTPRequestHandler)
  server.serve_forever()


# ----------------- LIVE MARKET SCANNER & CACHING SETUP -----------------
PAIRS = [
    # Forex Live Pairs
    "EURUSD=X",
    "GBPUSD=X",
    "USDJPY=X",
    "AUDUSD=X",
    "USDCAD=X",
    "USDCHF=X",
    "NZDUSD=X",
    "EURGBP=X",
    "EURJPY=X",
    "GBPJPY=X",
    "AUDJPY=X",
    "EURAUD=X",
    "GBPAUD=X",
    "CADJPY=X",
    "EURCAD=X",
    "GBPCAD=X",
    "NZDJPY=X",
    "AUDNZD=X",
    "EURCHF=X",
    "GBPCHF=X",
    # Crypto Live Pairs
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
    "XRP-USD",
    "DOGE-USD",
    "ADA-USD",
]

# Global cache taake 50+ users ke liye bar bar Yahoo ko request na jaye
market_cache = {}


def fetch_latest_candle(symbol):
  try:
    data = yf.download(symbol, period="1d", interval="1m", progress=False)
    if not data.empty:
      latest = data.iloc[-1]
      return {
          "symbol": symbol,
          "close": float(latest["Close"]),
          "open": float(latest["Open"]),
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
  print("Starting background live market cache scanner (Forex & Crypto)...")
  while True:
    print(f"\n--- Scan Cycle Started at {time.strftime('%H:%M:%S')} ---")

    for symbol in PAIRS:
      # Blocking yfinance call ko async thread mein chalana taake server block na ho
      candle = await asyncio.to_thread(fetch_latest_candle, symbol)
      if candle:
        clean_name = symbol.replace("=X", "").replace("-USD", "/USD")
        current_price = candle["close"]
        signal = evaluate_strategy(candle)

        # Cache mein data save karna
        market_cache[symbol] = {
            "clean_name": clean_name,
            "price": current_price,
            "signal": signal,
            "time": time.strftime("%H:%M:%S"),
        }
        print(f"[{clean_name}] Cached -> Price: {current_price} | Signal: {signal}")

      await asyncio.sleep(2)

    print("--- Cycle completed. Waiting for next background refresh... ---")
    await asyncio.sleep(60)


@app.on_event("startup")
async def startup_event():
  # Server start hote hi background cache updater chala dein
  asyncio.create_task(scan_all_pairs())


@app.get("/get-signals")
async def get_signals():
  """Jab 50+ users ek sath request karenge, sab ko foran cache se data milega[cite: 14]"""
  if not market_cache:
    return {"status": "loading", "message": "Data is loading, please wait..."}
  return {"status": "success", "data": market_cache}


# ----------------- MAIN PROGRAM EXECUTION -----------------
if __name__ == "__main__":
  # Render ke diye gaye PORT ko automatically uthana (default 10000)
  port = int(os.environ.get("PORT", 10000))

  # FastAPI server ko Uvicorn ke zariye directly Render port par chalana
  uvicorn.run(app, host="0.0.0.0", port=port)
