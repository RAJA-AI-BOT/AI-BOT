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


@app.get("/")
def read_root():
  return {"status": "online", "message": "Raja AI Bot is running successfully!"}


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

# Global cache taake users ke liye bar bar Yahoo ko request na jaye
market_cache = {}


def fetch_latest_candle(symbol):
  try:
    data = yf.download(symbol, period="5d", interval="1m", progress=False)
    if not data.empty:
      latest = data.iloc[-1]
      # Series/DataFrame issue fix karne ke liye safely float extract karna
      close_val = (
          float(latest["Close"].iloc[0])
          if hasattr(latest["Close"], "iloc")
          else float(latest["Close"])
      )
      open_val = (
          float(latest["Open"].iloc[0])
          if hasattr(latest["Open"], "iloc")
          else float(latest["Open"])
      )
      return {"symbol": symbol, "close": close_val, "open": open_val}
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
      # Blocking yfinance call ko async thread mein chalana
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

      # Rate limit se bachne ke liye har pair ke darmiyan 4 seconds ka gap rakha hai
      await asyncio.sleep(4)

    print("--- Cycle completed. Waiting for next background refresh... ---")
    await asyncio.sleep(30)


@app.on_event("startup")
async def startup_event():
  asyncio.create_task(scan_all_pairs())


@app.get("/get-signals")
async def get_signals():
  if not market_cache:
    return {"status": "loading", "message": "Data is loading, please wait..."}
  return {"status": "success", "data": market_cache}


# ----------------- MAIN PROGRAM EXECUTION -----------------
if __name__ == "__main__":
  port = int(os.environ.get("PORT", 10000))
  uvicorn.run(app, host="0.0.0.0", port=port)
