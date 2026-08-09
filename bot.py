import asyncio
import os
import time
from fastapi import FastAPI
import uvicorn
import yfinance as yf

app = FastAPI()

@app.get("/")
def read_root():
    return {"status": "online", "message": "Raja AI Bot is running successfully!"}

PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X",
    "USDCHF=X", "NZDUSD=X", "EURGBP=X", "EURJPY=X", "GBPJPY=X",
    "AUDJPY=X", "EURAUD=X", "GBPAUD=X", "CADJPY=X", "EURCAD=X",
    "GBPCAD=X", "NZDJPY=X", "AUDNZD=X", "EURCHF=X", "GBPCHF=X",
    "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "ADA-USD"
]

def fetch_latest_candle(symbol):
    try:
        data = yf.download(symbol, period="5d", interval="1m", progress=False)
        if not data.empty:
            latest = data.iloc[-1]
            close_val = float(latest["Close"].iloc[0]) if hasattr(latest["Close"], "iloc") else float(latest["Close"])
            open_val = float(latest["Open"].iloc[0]) if hasattr(latest["Open"], "iloc") else float(latest["Open"])
            return {"symbol": symbol, "close": close_val, "open": open_val}
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
    return None

def evaluate_strategy(candle):
    if candle["close"] > candle["open"]:
        return "CALL (UP)"
    else:
        return "PUT (DOWN)"

# Jab bhi koi /get-signals link par click karega, tab yeh function chalega
@app.get("/get-signals")
async def get_signals():
    market_data = {}
    print(f"\n--- On-Demand Scan Started at {time.strftime('%H:%M:%S')} ---")
    
    for symbol in PAIRS:
        candle = await asyncio.to_thread(fetch_latest_candle, symbol)
        if candle:
            clean_name = symbol.replace("=X", "").replace("-USD", "/USD")
            current_price = candle["close"]
            signal = evaluate_strategy(candle)
            
            market_data[symbol] = {
                "clean_name": clean_name,
                "price": current_price,
                "signal": signal,
                "time": time.strftime("%H:%M:%S")
            }
            
    return {"status": "success", "data": market_data}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
