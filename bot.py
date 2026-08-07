import asyncio
import time
import yfinance as yf

# Dropdown ke tamam major forex pairs ki list (Yahoo Finance format ke mutabiq)
PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", 
    "USDCHF=X", "NZDUSD=X", "EURGBP=X", "EURJPY=X", "GBPJPY=X", 
    "AUDJPY=X", "EURAUD=X", "GBPAUD=X", "CADJPY=X", "EURCAD=X", 
    "GBPCAD=X", "NZDJPY=X", "AUDNZD=X", "EURCHF=X", "GBPCHF=X"
]

def fetch_latest_candle(symbol):
    try:
        # Har pair ka latest 1-minute data fetch karna
        data = yf.download(symbol, period="1d", interval="1m", progress=False)
        if not data.empty:
            latest = data.iloc[-1]
            return {
                "symbol": symbol,
                "close": float(latest["Close"]),
                "open": float(latest["Open"]),
                "high": float(latest["High"]),
                "low": float(latest["Low"])
            }
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
    return None

async def scan_all_pairs():
    print("Starting multi-pair live market scan for all currency pairs...")
    
    while True:
        print(f"\n--- Scan Cycle Started at {time.strftime('%H:%M:%S')} ---")
        
        for symbol in PAIRS:
            candle = fetch_latest_candle(symbol)
            
            if candle:
                current_price = candle["close"]
                clean_name = symbol.replace("=X", "")
                print(f"[{clean_name}] Live Price: {current_price}")
                
                # Yahan aap apna indicator logic run kar sakte hain
                signal = evaluate_strategy(candle)
                
                if signal:
                    print(f"⚡ SIGNAL FOUND on {clean_name}: {signal} at {current_price}")
            
            # Rate limit bachane ke liye thora gap
            await asyncio.sleep(2)
        
        print("--- Scan Cycle Completed. Waiting for next cycle... ---")
        # Tamam pairs scan hone ke baad agla cycle 60 seconds baad chalega
        await asyncio.sleep(60)

def evaluate_strategy(candle):
    # Sample logic: Agar Close price Open se zyada hai toh CALL, warna PUT
    if candle["close"] > candle["open"]:
        return "CALL (UP)"
    else:
        return "PUT (DOWN)"

if __name__ == "__main__":
    try:
        asyncio.run(scan_all_pairs())
    except KeyboardInterrupt:
        print("Bot stopped by user.")
