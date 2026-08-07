import asyncio
import time
import yfinance as yf
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot is running successfully!")

def run_server():
    server = HTTPServer(('0.0.0.0', 10000), SimpleHandler)
    server.serve_forever()

# Dropdown ke tamam pairs ki mukammal list
PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", 
    "USDCHF=X", "NZDUSD=X", "EURGBP=X", "EURJPY=X", "GBPJPY=X", 
    "AUDJPY=X", "EURAUD=X", "GBPAUD=X", "CADJPY=X", "EURCAD=X", 
    "GBPCAD=X", "NZDJPY=X", "AUDNZD=X", "EURCHF=X", "GBPCHF=X"
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

async def scan_all_pairs():
    print("Starting full dropdown pairs live market scan...")
    while True:
        print(f"\n--- Scan Cycle Started at {time.strftime('%H:%M:%S')} ---")
        
        for symbol in PAIRS:
            candle = fetch_latest_candle(symbol)
            if candle:
                clean_name = symbol.replace("=X", "")
                current_price = candle["close"]
                print(f"[{clean_name}] Live Price: {current_price}")
                
                signal = evaluate_strategy(candle)
                if signal:
                    print(f"⚡ SIGNAL FOUND on {clean_name}: {signal} at {current_price}")
            
            # Rate limit se bachne ke liye delay (4 seconds har pair ke baad)
            await asyncio.sleep(4)
            
        print("--- Cycle completed. Waiting for next scan... ---")
        # Rate limit control karne ke liye cycle gap ko 60 seconds kar diya hai
        await asyncio.sleep(60)

def evaluate_strategy(candle):
    if candle["close"] > candle["open"]:
        return "CALL (UP)"
    else:
        return "PUT (DOWN)"

if __name__ == "__main__":
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    try:
        asyncio.run(scan_all_pairs())
    except KeyboardInterrupt:
        print("Bot stopped.")
