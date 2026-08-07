import os
import asyncio
import time
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
import yfinance as yf
import telebot

# ----------------- RENDER WEB SERVER SETUP -----------------
def run_server():
    server = HTTPServer(('0.0.0.0', 10000), SimpleHTTPRequestHandler)
    server.serve_forever()

# ----------------- TELEGRAM BOT SETUP -----------------
TOKEN = os.environ.get('BOT_TOKEN')
bot = telebot.TeleBot(TOKEN)

# Users ki chat IDs save karne ke liye set (taake duplicate na ho)
active_users = set()

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    chat_id = message.chat.id
    active_users.add(chat_id)
    print(f"New user added: {chat_id}")
    bot.reply_to(message, "Assalam-o-Alaikum! Raja AI Bot active hai. Aapko live market signals milna shuru ho jayenge.")

@bot.message_handler(func=lambda message: True)
def echo_all(message):
    chat_id = message.chat.id
    active_users.add(chat_id)
    bot.reply_to(message, "Aapka message mil gaya! Bot active hai aur background mein market scan kar raha hai.")

def run_telegram_bot():
    print("Telegram bot polling started...")
    bot.infinity_polling()

# ----------------- FOREX MARKET SCANNER SETUP -----------------
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

def evaluate_strategy(candle):
    if candle["close"] > candle["open"]:
        return "CALL (UP)"
    else:
        return "PUT (DOWN)"

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
                    signal_msg = f"⚡ SIGNAL FOUND!\nPair: {clean_name}\nSignal: {signal}\nPrice: {current_price}"
                    print(signal_msg)
                    
                    # Yahan tamam active users ko signal broadcast ho jayega
                    for chat_id in list(active_users):
                        try:
                            bot.send_message(chat_id, signal_msg)
                        except Exception as e:
                            print(f"Failed to send message to {chat_id}: {e}")
            
            await asyncio.sleep(4)
            
        print("--- Cycle completed. Waiting for next scan... ---")
        await asyncio.sleep(60)

# ----------------- MAIN PROGRAM EXECUTION -----------------
if __name__ == "__main__":
    # 1. Render web server thread start karein
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # 2. Telegram bot thread start karein
    telegram_thread = threading.Thread(target=run_telegram_bot, daemon=True)
    telegram_thread.start()
    
    # 3. Async market scanner main thread par chalayein
    try:
        asyncio.run(scan_all_pairs())
    except KeyboardInterrupt:
        print("Bot stopped.")
