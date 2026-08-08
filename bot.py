import os
import asyncio
import time
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
import yfinance as yf
import telebot

# ----------------- RENDER WEB SERVER SETUP -----------------
def run_server():
    # Render ke liye port 10000 par HTTP server chalana zaroori hai taake service active rahe
    server = HTTPServer(('0.0.0.0', 10000), SimpleHTTPRequestHandler)
    server.serve_forever()

# ----------------- TELEGRAM BOT SETUP -----------------
# Render environment variables se token aur chat ID uthayega
TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
bot = telebot.TeleBot(TOKEN)

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.reply_to(message, "Assalam-o-Alaikum! Raja AI Bot active hai aur market scan kar raha hai.")

@bot.message_handler(func=lambda message: True)
def echo_all(message):
    bot.reply_to(message, f"Aapka message mil gaya: {message.text}")

def run_telegram_bot():
    # Telegram bot ko background mein chalane ke liye
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
        return "CALL / HIGHER (BUY)"
    else:
        return "PUT / LOWER (SELL)"

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
                    
                    # Telegram par Signal bhejne ka code
                    if CHAT_ID and TOKEN:
                        if "BUY" in signal:
                            emoji = "🟢"
                        else:
                            emoji = "🔴"
                            
                        message_text = (
                            f"🚨 **Raja AI Premium Signal** 🚨\n\n"
                            f"📊 **Asset:** {clean_name} (OTC/Forex)\n"
                            f"📈 **Signal:** {emoji} **{signal}**\n"
                            f"💰 **Price:** {current_price}\n"
                            f"⏰ **Time:** {time.strftime('%H:%M:%S')}"
                        )
                        try:
                            bot.send_message(CHAT_ID, message_text, parse_mode="Markdown")
                        except Exception as ex:
                            print(f"Telegram send error: {ex}")
            
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
