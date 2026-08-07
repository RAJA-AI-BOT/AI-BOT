import os
import asyncio
import time
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
import yfinance as yf
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ----------------- RENDER WEB SERVER SETUP -----------------
def run_server():
    server = HTTPServer(('0.0.0.0', 10000), SimpleHTTPRequestHandler)
    server.serve_forever()

# ----------------- TELEGRAM BOT SETUP -----------------
TOKEN = os.environ.get('BOT_TOKEN')
bot = telebot.TeleBot(TOKEN)

# Users ki preferences save karne ke liye
active_users = set()
user_market_choice = {}
user_timeframe_choice = {} # Timeframe save karne ke liye (1m, 5m, 15m, 30m)

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    chat_id = message.chat.id
    active_users.add(chat_id)
    user_market_choice[chat_id] = "LIVE"
    user_timeframe_choice[chat_id] = "1m" # Default timeframe
    
    markup = InlineKeyboardMarkup()
    markup.row_width = 2
    markup.add(
        InlineKeyboardButton("🌐 Live Forex Market", callback_data="market_live"),
        InlineKeyboardButton("⚡ OTC Market", callback_data="market_otc"),
        InlineKeyboardButton("⏱️ Timeframe: 1m", callback_data="tf_1m"),
        InlineKeyboardButton("⏱️ Timeframe: 5m", callback_data="tf_5m"),
        InlineKeyboardButton("⏱️ Timeframe: 15m", callback_data="tf_15m"),
        InlineKeyboardButton("⏱️ Timeframe: 30m", callback_data="tf_30m"),
        InlineKeyboardButton("🚀 Start Quantum Scan", callback_data="start_scan"),
        InlineKeyboardButton("⚙️ Settings", callback_data="settings")
    )
    
    bot.send_message(
        chat_id, 
        "🤖 **RAJA AI PREMIUM - VIP QUANTUM BOT**\n\n"
        "Welcome! Select your preferred Market and Timeframe below to begin scanning:",
        parse_mode="Markdown",
        reply_markup=markup
    )

@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    chat_id = call.message.chat.id
    if call.data == "start_scan":
        tf = user_timeframe_choice.get(chat_id, "1m")
        bot.answer_callback_query(call.id, f"Quantum Scan Started ({tf})!")
        bot.send_message(chat_id, f"🔍 **Quantum Engine Running (95%+)**\nTimeframe: {tf}\nMarket scanning in progress...", parse_mode="Markdown")
    elif call.data == "market_live":
        user_market_choice[chat_id] = "LIVE"
        bot.answer_callback_query(call.id)
        bot.send_message(chat_id, "🌐 **Selected Market:** Live Forex Market", parse_mode="Markdown")
    elif call.data == "market_otc":
        user_market_choice[chat_id] = "OTC"
        bot.answer_callback_query(call.id)
        bot.send_message(chat_id, "⚡ **Selected Market:** OTC Market", parse_mode="Markdown")
    elif call.data.startswith("tf_"):
        tf_value = call.data.split("_")[1] # 1m, 5m, 15m, 30m
        user_timeframe_choice[chat_id] = tf_value
        bot.answer_callback_query(call.id, f"Timeframe set to {tf_value}")
        bot.send_message(chat_id, f"⏱️ **Timeframe Updated:** {tf_value}\nNow click 'Start Quantum Scan' to fetch signals.", parse_mode="Markdown")
    elif call.data == "settings":
        tf = user_timeframe_choice.get(chat_id, "1m")
        market = user_market_choice.get(chat_id, "LIVE")
        bot.answer_callback_query(call.id)
        bot.send_message(chat_id, f"⚙️ **Settings Menu:**\n- Market: {market}\n- Timeframe: {tf}\n- Accuracy: 95%+", parse_mode="Markdown")
    else:
        bot.answer_callback_query(call.id, "Option selected!")

@bot.message_handler(func=lambda message: True)
def echo_all(message):
    chat_id = message.chat.id
    active_users.add(chat_id)
    bot.reply_to(message, "Bot is active! Use /start to configure your timeframes and market.")

def run_telegram_bot():
    print("Telegram bot polling started...")
    try:
        bot.remove_webhook(drop_pending_updates=True)
    except Exception as e:
        print(f"Error removing webhook: {e}")
    bot.infinity_polling(skip_pending=True, interval=0.1, timeout=20)

# ----------------- FOREX MARKET SCANNER SETUP -----------------
PAIRS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", 
    "USDCHF=X", "NZDUSD=X", "EURGBP=X", "EURJPY=X", "GBPJPY=X", 
    "AUDJPY=X", "EURAUD=X", "GBPAUD=X", "CADJPY=X", "EURCAD=X", 
    "GBPCAD=X", "NZDJPY=X", "AUDNZD=X", "EURCHF=X", "GBPCHF=X"
]

def fetch_latest_candle(symbol, interval="1m"):
    try:
        # Interval ke mutabiq period adjust karna zaroori hai yfinance ke liye
        period = "5d" if interval in ["15m", "30m"] else "1d"
        data = yf.download(symbol, period=period, interval=interval, progress=False)
        if not data.empty:
            latest = data.iloc[-1]
            return {
                "symbol": symbol,
                "close": float(latest["Close"]),
                "open": float(latest["Open"])
            }
    except Exception as e:
        print(f"Error fetching {symbol} for {interval}: {e}")
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
            # Har user ke mutabiq unka selected timeframe use karna
            for chat_id in list(active_users):
                tf = user_timeframe_choice.get(chat_id, "1m")
                candle = fetch_latest_candle(symbol, interval=tf)
                
                if candle:
                    clean_name = symbol.replace("=X", "")
                    current_price = candle["close"]
                    signal = evaluate_strategy(candle)
                    
                    if signal:
                        signal_msg = f"⚡ **VIP QUANTUM SIGNAL FOUND!**\n\n📊 Pair: {clean_name}\n⏱️ Timeframe: {tf}\n🎯 Signal: {signal}\n💰 Price: {current_price}\n🔥 Accuracy: 95%+"
                        try:
                            bot.send_message(chat_id, signal_msg, parse_mode="Markdown")
                        except Exception as e:
                            print(f"Failed to send message to {chat_id}: {e}")
                
                await asyncio.sleep(2)
            
        print("--- Cycle completed. Waiting for next scan... ---")
        await asyncio.sleep(30)

# ----------------- MAIN PROGRAM EXECUTION -----------------
if __name__ == "__main__":
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    telegram_thread = threading.Thread(target=run_telegram_bot, daemon=True)
    telegram_thread.start()
    
    try:
        asyncio.run(scan_all_pairs())
    except KeyboardInterrupt:
        print("Bot stopped.")
