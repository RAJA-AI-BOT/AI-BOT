import os
import time
import threading
from datetime import datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler
import yfinance as yf
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

TOKEN = os.environ.get('BOT_TOKEN')
bot = telebot.TeleBot(TOKEN)

# Extended Market Lists
MARKETS = {
    "forex_live": ["EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X", "NZDUSD=X", "EURGBP=X", "EURJPY=X", "GBPJPY=X", "AUDJPY=X", "EURAUD=X", "GBPAUD=X", "CADJPY=X", "EURCAD=X", "GBPCAD=X", "NZDJPY=X", "AUDNZD=X", "EURCHF=X"],
    "forex_otc": ["NZDCAD_OTC", "NZDUSD_OTC", "NZDCHF_OTC", "USDBRL_OTC", "NZDJPY_OTC", "USDARS_OTC", "USDINR_OTC", "USDCAD_OTC", "USDDZD_OTC", "USDNGN_OTC", "USDPHP_OTC", "USDIDR_OTC", "USDEGP_OTC", "USDMXN_OTC", "USDPKR_OTC", "GBPNZD_OTC", "USDBDT_OTC", "USDCOP_OTC", "CADCHF_OTC"],
    "crypto_live": ["BTC-USD", "ETH-USD", "SOL-USD", "LTC-USD", "XRP-USD", "ADA-USD", "DOGE-USD", "AVAX-USD", "DOT-USD", "PAXG-USD"],
    "crypto_otc": ["BTC_OTC", "ETH_OTC", "SOL_OTC", "XRP_OTC", "ADA_OTC", "DOGE_OTC"],
    "commodities": ["GC=X", "SI=X", "CL=X", "NG=X", "BZ=X"]
}

# Mapping for Display Names
NAMES = {
    "EURUSD=X": "EUR/USD", "GBPUSD=X": "GBP/USD", "USDJPY=X": "USD/JPY", "AUDUSD=X": "AUD/USD", 
    "USDCAD=X": "USD/CAD", "USDCHF=X": "USD/CHF", "NZDUSD=X": "NZD/USD", "EURGBP=X": "EUR/GBP", 
    "EURJPY=X": "EUR/JPY", "GBPJPY=X": "GBP/JPY", "AUDJPY=X": "AUD/JPY", "EURAUD=X": "EUR/AUD", 
    "GBPAUD=X": "GBP/AUD", "CADJPY=X": "CAD/JPY", "EURCAD=X": "EUR/CAD", "GBPCAD=X": "GBP/CAD", 
    "NZDJPY=X": "NZD/JPY", "AUDNZD=X": "AUD/NZD", "EURCHF=X": "EUR/CHF",
    
    "NZDCAD_OTC": "NZD/CAD OTC", "NZDUSD_OTC": "NZD/USD OTC", "NZDCHF_OTC": "NZD/CHF OTC", 
    "USDBRL_OTC": "USD/BRL OTC", "NZDJPY_OTC": "NZD/JPY OTC", "USDARS_OTC": "USD/ARS OTC", 
    "USDINR_OTC": "USD/INR OTC", "USDCAD_OTC": "USD/CAD OTC", "USDDZD_OTC": "USD/DZD OTC", 
    "USDNGN_OTC": "USD/NGN OTC", "USDPHP_OTC": "USD/PHP OTC", "USDIDR_OTC": "USD/IDR OTC", 
    "USDEGP_OTC": "USD/EGP OTC", "USDMXN_OTC": "USD/MXN OTC", "USDPKR_OTC": "USD/PKR OTC", 
    "GBPNZD_OTC": "GBP/NZD OTC", "USDBDT_OTC": "USD/BDT OTC", "USDCOP_OTC": "USD/COP OTC", 
    "CADCHF_OTC": "CAD/CHF OTC",
    
    "BTC-USD": "Bitcoin (BTC)", "ETH-USD": "Ethereum (ETH)", "SOL-USD": "Solana (SOL)", 
    "LTC-USD": "Litecoin (LTC)", "XRP-USD": "Ripple (XRP)", "ADA-USD": "Cardano (ADA)", 
    "DOGE-USD": "Dogecoin (DOGE)", "AVAX-USD": "Avalanche (AVAX)", "DOT-USD": "Polkadot (DOT)", 
    "PAXG-USD": "Gold Crypto (PAXG)"
}

user_sessions = {}

# --- Bot Handlers (Logic remains same as updated above) ---

def run_server():
    server = HTTPServer(('0.0.0.0', 10000), SimpleHTTPRequestHandler)
    server.serve_forever()

# Start Server
if __name__ == "__main__":
    threading.Thread(target=run_server, daemon=True).start()
    
    # Clear old webhooks and pending updates to prevent 409 Conflict
    try:
        bot.remove_webhook()
        time.sleep(1)
    except Exception:
        pass
        
    bot.infinity_polling(skip_pending=True)
