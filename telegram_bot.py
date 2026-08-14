from flask import request, jsonify
import os
import json
import time
import threading
import hashlib
import html
from pathlib import Path
from urllib.request import Request as UrlRequest, urlopen
from urllib.error import HTTPError, URLError

try:
    import psycopg
except Exception:
    psycopg = None

# =========================================================
# RAJA AI TELEGRAM GATEWAY
# - Token is read ONLY from TELEGRAM_BOT_TOKEN.
# - Admin can be claimed once with TELEGRAM_ADMIN_SETUP_CODE.
# - Webhook is protected by Telegram's secret-token header.
# - Telegram users reuse the same market-analysis callbacks as the web app.
# =========================================================

BOT_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
BOT_USERNAME = (os.environ.get("TELEGRAM_BOT_USERNAME") or "Raja_Aii_bot").strip().lstrip("@")
SUPPORT_USERNAME = (os.environ.get("TELEGRAM_SUPPORT_USERNAME") or "RAJASIGNALAIPREMIUM").strip().lstrip("@")
PARTNER_URL = (os.environ.get("RAJA_QUOTEX_PARTNER_URL") or "https://broker-qx.pro/sign-up/?lid=2209395").strip()
PUBLIC_BASE_URL = (os.environ.get("RAJA_PUBLIC_BASE_URL") or "https://raja-ai-bot.onrender.com").strip().rstrip("/")
ADMIN_SETUP_CODE = (os.environ.get("TELEGRAM_ADMIN_SETUP_CODE") or "").strip()
ADMIN_ID_ENV = (os.environ.get("TELEGRAM_ADMIN_ID") or "").strip()
DATABASE_URL = (os.environ.get("DATABASE_URL") or os.environ.get("RAJA_DATABASE_URL") or "").strip()
DATA_DIR = Path(os.environ.get("RAJA_DATA_DIR", str(Path(__file__).resolve().parent))).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
USERS_FILE = DATA_DIR / "telegram_users.json"
META_FILE = DATA_DIR / "telegram_meta.json"
STORE_LOCK = threading.RLock()

WEBHOOK_SECRET = (os.environ.get("TELEGRAM_WEBHOOK_SECRET") or "").strip()
if not WEBHOOK_SECRET and BOT_TOKEN:
    WEBHOOK_SECRET = hashlib.sha256(("raja-telegram:" + BOT_TOKEN).encode("utf-8")).hexdigest()[:48]

TELEGRAM_API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}" if BOT_TOKEN else ""

MARKET_PAIRS = {
    "CryptoLive": ["BTC-USD", "ETH-USD", "SOL-USD", "LTC-USD", "XRP-USD", "ADA-USD", "DOGE-USD"],
    "CryptoOTC": [
        "Bitcoin (OTC)", "Ethereum (OTC)", "Litecoin (OTC)", "Ripple (OTC)", "Solana (OTC)",
        "Toncoin (OTC)", "Ethereum Classic (OTC)", "Axie Infinity (OTC)", "Binance Coin (OTC)",
        "Polkadot (OTC)", "Avalanche (OTC)", "Chainlink (OTC)", "Bitcoin Cash (OTC)", "Zcash (OTC)", "Cosmos (OTC)"
    ],
    "ForexLive": [
        "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD", "USD/CHF", "NZD/USD", "EUR/GBP", "EUR/JPY",
        "GBP/JPY", "AUD/JPY", "EUR/AUD", "GBP/AUD", "CAD/JPY", "EUR/CAD", "GBP/CAD", "NZD/JPY", "AUD/NZD",
        "EUR/CHF", "GBP/CHF", "XAUUSD"
    ],
    "ForexOTC": [
        "USD/BRL (OTC)", "NZD/CHF (OTC)", "NZD/JPY (OTC)", "USD/COP (OTC)", "USD/MXN (OTC)", "AUD/NZD (OTC)",
        "USD/BDT (OTC)", "USD/DZD (OTC)", "USD/NGN (OTC)", "USD/PHP (OTC)", "USD/PKR (OTC)", "USD/ZAR (OTC)",
        "USD/INR (OTC)", "USD/EGP (OTC)", "USD/IDR (OTC)", "USD/ARS (OTC)", "GBP/NZD (OTC)", "EUR/NZD (OTC)",
        "NZD/USD (OTC)", "NZD/CAD (OTC)", "CAD/CHF (OTC)"
    ],
}

MARKET_LABELS = {
    "CryptoLive": "🪙 Crypto Live",
    "CryptoOTC": "🪙 Crypto OTC Proxy",
    "ForexLive": "💱 Forex Live",
    "ForexOTC": "⚡ Forex OTC Proxy",
}

VALID_EXPIRIES = ["1m", "2m", "5m", "15m", "30m"]


def _db_connect():
    if not DATABASE_URL or psycopg is None:
        return None
    return psycopg.connect(DATABASE_URL, connect_timeout=10)


def _read_json(path, fallback):
    try:
        if not path.exists():
            return fallback
        data = json.loads(path.read_text(encoding="utf-8"))
        return data
    except Exception:
        return fallback


def _write_json(path, data):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def init_telegram_store():
    """Create Telegram-specific persistent tables without changing the web-license schema."""
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS raja_telegram_users (
                            telegram_id BIGINT PRIMARY KEY,
                            chat_id BIGINT NOT NULL,
                            username TEXT,
                            first_name TEXT,
                            last_name TEXT,
                            submitted_id TEXT,
                            license_key TEXT,
                            status TEXT NOT NULL DEFAULT 'NEW',
                            stage TEXT NOT NULL DEFAULT 'START',
                            market TEXT,
                            pair TEXT,
                            expiry TEXT,
                            created_at BIGINT,
                            updated_at BIGINT,
                            approved_at BIGINT
                        )
                    """)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS raja_telegram_meta (
                            meta_key TEXT PRIMARY KEY,
                            meta_value TEXT
                        )
                    """)
            return
        except Exception as exc:
            print(f"Telegram DB initialization warning: {exc}")
    with STORE_LOCK:
        if not USERS_FILE.exists():
            _write_json(USERS_FILE, {})
        if not META_FILE.exists():
            _write_json(META_FILE, {})


def get_meta(key, default=None):
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT meta_value FROM raja_telegram_meta WHERE meta_key=%s", (key,))
                    row = cur.fetchone()
                    return row[0] if row else default
        except Exception as exc:
            print(f"Telegram meta read warning: {exc}")
    with STORE_LOCK:
        return _read_json(META_FILE, {}).get(key, default)


def set_meta(key, value):
    value = str(value)
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO raja_telegram_meta(meta_key, meta_value)
                        VALUES(%s,%s)
                        ON CONFLICT(meta_key) DO UPDATE SET meta_value=EXCLUDED.meta_value
                    """, (key, value))
            return True
        except Exception as exc:
            print(f"Telegram meta write warning: {exc}")
    with STORE_LOCK:
        data = _read_json(META_FILE, {})
        data[key] = value
        _write_json(META_FILE, data)
    return True


def _default_user(telegram_id, chat_id=None):
    now = int(time.time())
    return {
        "telegram_id": int(telegram_id),
        "chat_id": int(chat_id or telegram_id),
        "username": "",
        "first_name": "",
        "last_name": "",
        "submitted_id": "",
        "license_key": "",
        "status": "NEW",
        "stage": "START",
        "market": "",
        "pair": "",
        "expiry": "1m",
        "created_at": now,
        "updated_at": now,
        "approved_at": None,
    }


def get_user(telegram_id, chat_id=None):
    telegram_id = int(telegram_id)
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT telegram_id,chat_id,username,first_name,last_name,submitted_id,license_key,
                               status,stage,market,pair,expiry,created_at,updated_at,approved_at
                        FROM raja_telegram_users WHERE telegram_id=%s
                    """, (telegram_id,))
                    row = cur.fetchone()
            if row:
                keys = ["telegram_id","chat_id","username","first_name","last_name","submitted_id","license_key",
                        "status","stage","market","pair","expiry","created_at","updated_at","approved_at"]
                return dict(zip(keys, row))
        except Exception as exc:
            print(f"Telegram user read warning: {exc}")
    else:
        with STORE_LOCK:
            data = _read_json(USERS_FILE, {})
            item = data.get(str(telegram_id))
            if isinstance(item, dict):
                return item
    return _default_user(telegram_id, chat_id)


def save_user(user):
    user = dict(user)
    user["telegram_id"] = int(user["telegram_id"])
    user["chat_id"] = int(user.get("chat_id") or user["telegram_id"])
    user["updated_at"] = int(time.time())
    user.setdefault("created_at", user["updated_at"])
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO raja_telegram_users(
                            telegram_id,chat_id,username,first_name,last_name,submitted_id,license_key,
                            status,stage,market,pair,expiry,created_at,updated_at,approved_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT(telegram_id) DO UPDATE SET
                            chat_id=EXCLUDED.chat_id, username=EXCLUDED.username, first_name=EXCLUDED.first_name,
                            last_name=EXCLUDED.last_name, submitted_id=EXCLUDED.submitted_id,
                            license_key=EXCLUDED.license_key, status=EXCLUDED.status, stage=EXCLUDED.stage,
                            market=EXCLUDED.market, pair=EXCLUDED.pair, expiry=EXCLUDED.expiry,
                            updated_at=EXCLUDED.updated_at, approved_at=EXCLUDED.approved_at
                    """, (
                        user["telegram_id"], user["chat_id"], user.get("username"), user.get("first_name"),
                        user.get("last_name"), user.get("submitted_id"), user.get("license_key"), user.get("status", "NEW"),
                        user.get("stage", "START"), user.get("market"), user.get("pair"), user.get("expiry", "1m"),
                        user.get("created_at"), user.get("updated_at"), user.get("approved_at")
                    ))
            return user
        except Exception as exc:
            print(f"Telegram user write warning: {exc}")
    with STORE_LOCK:
        data = _read_json(USERS_FILE, {})
        data[str(user["telegram_id"])] = user
        _write_json(USERS_FILE, data)
    return user


def pending_users(limit=20):
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT telegram_id,chat_id,username,first_name,last_name,submitted_id,license_key,
                               status,stage,market,pair,expiry,created_at,updated_at,approved_at
                        FROM raja_telegram_users
                        WHERE status='PENDING'
                        ORDER BY updated_at ASC LIMIT %s
                    """, (int(limit),))
                    rows = cur.fetchall()
            keys = ["telegram_id","chat_id","username","first_name","last_name","submitted_id","license_key",
                    "status","stage","market","pair","expiry","created_at","updated_at","approved_at"]
            return [dict(zip(keys, row)) for row in rows]
        except Exception as exc:
            print(f"Telegram pending read warning: {exc}")
    with STORE_LOCK:
        data = _read_json(USERS_FILE, {})
        items = [x for x in data.values() if isinstance(x, dict) and x.get("status") == "PENDING"]
        items.sort(key=lambda x: x.get("updated_at", 0))
        return items[:limit]


def approved_users(limit=100):
    """Return active Telegram users for the admin-only license list."""
    if DATABASE_URL and psycopg is not None:
        try:
            with _db_connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT telegram_id,chat_id,username,first_name,last_name,submitted_id,license_key,
                               status,stage,market,pair,expiry,created_at,updated_at,approved_at
                        FROM raja_telegram_users
                        WHERE status='ACTIVE'
                        ORDER BY approved_at DESC NULLS LAST, updated_at DESC
                        LIMIT %s
                    """, (int(limit),))
                    rows = cur.fetchall()
            keys = ["telegram_id","chat_id","username","first_name","last_name","submitted_id","license_key",
                    "status","stage","market","pair","expiry","created_at","updated_at","approved_at"]
            return [dict(zip(keys, row)) for row in rows]
        except Exception as exc:
            print(f"Telegram approved read warning: {exc}")
    with STORE_LOCK:
        data = _read_json(USERS_FILE, {})
        items = [x for x in data.values() if isinstance(x, dict) and str(x.get("status") or "").upper() == "ACTIVE"]
        items.sort(key=lambda x: (x.get("approved_at") or 0, x.get("updated_at") or 0), reverse=True)
        return items[:limit]


def admin_id():
    if ADMIN_ID_ENV.isdigit():
        return int(ADMIN_ID_ENV)
    saved = str(get_meta("admin_telegram_id", "") or "").strip()
    return int(saved) if saved.isdigit() else None


def tg_api(method, payload=None, timeout=20):
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
    payload = payload or {}
    data = json.dumps(payload).encode("utf-8")
    req = UrlRequest(
        f"{TELEGRAM_API_BASE}/{method}",
        data=data,
        headers={"Content-Type": "application/json", "User-Agent": "RAJA-AI-Telegram/1.0"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
        result = json.loads(body)
        if not result.get("ok"):
            raise RuntimeError(result.get("description") or f"Telegram {method} failed")
        return result.get("result")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
        raise RuntimeError(f"Telegram {method} HTTP {exc.code}: {body[:300]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Telegram {method} network error: {exc}") from exc


def send_message(chat_id, text, keyboard=None, parse_mode="HTML"):
    payload = {
        "chat_id": int(chat_id),
        "text": text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if keyboard:
        payload["reply_markup"] = keyboard
    return tg_api("sendMessage", payload)


def edit_message(chat_id, message_id, text, keyboard=None):
    payload = {"chat_id": int(chat_id), "message_id": int(message_id), "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if keyboard:
        payload["reply_markup"] = keyboard
    return tg_api("editMessageText", payload)


def answer_callback(callback_id, text=None, alert=False):
    payload = {"callback_query_id": callback_id, "show_alert": bool(alert)}
    if text:
        payload["text"] = text[:180]
    try:
        return tg_api("answerCallbackQuery", payload)
    except Exception as exc:
        print(f"Telegram answerCallbackQuery warning: {exc}")
        return None


def btn(text, callback_data=None, url=None):
    item = {"text": text}
    if url:
        item["url"] = url
    else:
        item["callback_data"] = callback_data
    return item


def markup(rows):
    return {"inline_keyboard": rows}


def contact_url():
    return f"https://t.me/{SUPPORT_USERNAME}"


def start_keyboard(active=False):
    if active:
        return markup([
            [btn("📊 AI MARKET SCAN", "menu:scan")],
            [btn("🔑 MY ACCESS / LICENSE", "menu:status"), btn("💬 CONTACT ADMIN", url=contact_url())],
        ])
    return markup([
        [btn("🔗 CREATE QUOTEX ACCOUNT", url=PARTNER_URL)],
        [btn("✅ SUBMIT UID / REQUEST ACCESS", "access:uid")],
        [btn("🔑 I ALREADY HAVE VIP KEY", "access:key")],
        [btn("💬 CONTACT ADMIN", url=contact_url())],
    ])


def welcome_text(user):
    status = str(user.get("status") or "NEW").upper()
    if status == "ACTIVE":
        return (
            "👑 <b>RAJA AI PREMIUM</b>\n\n"
            "✅ Your Telegram access is <b>ACTIVE</b>.\n"
            "Use the menu below to scan the same RAJA AI market engine used by the web dashboard."
        )
    status_line = ""
    if status == "PENDING":
        status_line = "\n\n⏳ <b>Status:</b> Pending admin verification."
    elif status == "REJECTED":
        status_line = "\n\n❌ <b>Status:</b> Verification was not approved. Contact admin if you need help."
    return (
        "👑 <b>RAJA AI PREMIUM</b>\n"
        "Multi-Broker AI Service\n\n"
        "<b>Step 1:</b> Create your Quotex account using our official partner link.\n"
        f"🔗 <a href=\"{html.escape(PARTNER_URL, quote=True)}\">Quotex Partner Link</a>\n"
        "<b>Step 2:</b> Deposit minimum $50.\n"
        "<b>Step 3:</b> Submit your Telegram ID or Quotex UID for verification.\n\n"
        "🔐 After verification, admin can approve your access and issue/confirm your VIP license.\n"
        "📱 One approved Telegram account per access record.\n\n"
        f"💬 <b>Official Support:</b> @{html.escape(SUPPORT_USERNAME)}"
        + status_line
    )


def sync_identity(update_user, chat_id):
    tg_id = int(update_user.get("id"))
    user = get_user(tg_id, chat_id)
    user["chat_id"] = int(chat_id)
    user["username"] = str(update_user.get("username") or "")[:120]
    user["first_name"] = str(update_user.get("first_name") or "")[:120]
    user["last_name"] = str(update_user.get("last_name") or "")[:120]
    return save_user(user)


def notify_admin_pending(user):
    aid = admin_id()
    if not aid:
        return False
    uname = f"@{user.get('username')}" if user.get("username") else "(no username)"
    text = (
        "🛡️ <b>NEW TELEGRAM ACCESS REQUEST</b>\n\n"
        f"Name: <b>{html.escape((user.get('first_name') or '') + ' ' + (user.get('last_name') or ''))}</b>\n"
        f"Username: {html.escape(uname)}\n"
        f"Telegram ID: <code>{user.get('telegram_id')}</code>\n"
        f"Submitted UID/ID: <code>{html.escape(user.get('submitted_id') or 'Not supplied')}</code>\n"
        f"VIP Key: <code>{html.escape(user.get('license_key') or 'Not supplied / issue on approval')}</code>\n\n"
        "Verify the user's Quotex/referral details, then approve or reject."
    )
    kb = markup([[btn("✅ APPROVE", f"admin:approve:{user['telegram_id']}"), btn("❌ REJECT", f"admin:reject:{user['telegram_id']}")]])
    send_message(aid, text, kb)
    return True


def show_pending_to_admin(chat_id):
    items = pending_users(20)
    if not items:
        send_message(chat_id, "✅ No pending Telegram access requests.")
        return
    send_message(chat_id, f"🛡️ <b>Pending requests:</b> {len(items)}")
    for user in items:
        notify_text = (
            f"👤 <b>{html.escape(user.get('first_name') or 'User')}</b> "
            f"@{html.escape(user.get('username') or 'no_username')}\n"
            f"Telegram ID: <code>{user['telegram_id']}</code>\n"
            f"Submitted: <code>{html.escape(user.get('submitted_id') or '--')}</code>\n"
            f"Key: <code>{html.escape(user.get('license_key') or 'Issue on approval')}</code>"
        )
        kb = markup([[btn("✅ APPROVE", f"admin:approve:{user['telegram_id']}"), btn("❌ REJECT", f"admin:reject:{user['telegram_id']}")]])
        send_message(chat_id, notify_text, kb)


def show_approved_to_admin(chat_id):
    items = approved_users(100)
    if not items:
        send_message(chat_id, "🔐 No approved Telegram licenses found.")
        return

    send_message(chat_id, f"🔐 <b>APPROVED TELEGRAM LICENSES</b> · {len(items)}")
    # Keep each Telegram message comfortably below the platform text limit.
    blocks = []
    for i, user in enumerate(items, 1):
        username = f"@{user.get('username')}" if user.get('username') else "(no username)"
        name = ((user.get('first_name') or '') + ' ' + (user.get('last_name') or '')).strip() or 'User'
        blocks.append(
            f"<b>{i}. {html.escape(name)}</b> · {html.escape(username)}\n"
            f"Telegram ID: <code>{user.get('telegram_id')}</code>\n"
            f"UID/ID: <code>{html.escape(user.get('submitted_id') or '--')}</code>\n"
            f"VIP Key: <code>{html.escape(user.get('license_key') or '--')}</code>"
        )

    chunk = ""
    for block in blocks:
        candidate = block if not chunk else chunk + "\n\n" + block
        if len(candidate) > 3300:
            send_message(chat_id, chunk)
            chunk = block
        else:
            chunk = candidate
    if chunk:
        send_message(chat_id, chunk)


def market_keyboard():
    return markup([
        [btn(MARKET_LABELS["CryptoLive"], "mkt:CryptoLive"), btn(MARKET_LABELS["CryptoOTC"], "mkt:CryptoOTC")],
        [btn(MARKET_LABELS["ForexLive"], "mkt:ForexLive"), btn(MARKET_LABELS["ForexOTC"], "mkt:ForexOTC")],
        [btn("⬅️ MAIN MENU", "menu:home")],
    ])


def pair_keyboard(market, page=0):
    pairs = MARKET_PAIRS.get(market, [])
    per_page = 8
    pages = max(1, (len(pairs) + per_page - 1) // per_page)
    page = max(0, min(int(page), pages - 1))
    start = page * per_page
    rows = [[btn("✨ AUTO SCAN BEST PAIR", f"pairauto:{market}")]]
    chunk = pairs[start:start+per_page]
    for offset in range(0, len(chunk), 2):
        row = []
        for j in range(offset, min(offset+2, len(chunk))):
            index = start + j
            row.append(btn(chunk[j], f"pair:{market}:{index}"))
        rows.append(row)
    nav = []
    if page > 0:
        nav.append(btn("◀️", f"pairs:{market}:{page-1}"))
    nav.append(btn(f"{page+1}/{pages}", f"pairs:{market}:{page}"))
    if page + 1 < pages:
        nav.append(btn("▶️", f"pairs:{market}:{page+1}"))
    rows.append(nav)
    rows.append([btn("⬅️ MARKETS", "menu:scan")])
    return markup(rows)


def expiry_keyboard():
    return markup([
        [btn("1m", "exp:1m"), btn("2m", "exp:2m"), btn("5m", "exp:5m"), btn("15m", "exp:15m"), btn("30m", "exp:30m")],
        [btn("⬅️ CHANGE PAIR", "pair:back")],
    ])


def ready_to_scan_keyboard():
    return markup([
        [btn("🚀 START AI MARKET SCAN", "scan:run")],
        [btn("⬅️ CHANGE EXPIRY", "scan:expiry"), btn("🏠 MAIN MENU", "menu:home")],
    ])


def format_signal(result, expiry):
    signal = result.get("signal")
    pair = result.get("pair") or "--"
    score = float(result.get("score") or 0)
    direction = "🟢 ⬆️ UP (BUY)" if signal == "CALL" else "🔴 ⬇️ DOWN (SELL)"
    summary = result.get("timeframe_summary") or {}
    tf_lines = []
    for tf in ["1m", "2m", "5m", "10m", "15m", "30m"]:
        item = summary.get(tf) or {}
        sig = item.get("signal")
        sc = float(item.get("score") or 0)
        label = "⬆ UP" if sig == "CALL" else ("⬇ DOWN" if sig == "PUT" else "→ WAIT")
        tf_lines.append(f"{tf}: {label} {sc:.0f}%")
    warning = ""
    if result.get("otc_proxy_warning"):
        warning = "\n\n⚠️ OTC uses an underlying-market proxy/reference feed and can differ from Quotex OTC quotes."
    return (
        "🤖 <b>RAJA AI · CONFIRMED SIGNAL</b>\n\n"
        f"Asset: <b>{html.escape(str(pair))}</b>\n"
        f"Signal: <b>{direction}</b>\n"
        f"Selected Expiry: <b>{html.escape(expiry)}</b>\n"
        f"Technical Confluence: <b>{score:.1f}%</b>\n"
        f"Agreement: <b>{float(result.get('multi_tf_agreement') or 0):.1f}%</b>\n\n"
        "<b>Multi-Timeframe:</b>\n" + "\n".join(tf_lines) +
        "\n\n⏳ <b>Trade Entry:</b> Take the trade after the current candle closes." +
        "\n\n<i>⚡ Powered by RAJA AI • Multi-Timeframe Smart Analysis</i>" + warning
    )


def _run_scan(user, services):
    chat_id = user["chat_id"]
    expiry = user.get("expiry") or "1m"
    pair = user.get("pair") or ""
    market = user.get("market") or ""
    try:
        if pair == "__AUTO__":
            pairs = MARKET_PAIRS.get(market, [])
            outcome = services["scan_auto"](pairs, expiry)
            result = (outcome or {}).get("best") if isinstance(outcome, dict) else None
            diagnostics = (outcome or {}).get("diagnostics", {}) if isinstance(outcome, dict) else {}
        else:
            result = services["scan_pair"](pair, expiry)
            diagnostics = {}
        if result and result.get("signal") in {"CALL", "PUT"}:
            send_message(chat_id, format_signal(result, expiry), markup([
                [btn("🔁 SCAN AGAIN", "scan:run")],
                [btn("🔄 CHANGE PAIR", "menu:scan"), btn("🏠 MAIN MENU", "menu:home")],
            ]))
        else:
            extra = ""
            if diagnostics:
                extra = f"\nData available: {diagnostics.get('data_available','--')}/{diagnostics.get('total_pairs','--')}"
            send_message(chat_id,
                "⚠️ <b>NO VALID LIVE SIGNAL FOUND</b>\n\n"
                f"No asset passed the strict multi-timeframe + {html.escape(expiry)} confirmation on the current closed candles.{extra}\n\n"
                "Use Scan Again or wait for a fresh market setup.",
                markup([[btn("🔁 SCAN AGAIN", "scan:run")], [btn("🏠 MAIN MENU", "menu:home")]])
            )
    except Exception as exc:
        print(f"Telegram scan error: {exc}")
        send_message(chat_id, "⚠️ Scan could not complete right now. Please try again shortly.", markup([[btn("🔁 TRY AGAIN", "scan:run")], [btn("🏠 MAIN MENU", "menu:home")]]))


def handle_message(message, services):
    chat = message.get("chat") or {}
    sender = message.get("from") or {}
    if chat.get("type") != "private" or not sender.get("id"):
        return
    chat_id = int(chat.get("id"))
    user = sync_identity(sender, chat_id)
    text = str(message.get("text") or "").strip()

    if text.split(maxsplit=1)[0].lower() in {"/licenses", "/keys", "/approved"}:
        current_admin = admin_id()
        if current_admin and int(sender["id"]) == current_admin:
            show_approved_to_admin(chat_id)
        else:
            send_message(chat_id, "⛔ Admin only command.")
        return

    if text.startswith("/admin"):
        parts = text.split(maxsplit=1)
        current_admin = admin_id()
        if current_admin and int(sender["id"]) == current_admin:
            show_pending_to_admin(chat_id)
            return
        code = parts[1].strip() if len(parts) > 1 else ""
        if not current_admin and ADMIN_SETUP_CODE and code and code == ADMIN_SETUP_CODE:
            set_meta("admin_telegram_id", str(sender["id"]))
            send_message(chat_id, "✅ <b>ADMIN TELEGRAM ACCOUNT BOUND</b>\n\nThis Telegram account can now approve/reject RAJA AI access requests.")
            show_pending_to_admin(chat_id)
            return
        if not current_admin:
            send_message(chat_id, "🔐 Admin is not bound yet. Set TELEGRAM_ADMIN_SETUP_CODE in Render, then send <code>/admin YOUR_CODE</code> from the admin Telegram account.")
        else:
            send_message(chat_id, "⛔ This Telegram account is not the configured admin.")
        return

    if text.startswith("/start") or text.startswith("/menu"):
        send_message(chat_id, welcome_text(user), start_keyboard(user.get("status") == "ACTIVE"))
        return

    if user.get("status") == "ACTIVE":
        send_message(chat_id, "Use the RAJA AI menu below.", start_keyboard(True))
        return

    stage = user.get("stage")
    if stage == "AWAITING_UID" and text:
        user["submitted_id"] = text[:160]
        user["stage"] = "AWAITING_LICENSE"
        save_user(user)
        send_message(chat_id,
            "✅ UID / ID received.\n\n"
            "🔑 <b>Now send your VIP License Key</b> if admin has already issued one.\n"
            "If you do not have a key yet, tap <b>REQUEST ADMIN APPROVAL</b>; a VIP key can be issued when admin approves you.",
            markup([[btn("🛡️ REQUEST ADMIN APPROVAL", "access:submit")], [btn("💬 CONTACT ADMIN", url=contact_url())]])
        )
        return

    if stage == "AWAITING_LICENSE" and text:
        user["license_key"] = text[:160]
        user["status"] = "PENDING"
        user["stage"] = "PENDING"
        save_user(user)
        notified = notify_admin_pending(user)
        send_message(chat_id,
            "⏳ <b>ACCESS REQUEST SUBMITTED</b>\n\n"
            "Your Telegram ID / Quotex UID and VIP key have been sent for admin verification.\n"
            f"Support: @{html.escape(SUPPORT_USERNAME)}" + ("" if notified else "\n\n⚠️ Admin Telegram binding is not configured yet; contact support."),
            start_keyboard(False)
        )
        return

    send_message(chat_id, welcome_text(user), start_keyboard(False))


def handle_callback(query, services):
    callback_id = query.get("id")
    sender = query.get("from") or {}
    message = query.get("message") or {}
    chat = message.get("chat") or {}
    if not sender.get("id") or not chat.get("id"):
        answer_callback(callback_id)
        return
    chat_id = int(chat["id"])
    message_id = int(message.get("message_id") or 0)
    user = sync_identity(sender, chat_id)
    data = str(query.get("data") or "")

    # Admin actions are always checked against the numeric admin Telegram ID.
    if data.startswith("admin:"):
        if admin_id() != int(sender["id"]):
            answer_callback(callback_id, "Admin only", True)
            return
        parts = data.split(":")
        if len(parts) != 3 or not parts[2].isdigit():
            answer_callback(callback_id, "Invalid request", True)
            return
        action, target_id = parts[1], int(parts[2])
        target = get_user(target_id)
        if target.get("status") != "PENDING":
            answer_callback(callback_id, f"Already {target.get('status','processed')}", True)
            return
        if action == "approve":
            user_ref = (target.get("submitted_id") or ("@" + target.get("username") if target.get("username") else str(target_id))).strip()
            submitted_key = (target.get("license_key") or "").strip()
            key = None
            if submitted_key:
                try:
                    if services["validate_license"](submitted_key, user_ref):
                        key = submitted_key
                except Exception:
                    key = None
            if not key:
                key = services["issue_license"](user_ref)
            target["license_key"] = key
            target["status"] = "ACTIVE"
            target["stage"] = "ACTIVE"
            target["approved_at"] = int(time.time())
            save_user(target)
            send_message(target["chat_id"],
                "✅ <b>RAJA AI TELEGRAM ACCESS APPROVED</b>\n\n"
                "Your bot access is now active.\n"
                f"VIP License Key: <code>{html.escape(key)}</code>\n\n"
                "Keep your key private. Use AI Market Scan below to continue.",
                start_keyboard(True)
            )
            answer_callback(callback_id, "Approved")
            send_message(chat_id, f"✅ Approved Telegram user <code>{target_id}</code>. License: <code>{html.escape(key)}</code>")
            return
        if action == "reject":
            target["status"] = "REJECTED"
            target["stage"] = "REJECTED"
            save_user(target)
            send_message(target["chat_id"],
                "❌ <b>ACCESS NOT APPROVED</b>\n\nPlease contact the admin if your Quotex UID/referral details need to be checked again.",
                markup([[btn("💬 CONTACT ADMIN", url=contact_url())]])
            )
            answer_callback(callback_id, "Rejected")
            send_message(chat_id, f"❌ Rejected Telegram user <code>{target_id}</code>.")
            return

    if data == "access:uid":
        user["stage"] = "AWAITING_UID"
        user["status"] = "NEW"
        save_user(user)
        answer_callback(callback_id)
        send_message(chat_id,
            "🆔 <b>SUBMIT TELEGRAM ID OR QUOTEX UID</b>\n\n"
            "Send the ID/UID in your next message. Your numeric Telegram ID is captured automatically as well.\n\n"
            f"Need help? @{html.escape(SUPPORT_USERNAME)}"
        )
        return

    if data == "access:key":
        user["stage"] = "AWAITING_LICENSE"
        save_user(user)
        answer_callback(callback_id)
        send_message(chat_id,
            "🔑 <b>ENTER VIP LICENSE KEY</b>\n\n"
            "Send your RAJA VIP license key in the next message. If your UID has not been submitted yet, use Request Access first."
        )
        return

    if data == "access:submit":
        if not user.get("submitted_id"):
            answer_callback(callback_id, "Submit UID first", True)
            return
        user["status"] = "PENDING"
        user["stage"] = "PENDING"
        save_user(user)
        notified = notify_admin_pending(user)
        answer_callback(callback_id, "Request sent")
        send_message(chat_id,
            "⏳ <b>ACCESS REQUEST SUBMITTED</b>\n\nAdmin will verify your UID and approve/reject your request." +
            ("" if notified else f"\n\n⚠️ Contact @{html.escape(SUPPORT_USERNAME)} because admin Telegram binding is not configured yet."),
            start_keyboard(False)
        )
        return

    if data == "menu:home":
        answer_callback(callback_id)
        send_message(chat_id, welcome_text(user), start_keyboard(user.get("status") == "ACTIVE"))
        return

    if data == "menu:status":
        answer_callback(callback_id)
        key = html.escape(user.get("license_key") or "Not issued")
        submitted = html.escape(user.get("submitted_id") or "--")
        send_message(chat_id,
            "🔐 <b>MY RAJA AI ACCESS</b>\n\n"
            f"Status: <b>{html.escape(str(user.get('status') or 'NEW'))}</b>\n"
            f"Telegram ID: <code>{user['telegram_id']}</code>\n"
            f"Submitted UID/ID: <code>{submitted}</code>\n"
            f"VIP Key: <code>{key}</code>",
            start_keyboard(user.get("status") == "ACTIVE")
        )
        return

    if data == "menu:scan":
        if user.get("status") != "ACTIVE":
            answer_callback(callback_id, "Access not active", True)
            return
        answer_callback(callback_id)
        send_message(chat_id, "📊 <b>SELECT MARKET TYPE</b>", market_keyboard())
        return

    if data.startswith("mkt:"):
        market = data.split(":",1)[1]
        if market not in MARKET_PAIRS:
            answer_callback(callback_id, "Unknown market", True)
            return
        user["market"] = market
        user["pair"] = ""
        save_user(user)
        answer_callback(callback_id)
        send_message(chat_id, f"{MARKET_LABELS[market]}\n\n<b>Select Pair / Asset</b>", pair_keyboard(market, 0))
        return

    if data.startswith("pairs:"):
        parts = data.split(":")
        if len(parts) == 3:
            market, page = parts[1], int(parts[2]) if parts[2].isdigit() else 0
            answer_callback(callback_id)
            try:
                edit_message(chat_id, message_id, f"{MARKET_LABELS.get(market, market)}\n\n<b>Select Pair / Asset</b>", pair_keyboard(market, page))
            except Exception:
                send_message(chat_id, f"{MARKET_LABELS.get(market, market)}\n\n<b>Select Pair / Asset</b>", pair_keyboard(market, page))
        return

    if data == "pair:back":
        market = user.get("market") or "CryptoLive"
        answer_callback(callback_id)
        send_message(chat_id, f"{MARKET_LABELS.get(market, market)}\n\n<b>Select Pair / Asset</b>", pair_keyboard(market, 0))
        return

    if data.startswith("pairauto:"):
        market = data.split(":",1)[1]
        user["market"] = market
        user["pair"] = "__AUTO__"
        save_user(user)
        answer_callback(callback_id)
        send_message(chat_id, f"✨ Auto Scan Best Pair · {MARKET_LABELS.get(market, market)}\n\n⏱️ <b>Select Trade Expiry</b>", expiry_keyboard())
        return

    if data.startswith("pair:") and data != "pair:back":
        parts = data.split(":")
        if len(parts) == 3 and parts[2].isdigit():
            market, index = parts[1], int(parts[2])
            pairs = MARKET_PAIRS.get(market, [])
            if 0 <= index < len(pairs):
                user["market"] = market
                user["pair"] = pairs[index]
                save_user(user)
                answer_callback(callback_id)
                send_message(chat_id, f"Asset: <b>{html.escape(pairs[index])}</b>\n\n⏱️ <b>Select Trade Expiry</b>", expiry_keyboard())
                return
        answer_callback(callback_id, "Invalid pair", True)
        return

    if data.startswith("exp:"):
        expiry = data.split(":",1)[1]
        if expiry not in VALID_EXPIRIES:
            answer_callback(callback_id, "Unsupported expiry", True)
            return
        user["expiry"] = expiry
        save_user(user)
        pair_label = "Auto Scan Best Pair" if user.get("pair") == "__AUTO__" else user.get("pair")
        answer_callback(callback_id)
        send_message(chat_id,
            "✅ <b>SCAN CONFIGURATION READY</b>\n\n"
            f"Market: {html.escape(MARKET_LABELS.get(user.get('market'), user.get('market') or '--'))}\n"
            f"Pair: <b>{html.escape(pair_label or '--')}</b>\n"
            f"Expiry: <b>{html.escape(expiry)}</b>",
            ready_to_scan_keyboard()
        )
        return

    if data == "scan:expiry":
        answer_callback(callback_id)
        send_message(chat_id, "⏱️ <b>Select Trade Expiry</b>", expiry_keyboard())
        return

    if data == "scan:run":
        if user.get("status") != "ACTIVE":
            answer_callback(callback_id, "Access not active", True)
            return
        if not user.get("market") or not user.get("pair") or not user.get("expiry"):
            answer_callback(callback_id, "Choose market, pair and expiry first", True)
            send_message(chat_id, "📊 <b>Select Market</b>", market_keyboard())
            return
        answer_callback(callback_id, "Scan started")
        pair_label = "best pair" if user.get("pair") == "__AUTO__" else user.get("pair")
        send_message(chat_id,
            "🔎 <b>AI MARKET SCAN STARTED</b>\n\n"
            f"Scanning {html.escape(str(pair_label))} with {html.escape(user.get('expiry') or '1m')} confirmation.\n"
            "I will send the result here when the scan finishes."
        )
        threading.Thread(target=_run_scan, args=(dict(user), services), daemon=True).start()
        return

    answer_callback(callback_id)


def handle_update(update, services):
    try:
        if update.get("callback_query"):
            handle_callback(update["callback_query"], services)
        elif update.get("message"):
            handle_message(update["message"], services)
    except Exception as exc:
        print(f"Telegram update handler error: {exc}")


def configure_webhook():
    if not BOT_TOKEN or not PUBLIC_BASE_URL:
        return False
    payload = {
        "url": f"{PUBLIC_BASE_URL}/telegram/webhook",
        "allowed_updates": ["message", "callback_query"],
        "drop_pending_updates": False,
    }
    if WEBHOOK_SECRET:
        payload["secret_token"] = WEBHOOK_SECRET
    result = tg_api("setWebhook", payload, timeout=20)
    print(f"Telegram webhook configured for @{BOT_USERNAME}: {result}")
    return bool(result)


def register_telegram_routes(app, services):
    init_telegram_store()

    @app.route("/telegram/webhook", methods=["POST"])
    def telegram_webhook():
        if not BOT_TOKEN:
            return jsonify({"ok": False, "message": "Telegram bot is not configured."}), 503
        if WEBHOOK_SECRET:
            provided = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if provided != WEBHOOK_SECRET:
                return jsonify({"ok": False, "message": "Invalid webhook secret."}), 403
        update = request.get_json(silent=True) or {}
        # Return fast. Any long market scan is already moved to a background thread.
        handle_update(update, services)
        return jsonify({"ok": True})

    @app.route("/telegram/health", methods=["GET"])
    def telegram_health():
        return jsonify({
            "status": "ok",
            "telegram_enabled": bool(BOT_TOKEN),
            "bot_username": BOT_USERNAME,
            "support_username": SUPPORT_USERNAME,
            "admin_bound": bool(admin_id()),
            "public_base_url": PUBLIC_BASE_URL,
        })

    if BOT_TOKEN:
        def delayed_setup():
            time.sleep(5)
            try:
                configure_webhook()
            except Exception as exc:
                print(f"Telegram webhook setup warning: {exc}")
        threading.Thread(target=delayed_setup, daemon=True).start()
    else:
        print("Telegram integration loaded but TELEGRAM_BOT_TOKEN is not set.")
