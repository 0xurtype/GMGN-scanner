"""
GMGN Signal Scanner — Live Backend
Polls GMGN API for real token data, serves via REST + SSE.
"""

import asyncio
import json
import os
from pathlib import Path

# Load .env from Hermes profile
_env_path = Path.home() / ".hermes" / "profiles" / "default" / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())
import subprocess
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

app = FastAPI(title="GMGN Scanner API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Config ──────────────────────────────────────────────────────────────
SCAN_INTERVAL = 30  # seconds between trench scans
ENRICH_INTERVAL = 60  # seconds between enrichment cycles
ENRICH_BATCH = 20  # tokens to enrich per cycle
ENRICH_DELAY = 0.5  # seconds between enrichments (rate limit)
SEEN_CACHE_SIZE = 2000

DATA_DIR = Path(__file__).parent / "scanner_data"
DATA_DIR.mkdir(exist_ok=True)
SEEN_FILE = DATA_DIR / "seen.json"

# ── State ───────────────────────────────────────────────────────────────
tokens_db: dict[str, dict] = {}
seen_set: set[str] = set()
sse_subscribers: list = []
last_scan_time: float = 0
scan_count: int = 0
kol_cache: dict = {}  # cached KOL data
wallets_db: dict[str, dict] = {}  # smart wallets keyed by address
smart_tokens_db: dict[str, dict] = {}  # smart tokens keyed by base_address
wallet_trades: list = []  # raw trades from last poll
SMART_WALLET_INTERVAL = 60  # seconds between smart wallet polls
SMART_TOKEN_ENRICH_INTERVAL = 30  # seconds between token enrich cycles
SMART_TOKEN_ENRICH_BATCH = 5  # max tokens to enrich per cycle
SMART_TOKEN_ENRICH_DELAY = 2  # seconds between enrichments
signals_db: list[dict] = []  # detected signals, newest first
detected_signal_keys: set[str] = set()  # dedup key: f"{signal_type}:{token_addr}"
TG_CHAT_ID = os.environ.get('TG_CHAT_ID', '6156910362')  # default to user's chat


def load_seen():
    global seen_set
    if SEEN_FILE.exists():
        try:
            seen_set = set(json.loads(SEEN_FILE.read_text()))
        except Exception:
            seen_set = set()


def save_seen():
    data = list(seen_set)[-SEEN_CACHE_SIZE:]
    SEEN_FILE.write_text(json.dumps(data))


def gmgn_cli(*args: str) -> dict:
    """Run gmgn-cli and return parsed JSON."""
    try:
        result = subprocess.run(
            ["/usr/bin/gmgn-cli", *args],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            print(f"[gmgn-cli] error: {result.stderr[:200]}")
            return {}
        return json.loads(result.stdout) if result.stdout.strip() else {}
    except Exception as e:
        print(f"[gmgn-cli] exception: {e}")
        return {}


def to_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def to_int(v, default=0):
    try:
        return int(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def score_token(t: dict) -> int:
    """0-100 quality score."""
    s = 50
    s += min(to_int(t.get("smart_degen_count", 0)) * 8, 40)
    s += min(to_int(t.get("renowned_count", 0)) * 6, 30)
    s += min(to_int(t.get("sniper_count", 0)) * 1, 8)
    s += min(to_int(t.get("holder_count", 0)) / 200, 10)

    bundler = to_float(t.get("bundler_pct", 0)) / 100
    s -= bundler * 50

    if t.get("honeypot"):
        s -= 40

    if t.get("renounced_mint"):
        s += 5
    if t.get("renounced_freeze"):
        s += 3

    top10 = to_float(t.get("top10_rate", 0))
    if top10 > 30:
        s -= 10

    return max(0, min(100, round(s)))


def format_token(raw: dict, source_type: str) -> dict:
    """Format raw GMGN data into frontend-friendly structure."""
    addr = raw.get("address", "")
    symbol = raw.get("symbol", "???")
    name = raw.get("name", "")
    created_ts = raw.get("created_timestamp", 0)

    price = to_float(raw.get("price", 0))
    mcap = to_float(raw.get("market_cap", 0))
    liq = to_float(raw.get("liquidity", 0))
    vol = to_float(raw.get("volume_24h", 0))
    holders = to_int(raw.get("holder_count", 0))

    now = time.time()
    age_seconds = now - created_ts if created_ts else 0
    if age_seconds < 3600:
        age_str = f"{max(1, age_seconds // 60)}m"
    elif age_seconds < 86400:
        age_str = f"{age_seconds // 3600:.1f}h"
    else:
        age_str = f"{age_seconds // 86400:.0f}d"

    dev_count = to_int(raw.get("creator_created_count", 0))
    rug_ratio = to_float(raw.get("rug_ratio", 0))
    if rug_ratio > 0.3:
        dev_status = "risky"
    elif dev_count > 5 or rug_ratio > 0.1:
        dev_status = "warn"
    else:
        dev_status = "clean"

    bundler_pct = round(to_float(raw.get("bundler_trader_amount_rate", 0)) * 100, 1)

    token = {
        "address": addr,
        "symbol": symbol,
        "name": name,
        "dex": raw.get("launchpad", "unknown"),
        "created_timestamp": created_ts,
        "age": age_str,
        "age_seconds": age_seconds,
        "price": price,
        "market_cap": mcap,
        "liquidity": liq,
        "volume_24h": vol,
        "holder_count": holders,
        "smart_degen_count": to_int(raw.get("smart_degen_count", 0)),
        "renowned_count": to_int(raw.get("renowned_count", 0)),
        "sniper_count": to_int(raw.get("sniper_count", 0)),
        "bundler_pct": bundler_pct,
        "bot_rate": round(to_float(raw.get("bot_degen_rate", 0)) * 100, 1),
        "entrapment": round(to_float(raw.get("entrapment_ratio", 0)) * 100, 1),
        "top10_rate": round(to_float(raw.get("top_10_holder_rate", 0)) * 100, 1),
        "dev_status": dev_status,
        "dev_deploys": dev_count,
        "dev_wallet": round(to_float(raw.get("creator_balance_rate", 0)) * 100, 1),
        "renounced_mint": raw.get("renounced_mint", False),
        "renounced_freeze": raw.get("renounced_freeze_account", False),
        "honeypot": raw.get("is_honeypot", "unknown") == "yes",
        "buy_tax": to_float(raw.get("buy_tax", 0)),
        "sell_tax": to_float(raw.get("sell_tax", 0)),
        "burn_status": raw.get("burn_status", ""),
        "fund_from": raw.get("fund_from", ""),
        "creator": raw.get("creator", ""),
        "twitter": raw.get("twitter_handle", ""),
        "twitter_followers": to_int(raw.get("x_user_follower", 0)),
        "wash_trading": raw.get("is_wash_trading", False),
        "progress": round(to_float(raw.get("progress", 0)) * 100, 1),
        "source": source_type,
        "score": 0,
        "fetched_at": now,
        "enriched": False,
    }
    token["score"] = score_token(token)
    return token


# ── Token Enrichment ────────────────────────────────────────────────────

async def enrich_token(addr: str):
    """Enrich a single token with price + security data."""
    if addr not in tokens_db:
        return

    # Fetch token info
    info = gmgn_cli("token", "info", "--chain", "sol", "--address", addr)
    if info:
        price_block = info.get("price", {})
        if isinstance(price_block, dict):
            tokens_db[addr]["price"] = to_float(price_block.get("price", 0))
            tokens_db[addr]["volume_24h"] = to_float(price_block.get("volume_24h", 0))
            tokens_db[addr]["buys_1h"] = to_int(price_block.get("buys_1h", 0))
            tokens_db[addr]["sells_1h"] = to_int(price_block.get("sell_volume_1h", 0))

        stat = info.get("stat", {})
        if stat:
            tokens_db[addr]["top10_rate"] = round(to_float(stat.get("top_10_holder_rate", 0)) * 100, 1)
            tokens_db[addr]["bundler_pct"] = round(to_float(stat.get("top_bundler_trader_percentage", 0)) * 100, 1)

        tags = info.get("wallet_tags_stat", {})
        if tags:
            tokens_db[addr]["smart_degen_count"] = to_int(tags.get("smart_wallets", 0))
            tokens_db[addr]["renowned_count"] = to_int(tags.get("renowned_wallets", 0))
            tokens_db[addr]["sniper_count"] = to_int(tags.get("sniper_wallets", 0))

        mcap = to_float(info.get("market_cap"))
        if mcap == 0:
            supply = to_float(info.get("total_supply", 0))
            price = tokens_db[addr]["price"]
            if supply > 0 and price > 0:
                mcap = price * supply
        tokens_db[addr]["market_cap"] = mcap
        tokens_db[addr]["holder_count"] = to_int(info.get("holder_count", tokens_db[addr]["holder_count"]))
        tokens_db[addr]["liquidity"] = to_float(info.get("liquidity", tokens_db[addr]["liquidity"]))

        link = info.get("link", {})
        if link:
            tokens_db[addr]["twitter"] = link.get("twitter_username", tokens_db[addr]["twitter"])

    # Fetch security data
    sec = gmgn_cli("token", "security", "--chain", "sol", "--address", addr)
    if sec:
        tokens_db[addr]["renounced_mint"] = sec.get("renounced_mint", False)
        tokens_db[addr]["renounced_freeze"] = sec.get("renounced_freeze_account", False)
        tokens_db[addr]["honeypot"] = sec.get("honeypot", "unknown") == "yes"
        tokens_db[addr]["buy_tax"] = to_float(sec.get("buy_tax", 0))
        tokens_db[addr]["sell_tax"] = to_float(sec.get("sell_tax", 0))
        tokens_db[addr]["burn_status"] = sec.get("burn_status", "")

    tokens_db[addr]["enriched"] = True
    tokens_db[addr]["score"] = score_token(tokens_db[addr])


async def enrich_loop():
    """Background loop that enriches top tokens with price/security data."""
    while True:
        await asyncio.sleep(ENRICH_INTERVAL)
        try:
            # Enrich top-scored tokens that haven't been enriched yet
            candidates = sorted(
                [t for t in tokens_db.values() if not t.get("enriched")],
                key=lambda t: t["score"],
                reverse=True,
            )[:ENRICH_BATCH]

            if candidates:
                print(f"[enrich] Enriching {len(candidates)} tokens...")
                for t in candidates:
                    await enrich_token(t["address"])
                    await asyncio.sleep(ENRICH_DELAY)
                print(f"[enrich] Done")
        except Exception as e:
            print(f"[enrich] error: {e}")


# ── Background Scanner ──────────────────────────────────────────────────

async def scan_smart_wallets():
    """Poll GMGN smart money trades and aggregate per wallet + per token."""
    global wallet_trades
    raw = gmgn_cli("track", "smartmoney", "--chain", "sol")
    if not raw:
        return

    trades = raw.get("list", []) if isinstance(raw, dict) else []
    if not isinstance(trades, list):
        return

    wallet_trades = trades  # store raw for frontend

    # ── Aggregate per wallet (existing) ──
    for t in trades:
        addr = t.get("maker", "")
        if not addr:
            continue

        info = t.get("maker_info", {})
        sym = t.get("base_token", {}).get("symbol", "")
        side = t.get("side", "")
        vol = to_float(t.get("amount_usd", 0))
        ts = t.get("timestamp", 0)
        is_close = t.get("is_open_or_close", 0)
        base_addr = t.get("base_address", "")

        if addr not in wallets_db:
            wallets_db[addr] = {
                "address": addr,
                "tags": info.get("tags", []),
                "twitter": info.get("twitter_username", ""),
                "first_seen": ts,
                "last_seen": ts,
                "total_trades": 0,
                "buys": 0,
                "sells": 0,
                "opens": 0,
                "closes": 0,
                "total_vol": 0.0,
                "tokens": {},  # sym -> {count, vol, last_ts}
            }

        w = wallets_db[addr]
        w["last_seen"] = max(w["last_seen"], ts)
        w["total_trades"] += 1
        if side == "buy":
            w["buys"] += 1
        else:
            w["sells"] += 1
        if is_close:
            w["closes"] += 1
        else:
            w["opens"] += 1
        w["total_vol"] += vol

        # Track per-token activity
        if sym and sym not in ("SOL", "WSOL"):
            if sym not in w["tokens"]:
                w["tokens"][sym] = {"count": 0, "vol": 0.0, "last_ts": 0, "address": base_addr}
            w["tokens"][sym]["count"] += 1
            w["tokens"][sym]["vol"] += vol
            w["tokens"][sym]["last_ts"] = max(w["tokens"][sym]["last_ts"], ts)

    # ── Token-centric aggregation ──
    for t in trades:
        addr = t.get("maker", "")
        base_addr = t.get("base_address", "")
        if not addr or not base_addr:
            continue

        info = t.get("maker_info", {})
        base_token = t.get("base_token", {})
        sym = base_token.get("symbol", "")
        side = t.get("side", "")
        vol = to_float(t.get("amount_usd", 0))
        balance = to_float(t.get("balance", 0))
        ts = t.get("timestamp", 0)
        is_close = t.get("is_open_or_close", 0)

        # Init token entry
        if base_addr not in smart_tokens_db:
            smart_tokens_db[base_addr] = {
                "address": base_addr,
                "symbol": sym,
                "name": base_token.get("name", ""),
                "logo": base_token.get("logo", ""),
                "mcap": 0.0,
                "volume_24h": 0.0,
                "holder_count": 0,
                "price": 0.0,
                "price_change_24h": 0.0,
                "liquidity": 0.0,
                "smart_inflow": 0.0,
                "wallets": {},
                "enriched": False,
            }

        tk = smart_tokens_db[base_addr]
        # Update token metadata from trade
        if sym:
            tk["symbol"] = sym
        if base_token.get("name"):
            tk["name"] = base_token["name"]
        if base_token.get("logo"):
            tk["logo"] = base_token["logo"]

        # Calculate inflow contribution
        if side == "buy":
            tk["smart_inflow"] += vol
        else:
            tk["smart_inflow"] -= vol

        # Init wallet entry on this token
        if addr not in tk["wallets"]:
            tk["wallets"][addr] = {
                "address": addr,
                "twitter": info.get("twitter_username", ""),
                "tags": info.get("tags", []),
                "balance": balance,
                "buys": 0,
                "sells": 0,
                "inflow": 0.0,
                "last_action_ts": ts,
                "action_type": "first_buy",
            }
        wk = tk["wallets"][addr]

        # Update wallet-on-token stats
        wk["balance"] = balance
        wk["last_action_ts"] = max(wk["last_action_ts"], ts)
        if info.get("twitter_username"):
            wk["twitter"] = info["twitter_username"]
        if info.get("tags"):
            wk["tags"] = info["tags"]

        if side == "buy":
            wk["buys"] += 1
            wk["inflow"] += vol
            if wk["action_type"] == "first_buy":
                pass  # keep first_buy
            else:
                wk["action_type"] = "buy_more"
        else:
            wk["sells"] += 1
            wk["inflow"] -= vol
            if balance <= 0:
                wk["action_type"] = "sell_all"
            else:
                wk["action_type"] = "sell_partial"

    # Round smart_inflow on tokens
    for tk in smart_tokens_db.values():
        tk["smart_inflow"] = round(tk["smart_inflow"], 2)
        for wk in tk["wallets"].values():
            wk["inflow"] = round(wk["inflow"], 2)

    print(f"[smart-wallets] {len(trades)} trades, {len(wallets_db)} unique wallets tracked")
    print(f"[smart-tokens] {len(smart_tokens_db)} unique tokens tracked")

    # Detect signals after aggregation
    new_signals = detect_signals()
    if new_signals:
        try:
            await send_tg_alert(new_signals)
        except Exception as e:
            print(f'[tg-alert] ERROR in scan_smart_wallets: {e}')
            import traceback; traceback.print_exc()


async def smart_wallet_loop():
    """Background loop for smart wallet tracking."""
    while True:
        try:
            await scan_smart_wallets()
        except Exception as e:
            print(f"[smart-wallets] error: {e}")
        if len(detected_signal_keys) > 500:
            detected_signal_keys.clear()
        await asyncio.sleep(SMART_WALLET_INTERVAL)


async def smart_token_enrich(token_addr: str):
    """Enrich a smart token with mcap, volume, holders, price changes."""
    if token_addr not in smart_tokens_db:
        return

    info = gmgn_cli("token", "info", "--chain", "sol", "--address", token_addr)
    if not info:
        return

    tk = smart_tokens_db[token_addr]

    price_block = info.get("price", {})
    if isinstance(price_block, dict):
        tk["price"] = to_float(price_block.get("price", 0))
        tk["volume_24h"] = to_float(price_block.get("volume_24h", 0))
        price_now = tk["price"]
        price_24h = to_float(price_block.get("price_24h", 0))
        price_1h = to_float(price_block.get("price_1h", 0))
        tk["price_change_24h"] = round((price_now - price_24h) / price_24h * 100, 2) if price_24h > 0 else 0.0
        tk["price_change_1h"] = round((price_now - price_1h) / price_1h * 100, 2) if price_1h > 0 else 0.0

    tk["holder_count"] = to_int(info.get("holder_count", tk["holder_count"]))
    tk["liquidity"] = to_float(info.get("liquidity", tk["liquidity"]))

    mcap = to_float(info.get("market_cap"))
    if mcap == 0:
        supply = to_float(info.get("total_supply", 0))
        price = tk["price"]
        if supply > 0 and price > 0:
            mcap = price * supply
    tk["mcap"] = mcap

    tk["enriched"] = True


async def smart_token_enrich_loop():
    """Background loop: enrich top smart tokens by inflow volume."""
    while True:
        await asyncio.sleep(SMART_TOKEN_ENRICH_INTERVAL)
        try:
            # Sort by abs(smart_inflow), take top N unenriched
            candidates = sorted(
                [t for t in smart_tokens_db.values() if not t.get("enriched")],
                key=lambda t: abs(t.get("smart_inflow", 0)),
                reverse=True,
            )[:SMART_TOKEN_ENRICH_BATCH]

            if candidates:
                print(f"[smart-token-enrich] Enriching {len(candidates)} tokens...")
                for t in candidates:
                    await smart_token_enrich(t["address"])
                    await asyncio.sleep(SMART_TOKEN_ENRICH_DELAY)
                print(f"[smart-token-enrich] Done")

            # Re-enrich already enriched tokens that are active (top 5 by inflow)
            active = sorted(
                [t for t in smart_tokens_db.values() if t.get("enriched")],
                key=lambda t: abs(t.get("smart_inflow", 0)),
                reverse=True,
            )[:SMART_TOKEN_ENRICH_BATCH]

            for t in active:
                t["enriched"] = False  # mark for re-enrich
                await smart_token_enrich(t["address"])
                await asyncio.sleep(SMART_TOKEN_ENRICH_DELAY)
                print(f"[smart-token-enrich] Re-enriched {t['symbol']}")

        except Exception as e:
            print(f"[smart-token-enrich] error: {e}")


ACCUMULATION_WINDOW = 7200  # 2 hours in seconds
ACCUMULATION_MIN_WALLETS = 2  # minimum smart wallets to trigger
LARGE_BUY_THRESHOLD = 500  # USD
CONCENTRATION_THRESHOLD = 0.10  # 10% of volume
MAX_TOKEN_AGE = 172800  # 2 days in seconds

def detect_signals():
    """Scan recent trades for pre-hype patterns."""
    global signals_db, detected_signal_keys
    now = int(time.time())
    new_signals = []

    for token_addr, token_data in smart_tokens_db.items():
        symbol = token_data.get('symbol', '?')
        name = token_data.get('name', '')
        wallets = list(token_data.get('wallets', {}).values())
        mcap = token_data.get('mcap', 0)
        volume = token_data.get('volume_24h', 0)

        # Get token age from tokens_db (trenches data)
        token_info = tokens_db.get(token_addr, {})
        created_ts = token_info.get('created_timestamp', 0)
        age_sec = now - created_ts if created_ts else 0  # default 0 = assume new
        if created_ts and age_sec > MAX_TOKEN_AGE:
            continue  # only skip if we KNOW it's old

        # --- SIGNAL 1: Accumulation ---
        buy_wallets = [w for w in wallets if w.get('action_type') in ('first_buy', 'buy_more') and (now - w.get('last_action_ts', 0)) < ACCUMULATION_WINDOW]
        if len(buy_wallets) >= ACCUMULATION_MIN_WALLETS:
            total_inflow = sum(w.get('inflow', 0) for w in buy_wallets)
            wallet_details = []
            for w in buy_wallets:
                label = w.get('twitter', '') or w.get('address', '')[:8]
                wallet_details.append(label)
            key = f'accumulation:{token_addr}'
            if key not in detected_signal_keys:
                age_hours = round(age_sec / 3600, 1)
                signal = {
                    'type': 'accumulation',
                    'token': symbol,
                    'token_name': name,
                    'token_address': token_addr,
                    'wallet_count': len(buy_wallets),
                    'total_usd': round(total_inflow, 2),
                    'mcap': mcap,
                    'volume_24h': volume,
                    'wallets': wallet_details,
                    'details': f'{len(buy_wallets)} smart wallets bought in {round((now - min(w.get("last_action_ts", now) for w in buy_wallets)) / 3600, 1)}h',
                    'age_hours': age_hours,
                    'timestamp': now,
                    'confidence': min(95, 40 + len(buy_wallets) * 15 + (10 if mcap < 500000 else 0)),
                }
                new_signals.append(signal)
                detected_signal_keys.add(key)

        # --- SIGNAL 2: Large Buy ---
        for w in wallets:
            if w.get('inflow', 0) >= LARGE_BUY_THRESHOLD and w.get('action_type') in ('first_buy', 'buy_more'):
                key = f'large_buy:{token_addr}:{w["address"]}'
                if key not in detected_signal_keys:
                    label = w.get('twitter', '') or w.get('address', '')[:8]
                    signal = {
                        'type': 'large_buy',
                        'token': symbol,
                        'token_name': name,
                        'token_address': token_addr,
                        'wallet': label,
                        'wallet_address': w.get('address', ''),
                        'amount': round(w.get('inflow', 0), 2),
                        'mcap': mcap,
                        'volume_24h': volume,
                        'age_hours': round(age_sec / 3600, 1),
                        'timestamp': now,
                        'confidence': min(90, 50 + (20 if w.get('inflow', 0) >= 1000 else 0) + (10 if mcap < 300000 else 0)),
                    }
                    new_signals.append(signal)
                    detected_signal_keys.add(key)

        # --- SIGNAL 3: Smart Money Concentration ---
        if volume > 0 and token_data.get('smart_inflow', 0) != 0:
            concentration = abs(token_data.get('smart_inflow', 0)) / volume
            if concentration >= CONCENTRATION_THRESHOLD:
                key = f'concentration:{token_addr}'
                if key not in detected_signal_keys:
                    signal = {
                        'type': 'concentration',
                        'token': symbol,
                        'token_name': name,
                        'token_address': token_addr,
                        'inflow_usd': round(token_data.get('smart_inflow', 0), 2),
                        'volume_24h': volume,
                        'concentration_pct': round(concentration * 100, 1),
                        'wallet_count': len(wallets),
                        'mcap': mcap,
                        'age_hours': round(age_sec / 3600, 1),
                        'timestamp': now,
                        'confidence': min(85, 40 + round(concentration * 100) + (10 if mcap < 500000 else 0)),
                    }
                    new_signals.append(signal)
                    detected_signal_keys.add(key)

    if new_signals:
        signals_db = new_signals + signals_db
        signals_db = signals_db[:200]  # keep last 200
        print(f'[signals] {len(new_signals)} new signals detected')

    return new_signals


async def send_tg_alert(signals: list[dict]):
    """Send signal alerts via Telegram bot."""
    import urllib.request
    if not signals:
        return

    token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
    if not token:
        return

    # Batch max 5 signals per message
    for batch in [signals[i:i+5] for i in range(0, len(signals), 5)]:
        lines = ['🚨 <b>PRE-HYPE SIGNAL</b>\n']

        emoji_map = {'accumulation': '🔥', 'large_buy': '💰', 'concentration': '📊'}

        for s in batch:
            emoji = emoji_map.get(s['type'], '⚡')
            type_label = s['type'].replace('_', ' ').upper()
            addr_short = s['token_address'][:6] + '...' + s['token_address'][-4:]

            if s['type'] == 'accumulation':
                lines.append(f'{emoji} <b>{type_label}</b> — ${s["token"]}')
                lines.append(f'{s["details"]}')
                lines.append(f'Inflow: ${s["total_usd"]:,.0f} | MCap: ${s["mcap"]:,.0f}')
                lines.append(f'Wallets: {", ".join(s["wallets"][:3])}')
            elif s['type'] == 'large_buy':
                lines.append(f'{emoji} <b>{type_label}</b> — ${s["token"]}')
                lines.append(f'{s["wallet"]} bought ${s["amount"]:,.0f}')
                lines.append(f'MCap: ${s["mcap"]:,.0f} | Age: {s["age_hours"]}h')
            elif s['type'] == 'concentration':
                lines.append(f'{emoji} <b>{type_label}</b> — ${s["token"]}')
                lines.append(f'Smart inflow: {s["concentration_pct"]}% of 24h vol')
                lines.append(f'${s["inflow_usd"]:,.0f} from {s["wallet_count"]} wallets')

            lines.append(f'Contract: <code>{addr_short}</code>')
            lines.append('')

        text = '\n'.join(lines)

        url = f'https://api.telegram.org/bot{token}/sendMessage'
        payload = json.dumps({
            'chat_id': TG_CHAT_ID,
            'text': text,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True,
        })

        try:
            print(f'[tg-alert] Sending batch of {len(batch)} signals...')
            req = urllib.request.Request(url, data=payload.encode(), headers={'Content-Type': 'application/json'})
            urllib.request.urlopen(req, timeout=10)
            print(f'[tg-alert] Sent {len(batch)} signals')
        except Exception as e:
            print(f'[tg-alert] Error: {e}')


async def scan_trenches():
    """Poll GMGN trenches for new tokens."""
    global last_scan_time, scan_count

    load_seen()
    scan_types = ["new_creation", "near_completion"]

    for scan_type in scan_types:
        args = [
            "market", "trenches",
            "--chain", "sol",
            "--type", scan_type,
            "--limit", "50",
        ]

        raw = gmgn_cli(*args)
        if not raw:
            continue

        tokens = []
        if isinstance(raw, dict):
            for key in [scan_type, "pump"]:
                items = raw.get(key, [])
                if isinstance(items, list):
                    tokens.extend(items)

        new_count = 0
        for t in tokens:
            addr = t.get("address", "")
            if not addr or addr in seen_set:
                continue

            seen_set.add(addr)
            formatted = format_token(t, scan_type)
            tokens_db[addr] = formatted
            new_count += 1

            if sse_subscribers:
                event_data = json.dumps(formatted)
                for q in sse_subscribers[:]:
                    try:
                        await q.put(event_data)
                    except Exception:
                        sse_subscribers.remove(q)

        if new_count:
            print(f"[scan] {scan_type}: {new_count} new tokens")

    save_seen()
    last_scan_time = time.time()
    scan_count += 1

    if len(seen_set) > SEEN_CACHE_SIZE:
        seen_list = sorted(seen_set, key=lambda a: tokens_db.get(a, {}).get("fetched_at", 0))
        seen_set.clear()
        seen_set.update(seen_list[-SEEN_CACHE_SIZE // 2:])


async def scanner_loop():
    """Background loop that scans every SCAN_INTERVAL seconds."""
    while True:
        try:
            await scan_trenches()
        except Exception as e:
            print(f"[scanner] error: {e}")
        await asyncio.sleep(SCAN_INTERVAL)


@app.on_event("startup")
async def startup():
    load_seen()
    asyncio.create_task(scanner_loop())
    asyncio.create_task(enrich_loop())
    asyncio.create_task(smart_wallet_loop())
    asyncio.create_task(smart_token_enrich_loop())
    print("[startup] Scanner + enricher + smart wallet + smart token enrich loops started")


# ── REST Endpoints ──────────────────────────────────────────────────────

@app.get("/api/tokens")
def get_tokens(
    source: Optional[str] = None,
    sort: str = Query("score", regex="^(score|age|holders|liquidity|market_cap|smart)$"),
    limit: int = Query(50, ge=1, le=200),
):
    """Get all tracked tokens, sorted."""
    tokens = list(tokens_db.values())

    if source and source != "all":
        tokens = [t for t in tokens if t["source"] == source]

    sort_key = {
        "score": lambda t: t["score"],
        "age": lambda t: t["age_seconds"],
        "holders": lambda t: t["holder_count"],
        "liquidity": lambda t: t["liquidity"],
        "market_cap": lambda t: t["market_cap"],
        "smart": lambda t: t["smart_degen_count"] + t["renowned_count"],
    }.get(sort, lambda t: t["score"])

    tokens.sort(key=sort_key, reverse=True)
    return {"tokens": tokens[:limit], "total": len(tokens_db), "scan_count": scan_count}


@app.get("/api/tokens/{address}")
def get_token(address: str):
    """Get a single token by address."""
    if address not in tokens_db:
        return JSONResponse({"error": "not found"}, status_code=404)
    return tokens_db[address]


@app.get("/api/trending")
def get_trending(limit: int = Query(20, ge=1, le=100)):
    """Get trending tokens by volume + smart money."""
    tokens = sorted(
        tokens_db.values(),
        key=lambda t: to_float(t.get("volume_24h", 0)) + t.get("smart_degen_count", 0) * 10000,
        reverse=True,
    )[:limit]
    return {"tokens": tokens}


@app.get("/api/smart-money")
def get_smart_money(limit: int = Query(20, ge=1, le=100)):
    """Get tokens with highest smart money activity."""
    tokens = sorted(
        tokens_db.values(),
        key=lambda t: t.get("smart_degen_count", 0) + t.get("renowned_count", 0),
        reverse=True,
    )[:limit]
    return {"tokens": tokens}


@app.get("/api/kol")
def get_kol_trades():
    """Get recent KOL trades from GMGN."""
    global kol_cache
    if kol_cache and time.time() - kol_cache.get("_ts", 0) < 120:
        return kol_cache

    raw = gmgn_cli("track", "kol", "--chain", "sol")
    if not raw:
        return {"trades": [], "_ts": time.time()}

    trades = raw if isinstance(raw, list) else raw.get("list", raw.get("trades", []))
    if not isinstance(trades, list):
        trades = []

    kol_cache = {"trades": trades[:50], "_ts": time.time()}
    return kol_cache


@app.get("/api/stats")
def get_stats():
    """Scanner stats."""
    tokens = list(tokens_db.values())
    total_vol = sum(t.get("volume_24h", 0) for t in tokens)
    avg_score = sum(t.get("score", 0) for t in tokens) / len(tokens) if tokens else 0
    enriched = sum(1 for t in tokens if t.get("enriched"))
    return {
        "total_tokens": len(tokens),
        "total_scans": scan_count,
        "last_scan": datetime.fromtimestamp(last_scan_time).isoformat() if last_scan_time else None,
        "total_volume_24h": round(total_vol, 2),
        "avg_score": round(avg_score, 1),
        "enriched": enriched,
        "smart_wallets_tracked": len(wallets_db),
    }


@app.get("/api/smart-tokens")
def get_smart_tokens(
    sort: str = Query("inflow", regex="^(inflow|mcap|holders|volume|wallets)$"),
    limit: int = Query(50, ge=1, le=200),
):
    """Get smart tokens grouped by token with smart wallet activity."""
    tokens = list(smart_tokens_db.values())
    now = int(time.time())

    sort_key = {
        "inflow": lambda t: abs(t.get("smart_inflow", 0)),
        "mcap": lambda t: t.get("mcap", 0),
        "holders": lambda t: t.get("holder_count", 0),
        "volume": lambda t: t.get("volume_24h", 0),
        "wallets": lambda t: len(t.get("wallets", {})),
    }.get(sort, lambda t: abs(t.get("smart_inflow", 0)))

    tokens.sort(key=sort_key, reverse=True)

    # Build unique wallets set across all tokens
    all_wallets = set()

    result = []
    for tk in tokens[:limit]:
        wallets_list = []
        for addr, wk in tk["wallets"].items():
            all_wallets.add(addr)

            # Action string: time since last action + action type
            age_sec = now - wk["last_action_ts"] if wk["last_action_ts"] else 0
            if age_sec < 60:
                age_str = f"{age_sec}s"
            elif age_sec < 3600:
                age_str = f"{age_sec // 60}m"
            elif age_sec < 86400:
                age_str = f"{age_sec // 3600}h"
            else:
                age_str = f"{age_sec // 86400}d"

            action_labels = {
                "first_buy": "First Buy",
                "buy_more": "Buy More",
                "sell_all": "Sell All",
                "sell_partial": "Sell Partial",
            }
            action_str = f"{age_str} {action_labels.get(wk['action_type'], wk['action_type'])}"

            wallets_list.append({
                "address": wk["address"],
                "twitter": wk.get("twitter", ""),
                "tags": wk.get("tags", []),
                "balance": round(wk["balance"], 2),
                "buys": wk["buys"],
                "sells": wk["sells"],
                "inflow": round(wk["inflow"], 2),
                "last_action_ts": wk["last_action_ts"],
                "action_type": wk["action_type"],
                "action_str": action_str,
            })

        # Sort wallets by inflow desc
        wallets_list.sort(key=lambda w: w["inflow"], reverse=True)

        result.append({
            "address": tk["address"],
            "symbol": tk["symbol"],
            "name": tk["name"],
            "logo": tk["logo"],
            "mcap": round(tk["mcap"], 2),
            "volume_24h": round(tk["volume_24h"], 2),
            "holder_count": tk["holder_count"],
            "price": tk["price"],
            "price_change_24h": round(tk.get("price_change_24h", 0), 2),
            "price_change_1h": round(tk.get("price_change_1h", 0), 2),
            "liquidity": round(tk["liquidity"], 2),
            "smart_inflow": round(tk["smart_inflow"], 2),
            "wallets": wallets_list,
        })

    return {
        "tokens": result,
        "total_tokens": len(smart_tokens_db),
        "total_wallets": len(all_wallets),
    }


# ── SSE Endpoint ────────────────────────────────────────────────────────

@app.get('/api/signals')
def get_signals(
    signal_type: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
):
    """Get detected pre-hype signals."""
    result = signals_db
    if signal_type:
        result = [s for s in result if s.get('type') == signal_type]
    return {'signals': result[:limit], 'total': len(signals_db)}

@app.get("/api/stream")
async def stream_tokens():
    """Server-Sent Events — real-time new token feed."""
    queue = asyncio.Queue()
    sse_subscribers.append(queue)

    async def event_generator():
        try:
            while True:
                data = await queue.get()
                yield {"event": "new_token", "data": data}
        except asyncio.CancelledError:
            if queue in sse_subscribers:
                sse_subscribers.remove(queue)

    return EventSourceResponse(event_generator())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
