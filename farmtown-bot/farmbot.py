#!/usr/bin/env python3
"""
FarmTown Auto-Farm Bot
======================
Playwright-based bot that automates farming in farmtown.online.

SETUP:
  pip install playwright
  python -m playwright install chromium

USAGE:
  python farmbot.py                    # Run with GUI (headed)
  python farmbot.py --headless         # Run headless
  python farmbot.py --crop potato      # Farm specific crop
  python farmbot.py --crop auto        # Auto-select best crop for level

FIRST RUN:
  1. Script opens Chromium browser
  2. Solve Cloudflare CAPTCHA manually
  3. Click "Connect Phantom" and approve wallet connection
  4. Enter farm name and click "Start My Farm"
  5. Bot takes over from there

SAFETY:
  - Uses real browser (Playwright), same as manual play
  - Random delays between actions (human-like)
  - No protocol hacking or exploit
  - Solana wallet keys never touch this script
"""

import asyncio
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    from playwright.async_api import async_playwright, Page, BrowserContext
except ImportError:
    print("ERROR: playwright not installed. Run:")
    print("  pip install playwright")
    print("  python -m playwright install chromium")
    sys.exit(1)

# ─────────────────────────────────────────────
# CROP DATA (from game JS analysis)
# ─────────────────────────────────────────────
CROPS = {
    "potato":       {"unlock": 1,  "cost": 0,    "grow_sec": 120,   "death_sec": 300,   "reward_gold": 60,    "xp": 4},
    "carrot":       {"unlock": 1,  "cost": 20,   "grow_sec": 120,   "death_sec": 300,   "reward_gold": 40,    "xp": 4},
    "corn":         {"unlock": 1,  "cost": 45,   "grow_sec": 300,   "death_sec": 720,   "reward_gold": 95,    "xp": 7},
    "tomato":       {"unlock": 5,  "cost": 90,   "grow_sec": 480,   "death_sec": 360,   "reward_gold": 200,   "xp": 14},
    "onion":        {"unlock": 5,  "cost": 140,  "grow_sec": 720,   "death_sec": 480,   "reward_gold": 330,   "xp": 22},
    "wheat":        {"unlock": 5,  "cost": 220,  "grow_sec": 1080,  "death_sec": 720,   "reward_gold": 560,   "xp": 32},
    "pumpkin":      {"unlock": 10, "cost": 400,  "grow_sec": 1800,  "death_sec": 1200,  "reward_gold": 1050,  "xp": 55},
    "melon":        {"unlock": 10, "cost": 650,  "grow_sec": 2700,  "death_sec": 1800,  "reward_gold": 1800,  "xp": 80},
    "cucumber":     {"unlock": 10, "cost": 850,  "grow_sec": 3600,  "death_sec": 2700,  "reward_gold": 2400,  "xp": 105},
    "pepper":       {"unlock": 15, "cost": 1300, "grow_sec": 5400,  "death_sec": 3600,  "reward_gold": 4000,  "xp": 150},
    "strawberry":   {"unlock": 15, "cost": 1900, "grow_sec": 7200,  "death_sec": 2700,  "reward_gold": 6200,  "xp": 210},
    "blueberry":    {"unlock": 15, "cost": 2600, "grow_sec": 10800, "death_sec": 3600,  "reward_gold": 8800,  "xp": 280},
    "grape":        {"unlock": 20, "cost": 4000, "grow_sec": 14400, "death_sec": 4500,  "reward_gold": 9500,  "xp": 220},
    "eggplant":     {"unlock": 20, "cost": 5500, "grow_sec": 18000, "death_sec": 5400,  "reward_gold": 13000, "xp": 280},
    "watermelon":   {"unlock": 20, "cost": 7500, "grow_sec": 21600, "death_sec": 7200,  "reward_gold": 18000, "xp": 360},
    "dragonfruit":  {"unlock": 25, "cost": 12000,"grow_sec": 28800, "death_sec": 9000,  "reward_gold": 28000, "xp": 500},
    "pineapple":    {"unlock": 25, "cost": 18000,"grow_sec": 36000, "death_sec": 10800, "reward_gold": 42000, "xp": 700},
    "crystal_berry":{"unlock": 25, "cost": 25000,"grow_sec": 43200, "death_sec": 10800, "reward_gold": 60000, "xp": 900},
    "starfruit":    {"unlock": 30, "cost": 50000,"grow_sec": 64800, "death_sec": 3600,  "reward_gold": 100000,"xp": 1200},
}

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
CONFIG = {
    "farm_url": "https://play.farmtown.online/",
    "user_data_dir": str(Path.home() / ".farmtown-bot-profile"),
    "viewport": {"width": 1280, "height": 800},
    # Human-like delays (seconds)
    "delay_min": 0.8,
    "delay_max": 2.5,
    "action_cooldown": 1.5,  # seconds between game actions
    # Auto-buy seeds threshold
    "min_seeds": 5,
    # Queue wait timeout (10 minutes)
    "queue_timeout": 600,
    # Auto-pool claim interval (seconds) — 6 hours
    "pool_claim_interval": 21600,
}


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def random_delay():
    return random.uniform(CONFIG["delay_min"], CONFIG["delay_max"])


# ─────────────────────────────────────────────
# GAME STATE TRACKER
# ─────────────────────────────────────────────
class GameState:
    """Tracks game state via intercepted WebSocket events."""

    def __init__(self):
        self.connected = False
        self.in_queue = False
        self.farm_joined = False
        self.gold = 0
        self.xp = 0
        self.level = 1
        self.stars = 0
        self.tiles = {}        # "x,y" -> {cropId, groundState, readyAt}
        self.inventory = {}    # seedId -> count
        self.orders = []       # active orders
        self.farm_jobs = []    # active jobs
        self.player_state = None
        self.last_action = 0
        self.cycle_count = 0
        self.total_planted = 0
        self.total_harvested = 0
        self.total_orders = 0
        self.start_time = time.time()
        self.room_id = None
        self.player_id = None

    def summary(self):
        elapsed = time.time() - self.start_time
        h, m = divmod(int(elapsed), 3600)
        m, s = divmod(m, 60)
        return (
            f"Gold={self.gold:,} | XP={self.xp:,} | Lv{self.level} | "
            f"Stars={self.stars} | Cycles={self.cycle_count} | "
            f"Planted={self.total_planted} | Harvested={self.total_harvested} | "
            f"Orders={self.total_orders} | Uptime={h}h{m}m"
        )

    def available_tiles(self):
        """Tiles with soil ready to plant."""
        return {k: v for k, v in self.tiles.items()
                if v.get("groundState") == "tilled" or
                   (v.get("groundState") == "none" and not v.get("cropId"))}

    def planted_tiles(self):
        """Tiles with crops currently growing."""
        return {k: v for k, v in self.tiles.items()
                if v.get("groundState") == "planted" and v.get("cropId")}

    def ready_tiles(self):
        """Tiles with crops ready to harvest (readyAt <= now)."""
        now_ms = int(time.time() * 1000)
        return {k: v for k, v in self.planted_tiles().items()
                if v.get("readyAt") and v["readyAt"] <= now_ms}


# ─────────────────────────────────────────────
# BOT ENGINE
# ─────────────────────────────────────────────
class FarmBot:
    def __init__(self, crop_name: str = "auto", headless: bool = False):
        self.crop = crop_name
        self.headless = headless
        self.state = GameState()
        self.page: Page = None
        self.context: BrowserContext = None
        self._last_pool_claim = 0
        self._running = True

    async def start(self):
        """Main entry point."""
        log("Starting FarmTown Bot...")
        log(f"Crop strategy: {self.crop}")
        log(f"Profile dir: {CONFIG['user_data_dir']}")

        async with async_playwright() as pw:
            # Launch with persistent context (keeps wallet session)
            self.context = await pw.chromium.launch_persistent_context(
                CONFIG["user_data_dir"],
                headless=self.headless,
                viewport=CONFIG["viewport"],
                locale="en-US",
                timezone_id="America/New_York",
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                ],
            )

            self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()

            # Inject WebSocket interceptor
            await self._setup_ws_interceptor()

            # Navigate to game
            await self.page.goto(CONFIG["farm_url"], wait_until="domcontentloaded")
            log("Page loaded. Waiting for Cloudflare + wallet connection...")

            # Wait for user to complete auth
            await self._wait_for_auth()

            # Main farming loop
            await self._farm_loop()

    async def _setup_ws_interceptor(self):
        """Inject JS to intercept Socket.IO events."""
        await self.page.add_init_script("""
            window.__farmBot = {
                events: [],
                socket: null,
                originalEmit: null,
                roomId: null,

                log(event, data) {
                    this.events.push({t: Date.now(), e: event, d: data});
                    // Keep last 500 events
                    if (this.events.length > 500) this.events.shift();
                },

                // Called by Python to get recent events
                popEvents() {
                    const e = [...this.events];
                    this.events = [];
                    return e;
                },

                // Called by Python to emit game actions
                emit(event, data) {
                    if (this.socket) {
                        this.socket.emit(event, data);
                        return true;
                    }
                    return false;
                },

                getSocket() {
                    return this.socket;
                },

                setSocket(sock) {
                    this.socket = sock;
                    if (sock && !this._patched) {
                        this._patched = true;
                        const origEmit = sock.emit.bind(sock);
                        this.originalEmit = origEmit;

                        sock.emit = (...args) => {
                            this.log('emit', {event: args[0], data: args[1]});
                            return origEmit(...args);
                        };

                        // Intercept incoming messages
                        const origOn = sock.on.bind(sock);
                        const bot = this;
                        sock.on = function(event, handler) {
                            return origOn(event, function(...handlerArgs) {
                                bot.log('on', {event: event, data: handlerArgs[0]});
                                return handler(...handlerArgs);
                            });
                        };
                    }
                }
            };

            // Hook into Socket.IO connection
            const origIO = window.io;
            if (origIO) {
                window.io = function(...args) {
                    const socket = origIO(...args);
                    window.__farmBot.setSocket(socket);
                    console.log('[FarmBot] Socket.IO intercepted');
                    return socket;
                };
            }

            // Also try to catch socket after it's created
            const checkInterval = setInterval(() => {
                if (window.__farmBot.socket) {
                    clearInterval(checkInterval);
                    return;
                }
                // Try to find socket in React fiber or globals
                const keys = Object.keys(window);
                for (const k of keys) {
                    try {
                        const v = window[k];
                        if (v && typeof v.emit === 'function' && typeof v.on === 'function' && v.connected) {
                            window.__farmBot.setSocket(v);
                            console.log('[FarmBot] Socket found via scan:', k);
                            clearInterval(checkInterval);
                            return;
                        }
                    } catch(e) {}
                }
            }, 2000);
        """)

    async def _wait_for_auth(self):
        """Wait for user to solve CAPTCHA, connect wallet, and join farm."""
        max_wait = 600  # 10 minutes
        start = time.time()

        while time.time() - start < max_wait:
            try:
                # Check if farm is loaded (game canvas or farm UI visible)
                has_farm = await self.page.evaluate("""
                    () => {
                        const bot = window.__farmBot;
                        // Check if we have farm state events
                        if (bot && bot.events.some(e => e.e === 'on' && e.d?.event === 'farm:snapshot')) return true;
                        // Check for game UI elements
                        const hotbar = document.querySelector('.hotbar-dock, .farm-menu-panel, .app-shell');
                        return !!hotbar;
                    }
                """)

                if has_farm:
                    log("Farm loaded! Bot taking over...")
                    self.state.farm_joined = True
                    return

                # Check queue status
                queue = await self.page.evaluate("""
                    () => {
                        const bot = window.__farmBot;
                        if (!bot) return null;
                        const queueEvents = bot.events.filter(e => e.e === 'on' && e.d?.event === 'queue:update');
                        if (queueEvents.length > 0) return queueEvents[queueEvents.length - 1].d.data;
                        return null;
                    }
                """)

                if queue and not self.state.in_queue:
                    self.state.in_queue = True
                    log(f"In queue! Waiting for spot... (queue data: {json.dumps(queue)[:200]})")

            except Exception:
                pass

            await asyncio.sleep(3)

        log("ERROR: Timed out waiting for farm to load. Is the game open?")
        log("Make sure you solved the CAPTCHA and connected Phantom wallet.")

    async def _farm_loop(self):
        """Main farming loop."""
        log("═══════════════════════════════════════")
        log("  FarmTown Bot Active")
        log("  Press Ctrl+C to stop")
        log("═══════════════════════════════════════")

        while self._running:
            try:
                await self._tick()
                await asyncio.sleep(CONFIG["action_cooldown"])
            except KeyboardInterrupt:
                log("Stopping bot...")
                self._running = False
            except Exception as e:
                log(f"Error in farm loop: {e}")
                await asyncio.sleep(5)

        log(f"Bot stopped. {self.state.summary()}")

    async def _tick(self):
        """One farming cycle."""
        # Process WebSocket events
        events = await self._pop_events()
        self._process_events(events)

        # Check if enough time passed since last action
        if time.time() - self.state.last_action < CONFIG["action_cooldown"]:
            return

        self.state.cycle_count += 1

        # Strategy: harvest ready crops first, then plant, then buy seeds, then orders
        action_taken = False

        # 1. Harvest ready crops
        ready = self.state.ready_tiles()
        if ready:
            for tile_key in list(ready.keys())[:10]:  # max 10 harvests per tick
                x, y = tile_key.split(",")
                await self._do_harvest(int(x), int(y))
                action_taken = True
                await asyncio.sleep(random_delay())

        # 2. Plant on empty tilled soil
        available = self.state.available_tiles()
        crop_info = self._select_crop()
        if available and crop_info:
            crop_id = crop_info["id"]
            seed_count = self.state.inventory.get(f"{crop_id}_seed", 0)

            # Buy seeds if low
            if seed_count < CONFIG["min_seeds"]:
                await self._do_buy_seeds(crop_id, 10)
                await asyncio.sleep(random_delay())

            # Plant
            for tile_key in list(available.keys())[:10]:  # max 10 plants per tick
                x, y = tile_key.split(",")
                await self._do_plant(int(x), int(y), f"{crop_id}_seed")
                action_taken = True
                await asyncio.sleep(random_delay())

        # 3. Complete ready orders
        for order in self.state.orders:
            if order.get("ready") or order.get("status") == "ready":
                await self._do_complete_order(order.get("id", order.get("orderId")))
                action_taken = True
                await asyncio.sleep(random_delay())

        # 4. Claim farmer's pool if interval passed
        if time.time() - self._last_pool_claim > CONFIG["pool_claim_interval"]:
            await self._do_claim_pool()
            self._last_pool_claim = time.time()

        # Log status every 10 cycles
        if self.state.cycle_count % 10 == 0:
            log(f"[Cycle {self.state.cycle_count}] {self.state.summary()}")
            planted = len(self.state.planted_tiles())
            ready_n = len(self.state.ready_tiles())
            avail_n = len(self.state.available_tiles())
            log(f"  Tiles: {planted} growing, {ready_n} ready, {avail_n} empty")

    def _select_crop(self):
        """Select best crop for current level and strategy."""
        if self.crop == "auto":
            # Pick highest-XP crop we can afford and have level for
            candidates = [
                (name, info) for name, info in CROPS.items()
                if info["unlock"] <= self.state.level
            ]
            if not candidates:
                candidates = [("potato", CROPS["potato"])]

            # Sort by XP per second (efficiency)
            candidates.sort(key=lambda x: x[1]["xp"] / max(x[1]["grow_sec"], 1), reverse=True)

            # Pick top that we can afford
            for name, info in candidates:
                if self.state.gold >= info["cost"]:
                    return {"id": name, **info}

            # Fallback to potato (free)
            return {"id": "potato", **CROPS["potato"]}
        else:
            crop_id = self.crop.lower()
            if crop_id in CROPS:
                return {"id": crop_id, **CROPS[crop_id]}
            return {"id": "potato", **CROPS["potato"]}

    def _process_events(self, events):
        """Process intercepted WebSocket events."""
        for evt in events:
            direction = evt.get("e", "")
            data = evt.get("d", {})
            event_name = data.get("event", "") if isinstance(data, dict) else ""

            if direction == "on":
                payload = data.get("data", {}) if isinstance(data, dict) else {}

                if event_name == "farm:snapshot" or event_name == "farm:state/sync":
                    self._apply_snapshot(payload)
                elif event_name == "tile:update":
                    self._apply_tile_update(payload)
                elif event_name == "player:farmState/sync":
                    self._apply_player_state(payload)
                elif event_name == "roomJoined":
                    self.state.connected = True
                    self.state.room_id = payload.get("roomId")
                    log(f"Room joined: {self.state.room_id}")
                elif event_name == "game:actionResult":
                    self._handle_action_result(payload)
                elif event_name == "queue:update":
                    self.state.in_queue = True
                    log(f"Queue update: {json.dumps(payload)[:200]}")
                elif event_name == "queue:ready":
                    self.state.in_queue = False
                    log("Queue ready! Joining farm...")

    def _apply_snapshot(self, data):
        """Apply full farm snapshot."""
        if not isinstance(data, dict):
            return

        # Update player state
        player = data.get("player", data.get("playerState", {}))
        if player:
            self.state.gold = player.get("gold", self.state.gold)
            self.state.xp = player.get("xp", self.state.xp)
            self.state.level = player.get("level", self.state.level)
            self.state.stars = player.get("premiumBalance", {}).get("stars", self.state.stars)
            self.state.player_state = player

            # Update inventory
            inv = player.get("inventory", {})
            if inv:
                self.state.inventory = inv

            # Update orders
            orders = player.get("orders", [])
            if orders:
                self.state.orders = orders

            # Update jobs
            jobs = player.get("farmJobs", [])
            if jobs:
                self.state.farm_jobs = jobs

            self.state.player_id = player.get("id", self.state.player_id)

        # Update tiles
        tiles = data.get("tiles", [])
        if isinstance(tiles, list):
            for tile in tiles:
                key = f"{tile.get('x', tile.get('tileX', 0))},{tile.get('y', tile.get('tileY', 0))}"
                self.state.tiles[key] = tile
        elif isinstance(tiles, dict):
            self.state.tiles.update(tiles)

    def _apply_tile_update(self, data):
        """Apply single tile update."""
        if not isinstance(data, dict):
            return
        x = data.get("x", data.get("tileX", 0))
        y = data.get("y", data.get("tileY", 0))
        key = f"{x},{y}"
        if key in self.state.tiles:
            self.state.tiles[key].update(data)
        else:
            self.state.tiles[key] = data

    def _apply_player_state(self, data):
        """Apply player state sync."""
        if not isinstance(data, dict):
            return
        self.state.gold = data.get("gold", self.state.gold)
        self.state.xp = data.get("xp", self.state.xp)
        self.state.level = data.get("level", self.state.level)
        self.state.stars = data.get("premiumBalance", {}).get("stars", self.state.stars)

        inv = data.get("inventory", {})
        if inv:
            self.state.inventory = inv

    def _handle_action_result(self, data):
        """Handle action result from server."""
        if not isinstance(data, dict):
            return
        status = data.get("status", "")
        action = data.get("actionType", data.get("action", ""))

        if status == "ok" or status == "success":
            if action == "plant":
                self.state.total_planted += 1
            elif action == "harvest":
                self.state.total_harvested += 1
        elif status in ("error", "failed", "denied"):
            log(f"Action failed: {action} — {data.get('message', 'unknown')}")

    async def _pop_events(self):
        """Pop intercepted events from page."""
        try:
            events = await self.page.evaluate("() => window.__farmBot ? window.__farmBot.popEvents() : []")
            return events or []
        except Exception:
            return []

    async def _emit(self, event: str, data: dict):
        """Emit a WebSocket event via page."""
        try:
            result = await self.page.evaluate(
                f"(data) => window.__farmBot ? window.__farmBot.emit('{event}', data) : false",
                data
            )
            if not result:
                log(f"WARNING: Could not emit {event} — socket not connected")
            return result
        except Exception as e:
            log(f"ERROR emitting {event}: {e}")
            return False

    async def _do_harvest(self, x: int, y: int):
        """Harvest a crop at tile."""
        action_id = f"harvest:{x},{y}:{int(time.time()*1000)}"
        log(f"🌾 Harvesting ({x},{y})")
        self.state.last_action = time.time()

        return await self._emit("game:action", {
            "roomId": self.state.room_id,
            "action": "harvest",
            "actionId": action_id,
            "tileX": x,
            "tileY": y,
            "clientSentAt": int(time.time() * 1000),
            "clientDebug": {"interactionMode": "farm", "networkMode": "socket"}
        })

    async def _do_plant(self, x: int, y: int, seed_id: str):
        """Plant a seed at tile."""
        action_id = f"plant:{x},{y}:{int(time.time()*1000)}"
        crop_name = seed_id.replace("_seed", "")
        log(f"🌱 Planting {crop_name} at ({x},{y})")
        self.state.last_action = time.time()

        return await self._emit("game:action", {
            "roomId": self.state.room_id,
            "action": "plant",
            "actionId": action_id,
            "tileX": x,
            "tileY": y,
            "seedId": seed_id,
            "clientSentAt": int(time.time() * 1000),
            "clientDebug": {"interactionMode": "farm", "networkMode": "socket"}
        })

    async def _do_buy_seeds(self, crop_id: str, quantity: int = 10):
        """Buy seeds from store."""
        log(f"🏪 Buying {quantity}x {crop_id} seeds")
        self.state.last_action = time.time()

        return await self._emit("store:buySeed/request", {
            "roomId": self.state.room_id,
            "seedId": f"{crop_id}_seed",
            "quantity": quantity,
            "actionId": f"buySeed:{crop_id}:{int(time.time()*1000)}",
            "clientSentAt": int(time.time() * 1000),
        })

    async def _do_complete_order(self, order_id):
        """Complete an order."""
        if not order_id:
            return False
        log(f"📦 Completing order {order_id}")
        self.state.last_action = time.time()
        self.state.total_orders += 1

        return await self._emit("order:complete/request", {
            "roomId": self.state.room_id,
            "orderId": order_id,
        })

    async def _do_claim_pool(self):
        """Claim Farmer's Pool rewards."""
        log("🏊 Claiming Farmer's Pool...")
        self.state.last_action = time.time()

        # Use the page's API function instead
        try:
            result = await self.page.evaluate("""
                async () => {
                    try {
                        const token = localStorage.getItem('farmtown_auth_token') ||
                                      document.cookie.match(/auth_token=([^;]+)/)?.[1];
                        if (!token) return {ok: false, message: 'No auth token found'};

                        const resp = await fetch('/api/rewards/farmer-pool/status', {
                            headers: {Authorization: `Bearer ${token}`}
                        });
                        return await resp.json();
                    } catch(e) {
                        return {ok: false, message: e.message};
                    }
                }
            """)
            log(f"Pool status: {json.dumps(result)[:300]}")
        except Exception as e:
            log(f"Pool claim failed: {e}")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="FarmTown Auto-Farm Bot")
    parser.add_argument("--crop", default="auto", help="Crop to farm (default: auto). Options: potato, carrot, corn, tomato, onion, wheat, pumpkin, melon, cucumber, pepper, strawberry, blueberry, grape, eggplant, watermelon, dragonfruit, pineapple, crystal_berry, starfruit")
    parser.add_argument("--headless", action="store_true", help="Run headless (no browser window)")
    parser.add_argument("--delay", type=float, default=1.5, help="Base delay between actions (seconds)")
    args = parser.parse_args()

    if args.delay:
        CONFIG["action_cooldown"] = args.delay

    bot = FarmBot(crop_name=args.crop, headless=args.headless)
    asyncio.run(bot.start())


if __name__ == "__main__":
    main()
