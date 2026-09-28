import websocket
import msgpack
import json
import time
import threading
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import ssl
import math
import sys
import os
import logging

sys.stdout.reconfigure(line_buffering=True)

# ==========================================
# LOGGING
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("fishbot")

# ==========================================
# TOKEN FROM ENV (GitHub Secrets)
# ==========================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
if not TELEGRAM_BOT_TOKEN:
    log.error("❌ TELEGRAM_BOT_TOKEN env variable is missing. Set it in GitHub Secrets.")
    sys.exit(1)

# ==========================================
# WATCHDOG GLOBALS
# ==========================================
last_tg_ok = time.time()   # Telegram နောက်ဆုံး ပြန်ဖြေတဲ့ အချိန်
last_ws_ok = time.time()   # Game WS နောက်ဆုံး message ရတဲ့ အချိန်
TG_TIMEOUT = 120           # 2 မိနစ် TG မဖြေရင် restart
WS_TIMEOUT = 180           # 3 မိနစ် WS မရရင် (in-game ဆိုရင်) restart

# ==========================================
# CONFIG
# ==========================================
WS_URL = "wss://api-fishmcloud.ugame.vn:2083"
CONFIG_FILE = "bot_config.json"
WS_HEADERS = [
    "User-Agent: Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Origin: https://fishmya.ugame.vn",
    "Accept-Language: my-MM,my;q=0.9,en-US;q=0.8,en;q=0.7",
    "X-Requested-With: com.mytel.myid",
]

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# ==========================================
# CONFIG MANAGEMENT
# ==========================================
config_data = {
    "owner_id": None,
    "tokens": [],
    "selected_index": 0,
}


def load_config():
    global config_data
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                loaded = json.load(f)
                config_data.update(loaded)
                if "tokens" not in config_data:
                    config_data["tokens"] = []
                if "selected_index" not in config_data:
                    config_data["selected_index"] = 0
        except Exception:
            log.exception("load_config failed")


def save_config():
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(config_data, f, indent=2)
    except Exception:
        log.exception("save_config failed")


load_config()

# ==========================================
# BOT INSTANCE
# ==========================================
class BotInstance:
    def __init__(self, token, owner_id):
        self.token = token
        self.owner_id = owner_id
        self.is_running = False
        self.ws_conn = None
        self.ws_lock = threading.Lock()
        self.error_count = 0
        self.max_errors = 1
        self.last_error_msg = "None"
        self.is_restarting = False
        self.heartbeat_alive = False
        self.shoot_alive = False
        self.use_4x_alive = False
        self.in_game = False
        self.login_handled = False
        self.play_handled = False
        self.game_creds = {"username": "", "password": ""}
        self.fish_list = {}
        self.fish_lock = threading.Lock()
        self.current_angle_deg = 0.0
        self.drag_direction = 1
        self.cycle_duration = 120
        self.cycle_pause = 5
        self.speed_multiplier = 200
        self.stats = {
            "requests_sent": 0,
            "coins_spent": 0,
            "coins_gained": 0,
            "fish_killed": 0,
            "start_balance": 0,
            "current_balance": 0,
        }
        self.stats_lock = threading.Lock()
        self.current_target_id = None

    def reset_stats(self):
        with self.stats_lock:
            for k in self.stats:
                self.stats[k] = 0

    def log_stats(self):
        with self.stats_lock:
            profit = self.stats["coins_gained"] - self.stats["coins_spent"]
            msg = (
                f"📊 STATS [{self.token[:10]}...]\n"
                f"Requests: {self.stats['requests_sent']}\n"
                f"Spent: {self.stats['coins_spent']:,}\n"
                f"Gained: {self.stats['coins_gained']:,}\n"
                f"Profit: {profit:,}\n"
                f"Fish: {self.stats['fish_killed']}\n"
                f"Balance: {self.stats['current_balance']:,}"
            )
        if self.owner_id:
            try:
                bot.send_message(self.owner_id, msg)
            except Exception as e:
                log.warning(f"log_stats send failed: {e}")

    def send_ws(self, payload_dict):
        if self.ws_conn and self.ws_conn.connected:
            try:
                self.ws_conn.send(
                    msgpack.packb(payload_dict, use_bin_type=True),
                    opcode=websocket.ABNF.OPCODE_BINARY,
                )
                with self.stats_lock:
                    self.stats["requests_sent"] += 1
                    if payload_dict.get("route") == "shoot":
                        self.stats["coins_spent"] += 6
                return True
            except Exception as e:
                if not self.is_restarting:
                    self.error_count += 1
                    self.last_error_msg = str(e)
                log.warning(f"[{self.token[:10]}...] Send error: {e}")
        return False

    def stop_all_threads(self):
        self.heartbeat_alive = False
        self.shoot_alive = False
        self.use_4x_alive = False
        self.login_handled = False
        self.play_handled = False
        self.in_game = False
        self.current_target_id = None
        with self.fish_lock:
            self.fish_list.clear()

    def start_ws_connection(self):
        url = f"{WS_URL}?access_token={self.token}"
        try:
            conn = websocket.create_connection(
                url,
                header=WS_HEADERS,
                sslopt={"cert_reqs": ssl.CERT_NONE},
                timeout=30,
            )
            with self.ws_lock:
                self.ws_conn = conn
            log.info(f"[{self.token[:10]}...] WS Connected")
            self.send_ws(
                {
                    "route": "mytelLogin",
                    "data": {"accessToken": self.token, "language": "my"},
                    "msgId": 1,
                }
            )
            threading.Thread(
                target=self.ws_recv_loop, args=(conn,), daemon=True
            ).start()
        except Exception as e:
            log.warning(f"[{self.token[:10]}...] Connection failed: {e}")
            time.sleep(2)

    def ws_recv_loop(self, ws):
        while ws.connected and not self.is_restarting:
            try:
                data = ws.recv()
                if not data:
                    break
                self.handle_message(data, ws)
            except Exception:
                break

    def handle_message(self, data, ws):
        global last_ws_ok
        try:
            last_ws_ok = time.time()
            decoded = msgpack.unpackb(data, raw=False)
            if not isinstance(decoded, dict):
                return
            route = decoded.get("route", "")
            msg_id = decoded.get("msgId", -1)
            inner = decoded.get("data", decoded)
            if not isinstance(inner, dict):
                inner = {}

            if route == "OnUpdateObjects":
                objects = inner.get("objects", [])
                dead_fish = inner.get("deadFish", [])
                with self.fish_lock:
                    for obj in objects:
                        f_id = obj.get("id")
                        if f_id:
                            self.fish_list[f_id] = obj
                    for df in dead_fish:
                        f_id = df.get("id")
                        if f_id in self.fish_list:
                            if f_id == self.current_target_id:
                                self.current_target_id = None
                            del self.fish_list[f_id]
            elif route == "OnUpdateObject":
                f_id = inner.get("id")
                if f_id:
                    with self.fish_lock:
                        self.fish_list[f_id] = inner
            elif route == "OnObjectDie":
                f_id = inner.get("id")
                with self.fish_lock:
                    if f_id in self.fish_list:
                        if f_id == self.current_target_id:
                            self.current_target_id = None
                        del self.fish_list[f_id]
                if inner.get("playerId") == self.game_creds.get("username"):
                    with self.stats_lock:
                        self.stats["fish_killed"] += 1
                        self.stats["coins_gained"] += inner.get("cash", 0)
            elif route == "OnUpdateCash":
                if inner.get("playerId") == self.game_creds.get("username"):
                    with self.stats_lock:
                        self.stats["current_balance"] = inner.get("cash", 0)

            if msg_id == 1:
                if inner.get("ok"):
                    self.login_handled = True
                    self.game_creds["username"] = inner.get("username", "")
                    self.game_creds["password"] = inner.get("password", "")
                    with self.stats_lock:
                        self.stats["start_balance"] = inner.get("cash", 0)
                        self.stats["current_balance"] = inner.get("cash", 0)

                    if self.owner_id:
                        try:
                            bot.send_message(
                                self.owner_id,
                                f"✅ Login Success!\n"
                                f"👤 Nickname: {inner.get('nickname', 'Unknown')}\n"
                                f"🆔 Username: {self.game_creds['username']}\n"
                                f"💰 Balance: {inner.get('cash', 0):,}\n"
                                f"🔑 Token: {self.token[:15]}...",
                            )
                        except Exception as e:
                            log.warning(f"login notify failed: {e}")

                    if not self.heartbeat_alive:
                        threading.Thread(
                            target=self.heartbeat_loop, args=(ws,), daemon=True
                        ).start()
                    time.sleep(0.5)
                    self.send_ws(
                        {
                            "route": "play",
                            "data": {
                                "playerId": self.game_creds["username"],
                                "password": self.game_creds["password"],
                                "index": 0,
                            },
                            "msgId": 2,
                        }
                    )
            elif msg_id == 2:
                if inner.get("ok"):
                    self.play_handled = True
                    self.start_game_actions(ws)
        except Exception as e:
            if not self.is_restarting:
                self.error_count += 1
                self.last_error_msg = str(e)

    def heartbeat_loop(self, ws):
        self.heartbeat_alive = True
        while (
            self.is_running
            and self.heartbeat_alive
            and ws.connected
            and not self.is_restarting
        ):
            self.send_ws({"route": "ping", "data": {}, "msgId": 0})
            time.sleep(2)
        self.heartbeat_alive = False

    def auto_shoot_loop(self, ws):
        self.shoot_alive = True
        log.info("🎯 Fish Hunter Mode - Killing fish one by one")

        while (
            self.is_running
            and self.shoot_alive
            and ws.connected
            and not self.is_restarting
        ):
            try:
                with self.fish_lock:
                    fish_list_items = list(self.fish_list.values())

                if not fish_list_items:
                    self.current_target_id = None
                    time.sleep(0.1)
                    continue

                target_fish = None
                if self.current_target_id:
                    for fish in fish_list_items:
                        if fish.get("id") == self.current_target_id:
                            target_fish = fish
                            break

                if not target_fish and fish_list_items:
                    target_fish = fish_list_items[0]
                    self.current_target_id = target_fish.get("id")
                    log.info(f"🎯 New target: {self.current_target_id}")

                if not target_fish:
                    time.sleep(0.1)
                    continue

                fish_id = target_fish.get("id")
                if fish_id:
                    angle_rad = math.radians(self.current_angle_deg)

                    self.send_ws(
                        {
                            "route": "shoot",
                            "data": {
                                "rad": angle_rad,
                                "type": 4,
                                "target": fish_id,
                                "rapidFire": True,
                                "auto": True,
                                "bulletSpeed": 1400,
                            },
                            "msgId": 0,
                        }
                    )

                    self.send_ws(
                        {
                            "route": "clientHitFish",
                            "data": {
                                "btype": 4,
                                "skillType": 0,
                                "fIds": [fish_id],
                                "bulletSpeed": 1400,
                            },
                            "msgId": 0,
                        }
                    )

                    self.current_angle_deg += self.drag_direction * 0.1
                    if self.current_angle_deg >= 60.0:
                        self.current_angle_deg = 60.0
                        self.drag_direction = -1
                    elif self.current_angle_deg <= -60.0:
                        self.current_angle_deg = -60.0
                        self.drag_direction = 1

                    time.sleep(0.01)

            except Exception as e:
                log.warning(f"Fish hunter error: {e}")
                break

        self.shoot_alive = False

    def use_4x_loop(self, ws):
        self.use_4x_alive = True
        while (
            self.is_running
            and self.use_4x_alive
            and ws.connected
            and not self.is_restarting
        ):
            self.send_ws({"route": "useItem", "data": {"type": 6}, "msgId": 0})
            time.sleep(10)
        self.use_4x_alive = False

    def start_game_actions(self, ws):
        if not self.is_running or self.is_restarting:
            return
        self.in_game = True
        self.current_target_id = None
        self.send_ws({"route": "useItem", "data": {"type": 4}, "msgId": 0})
        self.send_ws(
            {
                "route": "clientActiveGun",
                "data": {
                    "btype": 4,
                    "gun": "gun1",
                    "skillType": "none",
                    "locationX": 0,
                    "locationY": 0,
                    "bulletSpeed": 1400,
                },
                "msgId": 0,
            }
        )
        if not self.shoot_alive:
            threading.Thread(
                target=self.auto_shoot_loop, args=(ws,), daemon=True
            ).start()
        if not self.use_4x_alive:
            threading.Thread(
                target=self.use_4x_loop, args=(ws,), daemon=True
            ).start()

    def run_cycle(self):
        log.info(f"[{self.token[:10]}...] Starting cycle")
        self.is_running = True
        self.reset_stats()
        self.error_count = 0
        self.is_restarting = False

        while self.is_running:
            self.start_ws_connection()
            start_time = time.time()
            while self.is_running and not self.is_restarting:
                elapsed = time.time() - start_time
                if elapsed >= self.cycle_duration:
                    log.info(f"[{self.token[:10]}...] Cycle finished")
                    self.log_stats()
                    break
                if self.error_count >= self.max_errors:
                    log.info(f"[{self.token[:10]}...] Max errors, restarting")
                    self.log_stats()
                    if self.owner_id:
                        try:
                            bot.send_message(
                                self.owner_id,
                                f"⚠️ Restarting {self.token[:10]}... (Err: {self.last_error_msg})",
                            )
                        except Exception:
                            pass
                    break
                if (
                    self.in_game
                    and not self.shoot_alive
                    and self.ws_conn
                    and self.ws_conn.connected
                ):
                    log.info(f"[{self.token[:10]}...] Shoot died, restarting")
                    self.log_stats()
                    break
                time.sleep(1)

            log.info(f"[{self.token[:10]}...] Closing connection")
            self.is_restarting = True
            with self.ws_lock:
                if self.ws_conn:
                    try:
                        self.ws_conn.close()
                    except Exception:
                        pass
                self.ws_conn = None
            self.stop_all_threads()
            self.error_count = 0
            if self.is_running:
                log.info(f"[{self.token[:10]}...] Pausing {self.cycle_pause}s")
                time.sleep(self.cycle_pause)
            self.is_restarting = False
        log.info(f"[{self.token[:10]}...] Stopped")


# ==========================================
# GLOBAL ACTIVE BOT CONTROLLER
# ==========================================
active_bot = None
active_thread = None
bot_lock = threading.Lock()


def get_selected_token():
    if not config_data["tokens"]:
        return None
    idx = config_data.get("selected_index", 0)
    if idx >= len(config_data["tokens"]):
        idx = 0
        config_data["selected_index"] = 0
        save_config()
    return config_data["tokens"][idx]


def start_bot():
    global active_bot, active_thread
    with bot_lock:
        if active_bot and active_bot.is_running:
            return "Already running."
        token = get_selected_token()
        if not token:
            return "No tokens available. Add one first."
        active_bot = BotInstance(token, config_data["owner_id"])
        t = threading.Thread(target=active_bot.run_cycle, daemon=True)
        t.start()
        active_thread = t
        return f"✅ Started with token: {token[:10]}..."


def stop_bot():
    global active_bot
    with bot_lock:
        if not active_bot or not active_bot.is_running:
            return "No bot running."
        active_bot.is_running = False
        active_bot = None
        return "🛑 Stopped."


def switch_token(index):
    global active_bot
    if not config_data["tokens"]:
        return "No tokens."
    if index < 0 or index >= len(config_data["tokens"]):
        return "Invalid index."
    if active_bot and active_bot.is_running:
        stop_bot()
    config_data["selected_index"] = index
    save_config()
    return f"✅ Switched to token {index + 1}: {config_data['tokens'][index][:10]}..."


# ==========================================
# TELEGRAM COMMANDS
# ==========================================
def get_main_markup():
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("▶️ Start", callback_data="cmd_start"),
        InlineKeyboardButton("🛑 Stop", callback_data="cmd_stop"),
        InlineKeyboardButton("📋 List Tokens", callback_data="cmd_list"),
        InlineKeyboardButton("🔀 Select", callback_data="cmd_select"),
        InlineKeyboardButton("➕ Add Token", callback_data="cmd_add"),
        InlineKeyboardButton("➖ Remove", callback_data="cmd_remove"),
    )
    return markup


@bot.message_handler(commands=["start"])
def handle_start_cmd(message):
    user_id = message.chat.id
    if config_data["owner_id"] is None:
        config_data["owner_id"] = user_id
        save_config()
        bot.send_message(user_id, "👑 You are Owner.")
    elif config_data["owner_id"] != user_id:
        return
    bot.send_message(
        user_id, "🤖 Fish Bot Manager (Single Active)", reply_markup=get_main_markup()
    )


@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    user_id = call.message.chat.id
    if config_data["owner_id"] != user_id:
        bot.answer_callback_query(call.id, "Unauthorized")
        return
    cmd = call.data
    if cmd == "cmd_start":
        msg = start_bot()
        bot.answer_callback_query(call.id, msg)
        bot.send_message(user_id, msg)
    elif cmd == "cmd_stop":
        msg = stop_bot()
        bot.answer_callback_query(call.id, msg)
        bot.send_message(user_id, msg)
    elif cmd == "cmd_list":
        if not config_data["tokens"]:
            bot.send_message(user_id, "No tokens.")
        else:
            text = "📋 Tokens:\n"
            for i, t in enumerate(config_data["tokens"]):
                selected = "✅ " if i == config_data.get("selected_index", 0) else "   "
                text += f"{selected} {i + 1}. {t[:10]}...\n"
            bot.send_message(user_id, text)
    elif cmd == "cmd_select":
        bot.send_message(user_id, "Use /select <index> (e.g. /select 1)")
    elif cmd == "cmd_add":
        bot.send_message(user_id, "Use /add_token <token>")
    elif cmd == "cmd_remove":
        bot.send_message(user_id, "Use /remove_token <index>")


@bot.message_handler(commands=["add_token"])
def add_token(message):
    user_id = message.chat.id
    if config_data["owner_id"] != user_id:
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /add_token <token>")
        return
    token = parts[1].strip()
    if token not in config_data["tokens"]:
        config_data["tokens"].append(token)
        save_config()
        bot.reply_to(message, f"✅ Added: {token[:10]}...")
    else:
        bot.reply_to(message, "Already exists.")


@bot.message_handler(commands=["remove_token"])
def remove_token(message):
    user_id = message.chat.id
    if config_data["owner_id"] != user_id:
        return
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /remove_token <index>")
        return
    try:
        idx = int(parts[1]) - 1
        if 0 <= idx < len(config_data["tokens"]):
            removed = config_data["tokens"].pop(idx)
            if config_data.get("selected_index", 0) >= len(config_data["tokens"]):
                config_data["selected_index"] = max(0, len(config_data["tokens"]) - 1)
            save_config()
            if active_bot and active_bot.is_running and active_bot.token == removed:
                stop_bot()
            bot.reply_to(message, f"✅ Removed: {removed[:10]}...")
        else:
            bot.reply_to(message, "Invalid index.")
    except Exception:
        bot.reply_to(message, "Invalid number.")


@bot.message_handler(commands=["select"])
def select_token(message):
    user_id = message.chat.id
    if config_data["owner_id"] != user_id:
        return
    parts = message.text.split()
    if len(parts) < 2:
        bot.reply_to(message, "Usage: /select <index>")
        return
    try:
        idx = int(parts[1]) - 1
        if 0 <= idx < len(config_data["tokens"]):
            if active_bot and active_bot.is_running:
                stop_bot()
            config_data["selected_index"] = idx
            save_config()
            bot.reply_to(message, f"✅ Selected: {config_data['tokens'][idx][:10]}...")
        else:
            bot.reply_to(message, "Invalid index.")
    except Exception:
        bot.reply_to(message, "Invalid number.")


@bot.message_handler(commands=["status"])
def status_cmd(message):
    user_id = message.chat.id
    if config_data["owner_id"] != user_id:
        return
    status = "🔴 Stopped" if not active_bot or not active_bot.is_running else "🟢 Running"
    token = get_selected_token()
    token_show = token[:10] + "..." if token else "None"
    fish_count = len(active_bot.fish_list) if active_bot else 0
    bot.send_message(
        user_id,
        f"Status: {status}\n"
        f"Active Token: {token_show}\n"
        f"Total Tokens: {len(config_data['tokens'])}\n"
        f"🐟 Fish on Screen: {fish_count}",
    )


# ==========================================
# WATCHDOG + POLLING + SUPERVISOR
# ==========================================
def polling_loop():
    """Telegram ကို long-poll လုပ်ပြီး last_tg_ok ကို update လုပ်တယ်"""
    global last_tg_ok
    offset = None
    try:
        bot.delete_webhook(drop_pending_updates=False)
    except Exception as e:
        log.warning(f"delete_webhook failed: {e}")

    while True:
        try:
            updates = bot.get_updates(
                offset=offset,
                timeout=25,
                long_polling_timeout=20,
            )
            last_tg_ok = time.time()

            if updates:
                for u in updates:
                    offset = u.update_id + 1
                    try:
                        bot.process_new_updates([u])
                    except Exception:
                        log.exception("process_new_updates failed")
        except Exception as e:
            log.exception(f"getUpdates error: {e}")
            time.sleep(5)


def watchdog_loop():
    """TG/WS နှစ်ခုလုံး ဂပ်နေရင် process ကို သတ်ပြီး parent loop က ပြန် run စေတယ်"""
    global last_tg_ok, last_ws_ok
    while True:
        time.sleep(20)
        now = time.time()

        idle_tg = now - last_tg_ok
        if idle_tg > TG_TIMEOUT:
            log.error(f"⛔ Telegram silent for {idle_tg:.0f}s → self-restart")
            os._exit(1)

        if active_bot and active_bot.is_running and active_bot.in_game:
            idle_ws = now - last_ws_ok
            if idle_ws > WS_TIMEOUT:
                log.error(f"⛔ WS silent for {idle_ws:.0f}s → self-restart")
                os._exit(1)


def main_loop():
    """Main supervisor — crash ဖြစ်ရင် restart"""
    while True:
        try:
            log.info("🚀 Starting bot supervisor...")
            threading.Thread(target=watchdog_loop, daemon=True).start()
            polling_loop()
        except SystemExit:
            raise
        except Exception:
            log.exception("💥 Bot crashed → restart in 10s")
            time.sleep(10)


if __name__ == "__main__":
    main_loop()
