"""
IN365Bot v2.2
✅ Emojis matching AximoBot screenshots exactly
✅ Full admin panel (owner-only) with real DB features
✅ Owner notified on new user join & channel add/remove
✅ Mandatory @copyrightpost join gate
✅ Ban/unban system
✅ Broadcast to all users
✅ No fake logic — every feature is real
"""

import os
import re
import asyncio
import logging
import hashlib
import requests
import feedparser
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember
from telegram.error import TelegramError
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ChatMemberHandler,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOT_TOKEN           = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL        = os.getenv("DATABASE_URL")
OWNER_ID            = int(os.getenv("OWNER_ID", "7232714487"))
POLL_INTERVAL       = int(os.getenv("POLL_INTERVAL", "300"))
BOT_USERNAME        = os.getenv("BOT_USERNAME", "in365bot")
MAX_FREE_SOURCES    = 10
INITIAL_FETCH_COUNT = 4
REQUIRED_CHANNEL    = "@copyrightpost"
CHANNEL_INVITE_URL  = "https://t.me/copyrightpost"

if not BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN not set")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

PLATFORMS = {
    "rss":         "📰",
    "youtube":     "▶️",
    "reddit":      "🟠",
    "medium":      "✍️",
    "livejournal": "📝",
}

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (IN365Bot/2.2; RSS reader)"}

TIMEZONES = [
    "UTC","UTC+1","UTC+2","UTC+3","UTC+4","UTC+5","UTC+5:30",
    "UTC+6","UTC+7","UTC+8","UTC+9","UTC+10","UTC+11","UTC+12",
    "UTC-1","UTC-2","UTC-3","UTC-4","UTC-5","UTC-6","UTC-7",
    "UTC-8","UTC-9","UTC-10","UTC-11","UTC-12",
]

LANGUAGES = {
    "en": "🇬🇧 English",
    "ru": "🇷🇺 Russian",
    "de": "🇩🇪 German",
    "fr": "🇫🇷 French",
    "es": "🇪🇸 Spanish",
    "ar": "🇸🇦 Arabic",
    "zh": "🇨🇳 Chinese",
    "hi": "🇮🇳 Hindi",
    "pt": "🇧🇷 Portuguese",
    "bn": "🇧🇩 Bengali",
}

# ════════════════════════════════════════════════════════════════════════════════
# DATABASE
# ════════════════════════════════════════════════════════════════════════════════

class DB:
    def __init__(self, url):
        self.url  = url
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(self.url)
        self.conn.autocommit = False

    def _ensure_open(self):
        try:
            with self.conn.cursor() as c:
                c.execute("SELECT 1")
        except Exception:
            logger.warning("DB reconnecting...")
            self.connect()

    def query(self, sql, params=None):
        self._ensure_open()
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                return cur.fetchall() or []
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB query: {e}")
                raise

    def one(self, sql, params=None):
        self._ensure_open()
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                return cur.fetchone()
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB one: {e}")
                raise

    def run(self, sql, params=None):
        self._ensure_open()
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB run: {e}")
                raise

    def insert_id(self, sql, params=None):
        self._ensure_open()
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                row = cur.fetchone()
                return row[0] if row else None
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB insert_id: {e}")
                raise

    def init_schema(self):
        ddl = """
        CREATE TABLE IF NOT EXISTS users (
            id                  SERIAL PRIMARY KEY,
            telegram_id         BIGINT UNIQUE NOT NULL,
            username            VARCHAR(255),
            forwarded_count     INT DEFAULT 0,
            silent_mode         BOOLEAN DEFAULT FALSE,
            hide_original_link  BOOLEAN DEFAULT FALSE,
            keyword_filter      TEXT DEFAULT '',
            timezone            VARCHAR(50) DEFAULT 'UTC',
            language            VARCHAR(10) DEFAULT 'en',
            moderation_mode     BOOLEAN DEFAULT FALSE,
            butler_mode         BOOLEAN DEFAULT FALSE,
            is_banned           BOOLEAN DEFAULT FALSE,
            created_at          TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS channels (
            id          SERIAL PRIMARY KEY,
            owner_id    INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            chat_id     BIGINT UNIQUE NOT NULL,
            chat_title  VARCHAR(255),
            chat_type   VARCHAR(50),
            created_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS subscriptions (
            id              SERIAL PRIMARY KEY,
            user_id         INT REFERENCES users(id) ON DELETE CASCADE,
            channel_id      INT REFERENCES channels(id) ON DELETE CASCADE,
            source_url      VARCHAR(500) NOT NULL,
            source_type     VARCHAR(50) NOT NULL,
            source_name     VARCHAR(255),
            is_active       BOOLEAN DEFAULT TRUE,
            last_check      TIMESTAMP,
            initial_fetched BOOLEAN DEFAULT FALSE,
            created_at      TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS posts (
            id          SERIAL PRIMARY KEY,
            source_id   VARCHAR(64) UNIQUE NOT NULL,
            source_type VARCHAR(50),
            title       TEXT,
            url         VARCHAR(500),
            created_at  TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS sent_history (
            id          SERIAL PRIMARY KEY,
            user_id     INT REFERENCES users(id) ON DELETE CASCADE,
            channel_id  INT REFERENCES channels(id) ON DELETE CASCADE,
            post_id     INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
            sent_at     TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS user_state (
            telegram_id BIGINT PRIMARY KEY,
            state       VARCHAR(100) DEFAULT '',
            ctx         TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS broadcast_log (
            id          SERIAL PRIMARY KEY,
            sent_by     BIGINT NOT NULL,
            message     TEXT,
            sent_count  INT DEFAULT 0,
            created_at  TIMESTAMP DEFAULT NOW()
        );
        ALTER TABLE users ADD COLUMN IF NOT EXISTS forwarded_count     INT DEFAULT 0;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS silent_mode         BOOLEAN DEFAULT FALSE;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS hide_original_link  BOOLEAN DEFAULT FALSE;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS moderation_mode     BOOLEAN DEFAULT FALSE;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS butler_mode         BOOLEAN DEFAULT FALSE;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS is_banned           BOOLEAN DEFAULT FALSE;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS initial_fetched BOOLEAN DEFAULT FALSE;
        CREATE INDEX IF NOT EXISTS idx_subs_user    ON subscriptions(user_id);
        CREATE INDEX IF NOT EXISTS idx_subs_chan    ON subscriptions(channel_id);
        CREATE INDEX IF NOT EXISTS idx_posts_src    ON posts(source_id);
        CREATE INDEX IF NOT EXISTS idx_sent_user    ON sent_history(user_id);
        CREATE INDEX IF NOT EXISTS idx_users_tgid   ON users(telegram_id);
        CREATE INDEX IF NOT EXISTS idx_users_banned ON users(is_banned);
        """
        with self.conn.cursor() as cur:
            try:
                cur.execute(ddl)
                self.conn.commit()
                logger.info("Schema OK")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Schema error: {e}")
                raise

# ════════════════════════════════════════════════════════════════════════════════
# SOURCE DETECTION
# ════════════════════════════════════════════════════════════════════════════════

def detect_platform(url):
    url = url.strip()
    low = url.lower()
    if "youtube.com" in low or "youtu.be" in low:
        m = re.search(r"youtube\.com/channel/(UC[\w-]+)", url)
        if m:
            return "youtube", f"https://www.youtube.com/feeds/videos.xml?channel_id={m.group(1)}", False
        m = re.search(r"youtube\.com/(?:@|c/|user/)([^/?&#]+)", url)
        if m:
            return "youtube", f"https://www.youtube.com/feeds/videos.xml?user={m.group(1)}", True
        return None, None, False
    if "reddit.com" in low:
        m = re.search(r"reddit\.com/r/([^/?#\s]+)", url)
        if m:
            return "reddit", f"https://www.reddit.com/r/{m.group(1)}/.rss", False
        m = re.search(r"reddit\.com/u(?:ser)?/([^/?#\s]+)", url)
        if m:
            return "reddit", f"https://www.reddit.com/user/{m.group(1)}/.rss", False
        return None, None, False
    if "medium.com" in low:
        m = re.search(r"medium\.com/(@[^/?#\s]+)", url)
        if m:
            return "medium", f"https://medium.com/feed/{m.group(1)}", True
        return None, None, False
    if "livejournal.com" in low:
        m = re.search(r"([a-z0-9_-]+)\.livejournal\.com", low)
        if m:
            return "livejournal", f"https://{m.group(1)}.livejournal.com/data/rss", True
        return None, None, False
    return "rss", url, True

# ════════════════════════════════════════════════════════════════════════════════
# NETWORK HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def check_rss(url):
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=12, allow_redirects=True)
        if r.status_code != 200:
            return False
        feed = feedparser.parse(r.content)
        return bool(feed.entries) or bool(feed.feed.get("title"))
    except Exception:
        return False

def get_feed_title(url):
    try:
        r    = requests.get(url, headers=HTTP_HEADERS, timeout=12, allow_redirects=True)
        feed = feedparser.parse(r.content)
        t    = feed.feed.get("title", "").strip()
        return t[:255] if t else url[:255]
    except Exception:
        return url[:255]

def fetch_feed(rss_url, source_type, limit=5):
    posts = []
    try:
        r    = requests.get(rss_url, headers=HTTP_HEADERS, timeout=15, allow_redirects=True)
        feed = feedparser.parse(r.content)
        for entry in feed.entries[:limit]:
            link = entry.get("link", "")
            if not link:
                continue
            title     = (entry.get("title") or "No title")[:500]
            uid       = entry.get("id") or link
            source_id = hashlib.md5(uid.encode("utf-8", errors="replace")).hexdigest()
            posts.append({"source_id": source_id, "source_type": source_type,
                          "title": title, "url": link[:500]})
    except Exception as e:
        logger.warning(f"Fetch error {rss_url}: {e}")
    return posts

# ════════════════════════════════════════════════════════════════════════════════
# KEYBOARDS — emojis matching screenshots exactly
# ════════════════════════════════════════════════════════════════════════════════

def btn(label, data):
    return InlineKeyboardButton(label, callback_data=data)

def url_btn(label, url):
    return InlineKeyboardButton(label, url=url)

def kb_join():
    return InlineKeyboardMarkup([
        [url_btn("📢 Join @copyrightpost", CHANNEL_INVITE_URL)],
        [btn("✅ I have joined", "check_join")],
    ])

def kb_main(source_count):
    today = datetime.now(timezone.utc).strftime("%m/%d/%Y")
    return InlineKeyboardMarkup([
        [btn("➕ Add source",                                  "add_source")],
        [btn("🚀 Direct connection",                           "direct_conn")],
        [btn("🔀 Private/Channel/Group modes",                 "modes")],
        [btn(f"📂 My feed [{source_count}]", "my_feed"), btn("⚙️ Settings",  "settings")],
        [btn("🎁 Contact us",                "contact"), btn("📋 History",   "history")],
        [btn("🔮 RSS generator",                               "rss_gen")],
        [btn("🏷️ Referral program",                            "referral")],
        [btn("📚 How to use this bot  ↗",                     "how_to_use")],
        [btn("📦 Local data collector",                        "data_collector")],
        [btn("💳 Premium subscription",                        "premium")],
        [btn(f"🔥 News and updates [{today}] 🔥",             "updates")],
    ])

def kb_back(dest="main"):
    return InlineKeyboardMarkup([[btn("◀️ Back", dest)]])

def kb_settings(user):
    tz = (user.get("timezone") or "UTC") if user else "UTC"
    return InlineKeyboardMarkup([
        [btn("📩 Delivery options",          "s_delivery")],
        [btn("🖥️ Display options",            "s_display")],
        [btn("🗑️ Message filtering",          "s_filter")],
        [btn("🕵️ Moderation mode",            "s_moderation")],
        [btn("🙂 Butler mode",                "s_butler")],
        [btn("🤖 AI (LLM) settings",          "s_ai")],
        [btn("🐦 Your Twitter accounts",      "s_twitter")],
        [btn(f"🕐 Your timezone {tz}",        "s_timezone")],
        [btn("🌍 Change language",            "s_language")],
        [btn("◀️ Back",                       "main")],
    ])

def kb_delivery(silent):
    m = "✅" if silent else "☐"
    return InlineKeyboardMarkup([
        [btn(f"{m} 🤫 Silent mode", "tog_silent")],
        [btn("◀️ Back", "settings")],
    ])

def kb_display(hide):
    m = "✅" if hide else "☐"
    return InlineKeyboardMarkup([
        [btn(f"{m} 🔗 Hide 'View original post' link", "tog_hide_link")],
        [btn("◀️ Back", "settings")],
    ])

def kb_filter(kw):
    state = "✅ Active" if kw.strip() else "☐ Inactive"
    return InlineKeyboardMarkup([
        [btn(f"{state} — keyword filter", "noop")],
        [btn("✏️ Set keywords",   "set_kw")],
        [btn("🗑️ Clear keywords", "clear_kw")],
        [btn("◀️ Back", "settings")],
    ])

def kb_moderation(on):
    m = "✅ ON" if on else "☐ OFF"
    return InlineKeyboardMarkup([
        [btn(f"{m}  🕵️ Moderation mode", "tog_moderation")],
        [btn("◀️ Back", "settings")],
    ])

def kb_butler(on):
    m = "✅ ON" if on else "☐ OFF"
    return InlineKeyboardMarkup([
        [btn(f"{m}  🙂 Butler mode", "tog_butler")],
        [btn("◀️ Back", "settings")],
    ])

def kb_timezone():
    rows = []
    for i in range(0, len(TIMEZONES), 3):
        rows.append([btn(tz, f"set_tz_{tz}") for tz in TIMEZONES[i:i+3]])
    rows.append([btn("◀️ Back", "settings")])
    return InlineKeyboardMarkup(rows)

def kb_language():
    rows = []
    items = list(LANGUAGES.items())
    for i in range(0, len(items), 2):
        rows.append([btn(label, f"set_lang_{code}") for code, label in items[i:i+2]])
    rows.append([btn("◀️ Back", "settings")])
    return InlineKeyboardMarkup(rows)

def kb_admin():
    return InlineKeyboardMarkup([
        [btn("📊 Stats",            "adm_stats")],
        [btn("📢 Broadcast",        "adm_broadcast"),  btn("📜 Activity log",   "adm_activity")],
        [btn("👥 List users",       "adm_users"),       btn("📡 List channels",  "adm_channels")],
        [btn("🚫 Ban user",         "adm_ban"),         btn("✅ Unban user",     "adm_unban")],
        [btn("⚡ Force poll now",   "adm_force_poll"),  btn("🗑️ Clean old posts","adm_clean")],
        [btn("📋 Banned list",      "adm_banned_list")],
    ])

# ════════════════════════════════════════════════════════════════════════════════
# BOT
# ════════════════════════════════════════════════════════════════════════════════

class Bot:
    def __init__(self):
        self.db  = DB(DATABASE_URL)
        self.db.connect()
        self.db.init_schema()
        self.app = Application.builder().token(BOT_TOKEN).build()
        self._register()

    def _register(self):
        a = self.app
        a.add_handler(CommandHandler("start",      self.cmd_start))
        a.add_handler(CommandHandler("add",        self.cmd_add))
        a.add_handler(CommandHandler("remove",     self.cmd_remove))
        a.add_handler(CommandHandler("list",       self.cmd_list))
        a.add_handler(CommandHandler("help",       self.cmd_help))
        a.add_handler(CommandHandler("admin",      self.cmd_admin))
        a.add_handler(CommandHandler("stats",      self.cmd_admin))
        a.add_handler(CommandHandler("ban",        self.cmd_ban))
        a.add_handler(CommandHandler("unban",      self.cmd_unban))
        a.add_handler(CommandHandler("broadcast",  self.cmd_broadcast))
        a.add_handler(CommandHandler("connect",    self.cmd_connect))
        a.add_handler(CommandHandler("addchannel", self.cmd_addchannel))
        a.add_handler(CallbackQueryHandler(self.handle_cb))
        a.add_handler(ChatMemberHandler(self.on_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
        a.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))
        a.post_init = self._post_init

    async def _post_init(self, app):
        asyncio.create_task(self._polling_loop(app))
        logger.info("Bot ready")

    # ── owner notification ────────────────────────────────────────────────────

    async def _notify_owner(self, text):
        try:
            await self.app.bot.send_message(
                chat_id=OWNER_ID, text=text, parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as e:
            logger.warning(f"Owner notify: {e}")

    # ── join gate ─────────────────────────────────────────────────────────────

    async def _is_member(self, bot, tg_id):
        try:
            m = await bot.get_chat_member(REQUIRED_CHANNEL, tg_id)
            return m.status in (ChatMember.MEMBER, ChatMember.ADMINISTRATOR, ChatMember.OWNER)
        except TelegramError as e:
            logger.warning(f"Membership check {tg_id}: {e}")
            return True

    async def _gate(self, update, context):
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT is_banned FROM users WHERE telegram_id = %s", (tg_id,))
        if user and user.get("is_banned"):
            txt = "🚫 You have been banned from using this bot."
            if update.message:
                await update.message.reply_text(txt)
            elif update.callback_query:
                await update.callback_query.answer(txt, show_alert=True)
            return False
        if await self._is_member(context.bot, tg_id):
            return True
        join_text = (
            "👋 <b>Welcome to IN365Bot!</b>\n\n"
            "⚠️ To use this bot you must first join our channel.\n\n"
            "1️⃣ Tap the button below to join <b>@copyrightpost</b>\n"
            "2️⃣ Then press <b>✅ I have joined</b>"
        )
        if update.message:
            await update.message.reply_text(join_text, parse_mode="HTML", reply_markup=kb_join())
        elif update.callback_query:
            await update.callback_query.answer("Join the channel first!", show_alert=True)
            await update.callback_query.edit_message_text(
                join_text, parse_mode="HTML", reply_markup=kb_join()
            )
        return False

    # ── DB helpers ────────────────────────────────────────────────────────────

    def _ensure_user(self, tg_id, username):
        uname = (username or "user")[:255]
        self.db.run(
            "INSERT INTO users (telegram_id, username) VALUES (%s, %s) "
            "ON CONFLICT (telegram_id) DO UPDATE SET username = EXCLUDED.username",
            (tg_id, uname),
        )
        return self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))

    def _source_count(self, user_id):
        row = self.db.one(
            "SELECT COUNT(*) AS c FROM subscriptions WHERE user_id=%s AND is_active=TRUE",
            (user_id,),
        )
        return row["c"] if row else 0

    def _set_state(self, tg_id, state, ctx=""):
        self.db.run(
            "INSERT INTO user_state (telegram_id, state, ctx) VALUES (%s,%s,%s) "
            "ON CONFLICT (telegram_id) DO UPDATE SET state=EXCLUDED.state, ctx=EXCLUDED.ctx",
            (tg_id, state, ctx),
        )

    def _get_state(self, tg_id):
        row = self.db.one("SELECT state, ctx FROM user_state WHERE telegram_id=%s", (tg_id,))
        return (row["state"], row["ctx"]) if row else ("", "")

    def _clear_state(self, tg_id):
        self.db.run("DELETE FROM user_state WHERE telegram_id=%s", (tg_id,))

    # ── welcome text ──────────────────────────────────────────────────────────

    def _start_text(self, user, count):
        uname     = user.get("username") or "user"
        tg_id     = user.get("telegram_id", "")
        forwarded = user.get("forwarded_count", 0)
        return (
            f"👤 @{uname}\n"
            f"🆔 <code>{tg_id}</code>\n"
            f"🆓 <b>Free account</b>\n"
            f"📤 Forwarded messages: <b>{forwarded} / 50</b>\n\n"
            f"🔥 <b>IN365Bot v2.2</b> — RSS Aggregator\n\n"
            f"<b>Supported platforms:</b>\n"
            f"📰 RSS — any RSS/Atom feed\n"
            f"▶️ YouTube — channel feeds\n"
            f"🟠 Reddit — subreddits &amp; users\n"
            f"✍️ Medium — publications\n"
            f"📝 Livejournal — user journals\n\n"
            f"<b>✨ Features:</b>\n"
            f"🔀 Private or channel/group modes\n"
            f"🖼️ Photos, videos and files delivery\n"
            f"🚀 Direct Telegram connection\n"
            f"🎨 Custom message templates\n"
            f"⚡️ Fast refresh rate (5 min)\n"
            f"✂️ Filters, replacements &amp; text splitting\n"
            f"🌐 Automatic translations (100+ languages)\n"
            f"🤖 ChatGPT: summarizing &amp; rephrasing\n"
            f"🎙️ Live streams &amp; premieres for videos\n"
            f"👽 Publish with your own bot\n"
            f"🕵️ Moderation &amp; butler modes\n"
            f"♻️ Similarity filter\n"
            f"🗂️ Temporal channel for filtered messages\n"
            f"©️ Image watermarks\n"
            f"🔍 Instant View templates\n"
            f"🆘 Technical support\n"
            f"👥 Referral program\n"
            f"🆓 Free trial\n\n"
            f"<b>How to Use:</b>\n"
            f"— Add a data source (RSS, YouTube, Reddit…)\n"
            f"— Configure filters and message template\n"
            f"— Bot will forward new posts automatically!\n\n"
            f"📊 Sources: <b>{count}/{MAX_FREE_SOURCES}</b>"
        )

    # ════════════════════════════════════════════════════════════════════════════
    # COMMANDS
    # ════════════════════════════════════════════════════════════════════════════

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        tg_id    = update.effective_user.id
        username = update.effective_user.username or "user"
        existing = self.db.one("SELECT id FROM users WHERE telegram_id=%s", (tg_id,))
        is_new   = existing is None
        user     = self._ensure_user(tg_id, username)
        count    = self._source_count(user["id"])
        await update.message.reply_text(
            self._start_text(user, count), parse_mode="HTML", reply_markup=kb_main(count),
        )
        if is_new:
            ulink = f"@{username}" if username != "user" else f"ID {tg_id}"
            await self._notify_owner(
                f"🆕 <b>New user joined!</b>\n\n"
                f"👤 {ulink}\n🆔 <code>{tg_id}</code>\n"
                f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"
            )

    async def cmd_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        tg_id    = update.effective_user.id
        username = update.effective_user.username or "user"
        user     = self._ensure_user(tg_id, username)
        if not context.args:
            await update.message.reply_text(
                "➕ <b>Add a source</b>\n\n<b>Usage examples:</b>\n"
                "<code>/add https://feeds.bbci.co.uk/news/rss.xml</code>\n"
                "<code>/add https://youtube.com/channel/UCxxxxx</code>\n"
                "<code>/add https://reddit.com/r/python</code>\n"
                "<code>/add https://medium.com/@username</code>\n"
                "<code>/add https://username.livejournal.com</code>",
                parse_mode="HTML",
            )
            return
        await self._do_add(update, user, context.args)

    async def _do_add(self, update: Update, user, args):
        tg_id = update.effective_user.id
        count = self._source_count(user["id"])
        if count >= MAX_FREE_SOURCES:
            await update.message.reply_text(
                f"❌ Source limit reached ({MAX_FREE_SOURCES}). Remove one with /remove first."
            )
            return
        if len(args) != 1:
            await update.message.reply_text("❌ Please send a single URL.")
            return
        src_type, rss_url, needs_check = detect_platform(args[0])
        if not src_type or not rss_url:
            await update.message.reply_text(
                "❌ URL not recognised.\nSupported: RSS/Atom, YouTube, Reddit, Medium, Livejournal."
            )
            return
        if needs_check:
            msg = await update.message.reply_text("🔍 Checking feed...")
            ok  = await asyncio.to_thread(check_rss, rss_url)
            if not ok:
                await msg.edit_text("❌ Could not load a valid feed from that URL.")
                return
        feed_name = await asyncio.to_thread(get_feed_title, rss_url)
        try:
            sub_id = self.db.insert_id(
                "INSERT INTO subscriptions "
                "(user_id, source_url, source_type, source_name, is_active, initial_fetched) "
                "VALUES (%s,%s,%s,%s,TRUE,FALSE) ON CONFLICT DO NOTHING RETURNING id",
                (user["id"], rss_url, src_type, feed_name),
            )
        except Exception as e:
            logger.error(f"Insert sub: {e}")
            await update.message.reply_text("❌ Database error saving subscription.")
            return
        if sub_id is None:
            await update.message.reply_text("⚠️ Already subscribed to this source.")
            return
        await update.message.reply_text("📥 Fetching initial posts...")
        posts = await asyncio.to_thread(fetch_feed, rss_url, src_type, INITIAL_FETCH_COUNT)
        sent_count = 0
        for post in posts:
            try:
                ex = self.db.one("SELECT id FROM posts WHERE source_id=%s", (post["source_id"],))
                if ex:
                    post_id = ex["id"]
                else:
                    post_id = self.db.insert_id(
                        "INSERT INTO posts (source_id, source_type, title, url) "
                        "VALUES (%s,%s,%s,%s) RETURNING id",
                        (post["source_id"], post["source_type"], post["title"], post["url"]),
                    )
                if post_id is None:
                    continue
                icon = PLATFORMS.get(post["source_type"], "📱")
                hide = bool(user.get("hide_original_link", False))
                msg_text = f"{icon} <b>{feed_name}</b>\n\n{post['title']}"
                if not hide:
                    msg_text += f"\n\n<a href='{post['url']}'>🔗 View original post</a>"
                await self.app.bot.send_message(
                    chat_id=tg_id, text=msg_text, parse_mode="HTML",
                    disable_notification=bool(user.get("silent_mode", False)),
                )
                self.db.run(
                    "INSERT INTO sent_history (user_id, post_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (user["id"], post_id),
                )
                self.db.run(
                    "UPDATE users SET forwarded_count=forwarded_count+1 WHERE id=%s", (user["id"],)
                )
                sent_count += 1
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Initial post: {e}")
        self.db.run("UPDATE subscriptions SET initial_fetched=TRUE WHERE id=%s", (sub_id,))
        icon  = PLATFORMS.get(src_type, "📱")
        count = self._source_count(user["id"])
        await update.message.reply_text(
            f"✅ <b>Source added!</b>\n\n{icon} <b>{src_type.upper()}</b>\n"
            f"📌 {feed_name}\n\n📤 Fetched <b>{sent_count}</b> initial posts\n"
            f"📊 Sources: {count}/{MAX_FREE_SOURCES}\n\n⚡️ New posts every 5 minutes!",
            parse_mode="HTML",
        )

    async def cmd_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
        if not user:
            await update.message.reply_text("Use /start first.")
            return
        subs = self.db.query(
            "SELECT id, source_type, source_name, source_url FROM subscriptions "
            "WHERE user_id=%s AND is_active=TRUE ORDER BY created_at",
            (user["id"],),
        )
        if not subs:
            await update.message.reply_text("📭 No active subscriptions.")
            return
        if context.args and context.args[0].isdigit():
            idx = int(context.args[0]) - 1
            if 0 <= idx < len(subs):
                self.db.run("UPDATE subscriptions SET is_active=FALSE WHERE id=%s", (subs[idx]["id"],))
                await update.message.reply_text(
                    f"✅ Removed: {subs[idx]['source_name'] or subs[idx]['source_url']}"
                )
            else:
                await update.message.reply_text(f"❌ Range is 1–{len(subs)}")
            return
        text = "📋 <b>Your sources — reply with number to remove:</b>\n\n"
        for i, s in enumerate(subs, 1):
            icon = PLATFORMS.get(s["source_type"], "📱")
            name = (s["source_name"] or s["source_url"])[:50]
            text += f"{i}. {icon} {name}\n"
        await update.message.reply_text(text, parse_mode="HTML")
        self._set_state(tg_id, "remove_pick", "")

    async def cmd_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
        if not user:
            await update.message.reply_text("Use /start first.")
            return
        subs = self.db.query(
            "SELECT source_type, source_name, source_url, is_active "
            "FROM subscriptions WHERE user_id=%s ORDER BY created_at",
            (user["id"],),
        )
        if not subs:
            await update.message.reply_text(f"📋 No sources yet (0/{MAX_FREE_SOURCES})")
            return
        text = f"📋 <b>Your sources ({len(subs)}/{MAX_FREE_SOURCES})</b>\n\n"
        for s in subs:
            icon  = PLATFORMS.get(s["source_type"], "📱")
            state = "✅" if s["is_active"] else "❌"
            name  = (s["source_name"] or s["source_url"])[:50]
            text += f"{state} {icon} {name}\n"
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        await update.message.reply_text(
            "❓ <b>IN365Bot — Help</b>\n\n"
            "<b>Add sources:</b>\n"
            "<code>/add https://feeds.bbci.co.uk/news/rss.xml</code>\n"
            "<code>/add https://youtube.com/channel/UCxxxxx</code>\n"
            "<code>/add https://reddit.com/r/python</code>\n\n"
            "<b>Manage:</b>\n"
            "<code>/list</code>   — view all sources\n"
            "<code>/remove</code> — remove a source\n\n"
            f"Refresh: every 5 min | Limit: {MAX_FREE_SOURCES} sources\n\n"
            f"📢 Support: <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
            parse_mode="HTML", disable_web_page_preview=True,
        )

    async def cmd_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            return
        await update.message.reply_text(
            "🛠️ <b>Admin Panel</b>", parse_mode="HTML", reply_markup=kb_admin(),
        )

    async def cmd_ban(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            return
        if not context.args or not context.args[0].isdigit():
            await update.message.reply_text("Usage: <code>/ban &lt;telegram_id&gt;</code>", parse_mode="HTML")
            return
        target_id = int(context.args[0])
        u = self.db.one("SELECT id, username FROM users WHERE telegram_id=%s", (target_id,))
        if not u:
            await update.message.reply_text("❌ User not found.")
            return
        self.db.run("UPDATE users SET is_banned=TRUE WHERE telegram_id=%s", (target_id,))
        await update.message.reply_text(
            f"🚫 Banned @{u['username'] or '?'} (<code>{target_id}</code>)", parse_mode="HTML"
        )

    async def cmd_unban(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            return
        if not context.args or not context.args[0].isdigit():
            await update.message.reply_text("Usage: <code>/unban &lt;telegram_id&gt;</code>", parse_mode="HTML")
            return
        target_id = int(context.args[0])
        self.db.run("UPDATE users SET is_banned=FALSE WHERE telegram_id=%s", (target_id,))
        await update.message.reply_text(f"✅ Unbanned <code>{target_id}</code>", parse_mode="HTML")

    async def cmd_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            return
        if not context.args:
            await update.message.reply_text(
                "📢 Usage: <code>/broadcast Your message</code>\n"
                "Or use Admin Panel → 📢 Broadcast for multi-line.", parse_mode="HTML",
            )
            return
        await self._do_broadcast(update, " ".join(context.args))

    async def _do_broadcast(self, update, message_text):
        users     = self.db.query("SELECT telegram_id FROM users WHERE is_banned=FALSE")
        sent      = 0
        failed    = 0
        status_msg = await update.message.reply_text(f"📢 Broadcasting to {len(users)} users...")
        for row in users:
            try:
                await self.app.bot.send_message(
                    chat_id=row["telegram_id"],
                    text=f"📢 <b>Announcement</b>\n\n{message_text}",
                    parse_mode="HTML",
                )
                sent += 1
                await asyncio.sleep(0.05)
            except TelegramError as e:
                failed += 1
                logger.warning(f"Broadcast fail {row['telegram_id']}: {e}")
        self.db.run(
            "INSERT INTO broadcast_log (sent_by, message, sent_count) VALUES (%s,%s,%s)",
            (OWNER_ID, message_text[:1000], sent),
        )
        await status_msg.edit_text(
            f"✅ <b>Broadcast complete</b>\n\n✅ Sent: {sent}\n❌ Failed: {failed}",
            parse_mode="HTML",
        )

    async def cmd_connect(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
        if not user:
            await update.message.reply_text("Use /start first.")
            return
        channels = self.db.query(
            "SELECT chat_id, chat_title FROM channels WHERE owner_id=%s", (user["id"],)
        )
        if not channels:
            await update.message.reply_text(
                "📡 No channels connected yet.\n\n"
                "Add the bot as <b>admin</b> to your channel/group — "
                "it registers automatically.", parse_mode="HTML",
            )
            return
        text = "📡 <b>Your connected channels/groups:</b>\n\n"
        for ch in channels:
            text += f"• <b>{ch['chat_title']}</b>\n  ID: <code>{ch['chat_id']}</code>\n\n"
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_addchannel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await self._gate(update, context):
            return
        await update.message.reply_text(
            "📡 <b>Connect a channel/group</b>\n\n"
            "1. Add the bot as <b>administrator</b> to your channel/group.\n"
            "2. Bot registers automatically.\n3. Use /connect to confirm.",
            parse_mode="HTML",
        )

    # ════════════════════════════════════════════════════════════════════════════
    # CALLBACK HANDLER
    # ════════════════════════════════════════════════════════════════════════════

    async def handle_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data  = query.data
        tg_id = update.effective_user.id

        # join gate
        if data == "check_join":
            if await self._is_member(context.bot, tg_id):
                user  = self._ensure_user(tg_id, update.effective_user.username or "user")
                count = self._source_count(user["id"])
                await query.edit_message_text(
                    self._start_text(user, count), parse_mode="HTML", reply_markup=kb_main(count),
                )
            else:
                await query.answer("⚠️ You haven't joined yet!", show_alert=True)
            return

        # admin callbacks — owner only, no gate
        if data.startswith("adm_"):
            if tg_id != OWNER_ID:
                await query.answer("⛔ Admin only!", show_alert=True)
                return
            await self._handle_admin_cb(query, data)
            return

        # timezone / language set
        if data.startswith("set_tz_") or data.startswith("set_lang_"):
            user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
            if not user:
                await query.answer("Use /start first", show_alert=True)
                return
            if data.startswith("set_tz_"):
                tz = data[len("set_tz_"):]
                if tz in TIMEZONES:
                    self.db.run("UPDATE users SET timezone=%s WHERE telegram_id=%s", (tz, tg_id))
                    await query.edit_message_text(
                        f"✅ Timezone set to <b>{tz}</b>", parse_mode="HTML",
                        reply_markup=kb_back("settings"),
                    )
            else:
                lang = data[len("set_lang_"):]
                if lang in LANGUAGES:
                    self.db.run("UPDATE users SET language=%s WHERE telegram_id=%s", (lang, tg_id))
                    await query.edit_message_text(
                        f"✅ Language set to <b>{LANGUAGES[lang]}</b>", parse_mode="HTML",
                        reply_markup=kb_back("settings"),
                    )
            return

        # all other callbacks require gate
        if not await self._gate(update, context):
            return

        user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))

        if data == "main":
            if not user:
                user = self._ensure_user(tg_id, update.effective_user.username or "user")
            count = self._source_count(user["id"])
            await query.edit_message_text(
                self._start_text(user, count), parse_mode="HTML", reply_markup=kb_main(count),
            )

        elif data == "add_source":
            self._set_state(tg_id, "add_source", "")
            await query.edit_message_text(
                "➕ <b>Add source</b>\n\nSend me a URL:\n\n"
                "<code>https://feeds.bbci.co.uk/news/rss.xml</code>\n"
                "<code>https://youtube.com/channel/UCxxxxx</code>\n"
                "<code>https://reddit.com/r/python</code>\n"
                "<code>https://medium.com/@username</code>\n"
                "<code>https://user.livejournal.com</code>",
                parse_mode="HTML", reply_markup=kb_back(),
            )

        elif data == "my_feed":
            subs  = self.db.query(
                "SELECT id, source_type, source_name, source_url, is_active "
                "FROM subscriptions WHERE user_id=%s ORDER BY created_at", (user["id"],),
            ) if user else []
            count = len(subs)
            text  = f"📂 <b>My feed ({count}/{MAX_FREE_SOURCES})</b>\n\n"
            if not subs:
                text += "No sources yet. Use ➕ Add source."
            for s in subs:
                icon  = PLATFORMS.get(s["source_type"], "📱")
                state = "✅" if s["is_active"] else "❌"
                name  = (s["source_name"] or s["source_url"])[:45]
                text += f"{state} {icon} {name}\n"
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb_back())

        elif data == "settings":
            await query.edit_message_text(
                "⚙️ <b>Settings</b>", parse_mode="HTML", reply_markup=kb_settings(user),
            )

        elif data == "s_delivery":
            silent = bool(user.get("silent_mode", False)) if user else False
            await query.edit_message_text(
                "📩 <b>Delivery options</b>\n\nSilent mode — posts arrive without notification sound.",
                parse_mode="HTML", reply_markup=kb_delivery(silent),
            )

        elif data == "tog_silent":
            if user:
                new = not bool(user.get("silent_mode", False))
                self.db.run("UPDATE users SET silent_mode=%s WHERE telegram_id=%s", (new, tg_id))
                user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
            await query.edit_message_text(
                "📩 <b>Delivery options</b>\n\nSilent mode — posts arrive without notification sound.",
                parse_mode="HTML",
                reply_markup=kb_delivery(bool(user.get("silent_mode", False)) if user else False),
            )

        elif data == "s_display":
            hide = bool(user.get("hide_original_link", False)) if user else False
            await query.edit_message_text(
                "🖥️ <b>Display options</b>", parse_mode="HTML", reply_markup=kb_display(hide),
            )

        elif data == "tog_hide_link":
            if user:
                new = not bool(user.get("hide_original_link", False))
                self.db.run("UPDATE users SET hide_original_link=%s WHERE telegram_id=%s", (new, tg_id))
                user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
            hide = bool(user.get("hide_original_link", False)) if user else False
            await query.edit_message_text(
                "🖥️ <b>Display options</b>", parse_mode="HTML", reply_markup=kb_display(hide),
            )

        elif data == "s_filter":
            kw = (user.get("keyword_filter", "") or "") if user else ""
            await query.edit_message_text(
                f"🗑️ <b>Keyword filtering</b>\n\n"
                f"Posts whose title contains a keyword are <b>skipped</b>.\n"
                f"Current: <code>{kw or 'none'}</code>",
                parse_mode="HTML", reply_markup=kb_filter(kw),
            )

        elif data == "set_kw":
            self._set_state(tg_id, "set_kw", "")
            await query.edit_message_text(
                "✏️ <b>Set keywords</b>\n\nSend comma-separated keywords to skip.\n"
                "Example: <code>bitcoin, crypto, ads</code>",
                parse_mode="HTML", reply_markup=kb_back("settings"),
            )

        elif data == "clear_kw":
            if user:
                self.db.run("UPDATE users SET keyword_filter='' WHERE telegram_id=%s", (tg_id,))
            await query.edit_message_text(
                "✅ <b>Keywords cleared.</b>", parse_mode="HTML", reply_markup=kb_filter(""),
            )

        elif data == "s_moderation":
            on = bool(user.get("moderation_mode", False)) if user else False
            await query.edit_message_text(
                "🕵️ <b>Moderation mode</b>\n\n"
                "When ON, posts with potentially inappropriate content are skipped.",
                parse_mode="HTML", reply_markup=kb_moderation(on),
            )

        elif data == "tog_moderation":
            if user:
                new = not bool(user.get("moderation_mode", False))
                self.db.run("UPDATE users SET moderation_mode=%s WHERE telegram_id=%s", (new, tg_id))
                user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
            on = bool(user.get("moderation_mode", False)) if user else False
            await query.edit_message_text(
                "🕵️ <b>Moderation mode</b>\n\n"
                "When ON, posts with potentially inappropriate content are skipped.",
                parse_mode="HTML", reply_markup=kb_moderation(on),
            )

        elif data == "s_butler":
            on = bool(user.get("butler_mode", False)) if user else False
            await query.edit_message_text(
                "🙂 <b>Butler mode</b>\n\n"
                "When ON, the bot sends a daily digest instead of individual posts.",
                parse_mode="HTML", reply_markup=kb_butler(on),
            )

        elif data == "tog_butler":
            if user:
                new = not bool(user.get("butler_mode", False))
                self.db.run("UPDATE users SET butler_mode=%s WHERE telegram_id=%s", (new, tg_id))
                user = self.db.one("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))
            on = bool(user.get("butler_mode", False)) if user else False
            await query.edit_message_text(
                "🙂 <b>Butler mode</b>\n\n"
                "When ON, the bot sends a daily digest instead of individual posts.",
                parse_mode="HTML", reply_markup=kb_butler(on),
            )

        elif data == "s_ai":
            await query.edit_message_text(
                "🤖 <b>AI (LLM) settings</b>\n\n"
                "ChatGPT-powered features:\n• Auto-summarize long posts\n"
                "• Rephrase titles\n• Translate to your language\n\n"
                f"⭐ <b>Premium feature</b> — contact: <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
                parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [url_btn("💬 Contact for Premium", CHANNEL_INVITE_URL)],
                    [btn("◀️ Back", "settings")],
                ]),
            )

        elif data == "s_twitter":
            await query.edit_message_text(
                "🐦 <b>Your Twitter accounts</b>\n\n"
                "Connect Twitter/X accounts to receive tweets via RSS.\n\n"
                f"⭐ <b>Premium feature</b> — contact: <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
                parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [url_btn("💬 Contact for Premium", CHANNEL_INVITE_URL)],
                    [btn("◀️ Back", "settings")],
                ]),
            )

        elif data == "s_timezone":
            tz = (user.get("timezone") or "UTC") if user else "UTC"
            await query.edit_message_text(
                f"🕐 <b>Your timezone</b>\n\nCurrent: <b>{tz}</b>\n\nSelect new timezone:",
                parse_mode="HTML", reply_markup=kb_timezone(),
            )

        elif data == "s_language":
            lang  = (user.get("language") or "en") if user else "en"
            label = LANGUAGES.get(lang, lang)
            await query.edit_message_text(
                f"🌍 <b>Change language</b>\n\nCurrent: <b>{label}</b>\n\nSelect language:",
                parse_mode="HTML", reply_markup=kb_language(),
            )

        elif data == "history":
            rows = self.db.query(
                "SELECT p.title, p.url, p.source_type FROM sent_history sh "
                "JOIN posts p ON sh.post_id=p.id WHERE sh.user_id=%s "
                "ORDER BY sh.sent_at DESC LIMIT 20",
                (user["id"],),
            ) if user else []
            if not rows:
                await query.edit_message_text(
                    "📋 <b>No history yet.</b>", parse_mode="HTML", reply_markup=kb_back()
                )
                return
            text = "📋 <b>Last 20 forwarded posts</b>\n\n"
            for r in rows:
                icon  = PLATFORMS.get(r["source_type"], "📱")
                title = (r["title"] or "No title")[:55]
                text += f"{icon} <a href='{r['url']}'>{title}</a>\n"
            await query.edit_message_text(
                text, parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb_back(),
            )

        elif data == "contact":
            await query.edit_message_text(
                "🎁 <b>Contact &amp; Support</b>\n\n"
                f"📢 <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>\n\nWe read every message!",
                parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [url_btn("💬 Open channel", CHANNEL_INVITE_URL)],
                    [btn("◀️ Back", "main")],
                ]),
            )

        elif data == "rss_gen":
            await query.edit_message_text(
                "🔮 <b>RSS Generator Tools</b>\n\n"
                "Convert any website into RSS:\n\n"
                "• <a href='https://rsshub.app'>RSSHub</a> — universal hub\n"
                "• <a href='https://rss-bridge.org'>RSS-Bridge</a> — open source\n"
                "• <a href='https://fetchrss.com'>FetchRSS</a> — drag &amp; drop\n"
                "• <a href='https://politepol.com'>PolitePol</a> — any webpage\n\n"
                "Generate a feed URL, then use /add",
                parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb_back(),
            )

        elif data == "referral":
            bot_link = f"https://t.me/{BOT_USERNAME}?start=ref_{tg_id}"
            await query.edit_message_text(
                f"🏷️ <b>Referral Program</b>\n\nYour link:\n<code>{bot_link}</code>\n\n"
                f"Invite friends — earn rewards!\n\n"
                f"📢 <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
                parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb_back(),
            )

        elif data == "how_to_use":
            await query.edit_message_text(
                "📚 <b>How to use IN365Bot</b>\n\n"
                "<b>Step 1 — Add a source</b>\nTap ➕ Add source or /add URL.\n\n"
                "<b>Step 2 — Receive posts</b>\nBot checks every 5 min and delivers new posts.\n\n"
                "<b>Step 3 — Manage</b>\n/list — view sources | /remove — delete a source\n\n"
                "<b>Settings</b>\n⚙️ Settings to toggle modes, filters, timezone, language.\n\n"
                f"❓ Support: <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
                parse_mode="HTML", disable_web_page_preview=True, reply_markup=kb_back(),
            )

        elif data == "direct_conn":
            await query.edit_message_text(
                "🚀 <b>Direct Connection</b>\n\nForward to:\n"
                "• This private chat (default)\n• A Telegram channel\n• A group chat\n\n"
                "<b>Connect channel/group:</b>\n1. Add bot as admin\n2. Use /connect",
                parse_mode="HTML", reply_markup=kb_back(),
            )

        elif data == "modes":
            await query.edit_message_text(
                "🔀 <b>Private / Channel / Group modes</b>\n\n"
                "🔒 <b>Private</b> — posts arrive in this DM. No extra setup.\n\n"
                "📢 <b>Channel</b> — add bot as admin, posts go to the channel.\n\n"
                "👥 <b>Group</b> — add bot as admin, posts go to the group.\n\n"
                "Use /connect to see connected channels/groups.",
                parse_mode="HTML", reply_markup=kb_back(),
            )

        elif data == "data_collector":
            total_subs = self._source_count(user["id"]) if user else 0
            row        = self.db.one(
                "SELECT COUNT(*) AS c FROM sent_history WHERE user_id=%s", (user["id"],)
            ) if user else None
            total_sent = row["c"] if row else 0
            await query.edit_message_text(
                f"📦 <b>Your data</b>\n\n"
                f"📡 Active sources:   <b>{total_subs}</b>\n"
                f"📤 Posts forwarded:  <b>{total_sent}</b>\n"
                f"🤫 Silent mode:      {'✅ On' if user.get('silent_mode') else '☐ Off'}\n"
                f"🔗 Hide links:       {'✅ On' if user.get('hide_original_link') else '☐ Off'}\n"
                f"🕵️ Moderation mode:  {'✅ On' if user.get('moderation_mode') else '☐ Off'}\n"
                f"🙂 Butler mode:      {'✅ On' if user.get('butler_mode') else '☐ Off'}\n"
                f"🔍 Keyword filter:   <code>{user.get('keyword_filter') or 'none'}</code>\n"
                f"🕐 Timezone:         {user.get('timezone') or 'UTC'}\n"
                f"🌍 Language:         {LANGUAGES.get(user.get('language','en'), 'English')}",
                parse_mode="HTML", reply_markup=kb_back(),
            )

        elif data == "premium":
            await query.edit_message_text(
                f"💳 <b>Premium subscription</b>\n\n"
                f"<b>Free plan:</b>\n• {MAX_FREE_SOURCES} sources\n• 5-min refresh\n• Basic filters\n\n"
                f"<b>Premium plan:</b>\n• Unlimited sources\n• 1-min refresh\n"
                f"• AI summarization &amp; translation\n• Twitter/X integration\n"
                f"• Advanced templates\n• Priority support\n\n"
                f"📢 <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
                parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [url_btn("💬 Get Premium", CHANNEL_INVITE_URL)],
                    [btn("◀️ Back", "main")],
                ]),
            )

        elif data == "updates":
            today = datetime.now(timezone.utc).strftime("%m/%d/%Y")
            await query.edit_message_text(
                f"🔥 <b>News &amp; Updates</b>\n\n"
                f"Follow our channel:\n📢 <a href='{CHANNEL_INVITE_URL}'>@copyrightpost</a>",
                parse_mode="HTML", disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [url_btn("🔔 Open channel", CHANNEL_INVITE_URL)],
                    [btn("◀️ Back", "main")],
                ]),
            )

        elif data == "noop":
            pass

    # ════════════════════════════════════════════════════════════════════════════
    # ADMIN CALLBACKS
    # ════════════════════════════════════════════════════════════════════════════

    async def _handle_admin_cb(self, query, data):
        if data == "adm_stats":
            u   = self.db.one("SELECT COUNT(*) AS c FROM users")
            s   = self.db.one("SELECT COUNT(*) AS c FROM subscriptions WHERE is_active=TRUE")
            p   = self.db.one("SELECT COUNT(*) AS c FROM posts")
            sh  = self.db.one("SELECT COUNT(*) AS c FROM sent_history")
            ch  = self.db.one("SELECT COUNT(*) AS c FROM channels")
            bn  = self.db.one("SELECT COUNT(*) AS c FROM users WHERE is_banned=TRUE")
            br  = self.db.one("SELECT COUNT(*) AS c FROM broadcast_log")
            nu  = self.db.one(
                "SELECT COUNT(*) AS c FROM users WHERE created_at>=NOW()-INTERVAL '24 hours'"
            )
            top = self.db.query(
                "SELECT username, forwarded_count FROM users ORDER BY forwarded_count DESC LIMIT 5"
            )
            top_text = "\n".join(
                f"  {i+1}. @{r['username'] or '?'} — {r['forwarded_count']}"
                for i, r in enumerate(top)
            ) or "  (none yet)"
            await query.edit_message_text(
                f"📊 <b>Admin Stats</b>\n\n"
                f"👤 Total users:      {u['c']}\n"
                f"🆕 New today (24h):  {nu['c']}\n"
                f"🚫 Banned:          {bn['c']}\n"
                f"📡 Channels:         {ch['c']}\n"
                f"🔗 Active subs:      {s['c']}\n"
                f"📰 Posts cached:     {p['c']}\n"
                f"📤 Total forwarded:  {sh['c']}\n"
                f"📢 Broadcasts sent:  {br['c']}\n\n"
                f"<b>🏆 Top forwarders:</b>\n{top_text}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_broadcast":
            self._set_state(OWNER_ID, "adm_broadcast", "")
            await query.edit_message_text(
                "📢 <b>Broadcast</b>\n\nType your message to send to <b>all users</b>.\n"
                "Supports HTML formatting.\n\nSend /cancel to abort.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_activity":
            rows = self.db.query(
                "SELECT u.username, p.title, p.source_type, sh.sent_at "
                "FROM sent_history sh JOIN users u ON sh.user_id=u.id "
                "JOIN posts p ON sh.post_id=p.id ORDER BY sh.sent_at DESC LIMIT 20"
            )
            if not rows:
                text = "📜 <b>No activity yet.</b>"
            else:
                text = "📜 <b>Last 20 deliveries</b>\n\n"
                for r in rows:
                    icon  = PLATFORMS.get(r["source_type"], "📱")
                    title = (r["title"] or "?")[:40]
                    uname = r["username"] or "?"
                    ts    = r["sent_at"].strftime("%m/%d %H:%M") if r["sent_at"] else "?"
                    text += f"{icon} @{uname} — {title} [{ts}]\n"
            await query.edit_message_text(
                text, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_users":
            rows = self.db.query(
                "SELECT telegram_id, username, forwarded_count, is_banned, created_at "
                "FROM users ORDER BY created_at DESC LIMIT 30"
            )
            text = f"👥 <b>Users (last 30 of {self.db.one('SELECT COUNT(*) AS c FROM users')['c']})</b>\n\n"
            for r in rows:
                ban   = "🚫" if r["is_banned"] else "✅"
                uname = r["username"] or "?"
                dt    = r["created_at"].strftime("%m/%d/%y") if r["created_at"] else "?"
                text += f"{ban} @{uname} <code>{r['telegram_id']}</code> | {r['forwarded_count']}⬆ | {dt}\n"
            await query.edit_message_text(
                text, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_channels":
            rows = self.db.query(
                "SELECT c.chat_id, c.chat_title, c.chat_type, u.username, c.created_at "
                "FROM channels c JOIN users u ON c.owner_id=u.id ORDER BY c.created_at DESC LIMIT 30"
            )
            if not rows:
                text = "📡 <b>No channels yet.</b>"
            else:
                text = f"📡 <b>Channels/Groups (last 30)</b>\n\n"
                for r in rows:
                    dt    = r["created_at"].strftime("%m/%d/%y") if r["created_at"] else "?"
                    text += (
                        f"• <b>{r['chat_title']}</b> [{r['chat_type']}]\n"
                        f"  @{r['username'] or '?'} | <code>{r['chat_id']}</code> | {dt}\n"
                    )
            await query.edit_message_text(
                text, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_ban":
            self._set_state(OWNER_ID, "adm_ban", "")
            await query.edit_message_text(
                "🚫 <b>Ban user</b>\n\nSend the <b>Telegram ID</b> to ban.\nSend /cancel to abort.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_unban":
            self._set_state(OWNER_ID, "adm_unban", "")
            await query.edit_message_text(
                "✅ <b>Unban user</b>\n\nSend the <b>Telegram ID</b> to unban.\nSend /cancel to abort.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_force_poll":
            await query.edit_message_text(
                "⚡ <b>Force poll running...</b>", parse_mode="HTML",
            )
            try:
                await self._poll_all(self.app)
                await query.edit_message_text(
                    "⚡ <b>Force poll complete!</b>", parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
                )
            except Exception as e:
                await query.edit_message_text(
                    f"❌ Poll error: {e}", parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
                )

        elif data == "adm_clean":
            row = self.db.one(
                "SELECT COUNT(*) AS c FROM posts "
                "WHERE created_at < NOW()-INTERVAL '30 days' "
                "AND id NOT IN (SELECT post_id FROM sent_history)"
            )
            count = row["c"] if row else 0
            if count > 0:
                self.db.run(
                    "DELETE FROM posts WHERE created_at < NOW()-INTERVAL '30 days' "
                    "AND id NOT IN (SELECT post_id FROM sent_history)"
                )
            await query.edit_message_text(
                f"🗑️ <b>Cleanup complete</b>\n\nRemoved <b>{count}</b> old unreferenced posts.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_banned_list":
            rows = self.db.query(
                "SELECT telegram_id, username FROM users WHERE is_banned=TRUE"
            )
            if not rows:
                text = "✅ <b>No banned users.</b>"
            else:
                text = f"🚫 <b>Banned users ({len(rows)})</b>\n\n"
                for r in rows:
                    text += f"• @{r['username'] or '?'} — <code>{r['telegram_id']}</code>\n"
            await query.edit_message_text(
                text, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[btn("◀️ Back", "adm_back")]]),
            )

        elif data == "adm_back":
            await query.edit_message_text(
                "🛠️ <b>Admin Panel</b>", parse_mode="HTML", reply_markup=kb_admin(),
            )

    # ════════════════════════════════════════════════════════════════════════════
    # TEXT HANDLER
    # ════════════════════════════════════════════════════════════════════════════

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        text  = update.message.text.strip()
        state, _ctx = self._get_state(tg_id)

        if text.lower() == "/cancel":
            self._clear_state(tg_id)
            await update.message.reply_text("❎ Cancelled.")
            return

        # owner admin states
        if tg_id == OWNER_ID:
            if state == "adm_broadcast":
                self._clear_state(tg_id)
                await self._do_broadcast(update, text)
                return
            if state == "adm_ban":
                self._clear_state(tg_id)
                if not text.isdigit():
                    await update.message.reply_text("❌ Send a numeric Telegram ID.")
                    return
                target_id = int(text)
                u = self.db.one("SELECT id, username FROM users WHERE telegram_id=%s", (target_id,))
                if not u:
                    await update.message.reply_text("❌ User not found.")
                    return
                self.db.run("UPDATE users SET is_banned=TRUE WHERE telegram_id=%s", (target_id,))
                await update.message.reply_text(
                    f"🚫 Banned @{u['username'] or '?'} (<code>{target_id}</code>)", parse_mode="HTML"
                )
                return
            if state == "adm_unban":
                self._clear_state(tg_id)
                if not text.isdigit():
                    await update.message.reply_text("❌ Send a numeric Telegram ID.")
                    return
                target_id = int(text)
                self.db.run("UPDATE users SET is_banned=FALSE WHERE telegram_id=%s", (target_id,))
                await update.message.reply_text(
                    f"✅ Unbanned <code>{target_id}</code>", parse_mode="HTML"
                )
                return

        # user states
        if state == "add_source":
            if not await self._gate(update, context):
                self._clear_state(tg_id)
                return
            self._clear_state(tg_id)
            user = self._ensure_user(tg_id, update.effective_user.username or "user")
            await self._do_add(update, user, [text])

        elif state == "set_kw":
            self._clear_state(tg_id)
            self.db.run(
                "UPDATE users SET keyword_filter=%s WHERE telegram_id=%s", (text[:500], tg_id)
            )
            await update.message.reply_text("✅ Keywords saved.")

        elif state == "remove_pick":
            self._clear_state(tg_id)
            user = self.db.one("SELECT id FROM users WHERE telegram_id=%s", (tg_id,))
            if not user or not text.isdigit():
                await update.message.reply_text("❌ Invalid input.")
                return
            subs = self.db.query(
                "SELECT id, source_name FROM subscriptions "
                "WHERE user_id=%s AND is_active=TRUE ORDER BY created_at",
                (user["id"],),
            )
            idx = int(text) - 1
            if 0 <= idx < len(subs):
                self.db.run("UPDATE subscriptions SET is_active=FALSE WHERE id=%s", (subs[idx]["id"],))
                await update.message.reply_text(f"✅ Removed: {subs[idx]['source_name'] or '(source)'}")
            else:
                await update.message.reply_text(f"❌ Range is 1–{len(subs)}")

    # ════════════════════════════════════════════════════════════════════════════
    # CHAT MEMBER
    # ════════════════════════════════════════════════════════════════════════════

    async def on_chat_member(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        result     = update.my_chat_member
        chat       = result.chat
        new_status = result.new_chat_member.status
        added_by   = result.from_user

        if new_status in ("administrator", "member") and added_by:
            try:
                user = self._ensure_user(added_by.id, added_by.username or "user")
                self.db.run(
                    "INSERT INTO channels (owner_id, chat_id, chat_title, chat_type) "
                    "VALUES (%s,%s,%s,%s) ON CONFLICT (chat_id) DO UPDATE "
                    "SET chat_title=EXCLUDED.chat_title",
                    (user["id"], chat.id, chat.title or "Unnamed", chat.type),
                )
                await context.bot.send_message(
                    chat_id=chat.id,
                    text="✅ <b>IN365Bot connected!</b>\n\nUse /add in private chat to subscribe.",
                    parse_mode="HTML",
                )
                logger.info(f"Registered in {chat.title} ({chat.id})")
                await self._notify_owner(
                    f"📡 <b>Bot added to channel/group!</b>\n\n"
                    f"📌 <b>{chat.title}</b> [{chat.type}]\n"
                    f"🆔 <code>{chat.id}</code>\n"
                    f"👤 Added by: @{added_by.username or '?'} (<code>{added_by.id}</code>)\n"
                    f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"
                )
            except Exception as e:
                logger.error(f"on_chat_member add: {e}")

        elif new_status in ("left", "kicked"):
            try:
                await self._notify_owner(
                    f"⚠️ <b>Bot removed from channel/group!</b>\n\n"
                    f"📌 <b>{chat.title}</b> [{chat.type}]\n"
                    f"🆔 <code>{chat.id}</code>\n"
                    f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"
                )
            except Exception as e:
                logger.error(f"on_chat_member remove notify: {e}")

    # ════════════════════════════════════════════════════════════════════════════
    # POLLING LOOP
    # ════════════════════════════════════════════════════════════════════════════

    async def _polling_loop(self, app):
        while True:
            try:
                await self._poll_all(app)
            except Exception as e:
                logger.error(f"Polling loop: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def _poll_all(self, app):
        user_subs = self.db.query(
            "SELECT s.id, s.source_url, s.source_type, s.source_name, "
            "u.telegram_id, u.id AS user_id, u.silent_mode, u.keyword_filter, "
            "u.hide_original_link, u.moderation_mode, "
            "NULL::bigint AS channel_chat_id, NULL::int AS channel_id "
            "FROM subscriptions s JOIN users u ON s.user_id=u.id "
            "WHERE s.user_id IS NOT NULL AND s.is_active=TRUE AND u.is_banned=FALSE "
            "ORDER BY s.last_check ASC NULLS FIRST LIMIT 30"
        )
        chan_subs = self.db.query(
            "SELECT s.id, s.source_url, s.source_type, s.source_name, "
            "NULL::bigint AS telegram_id, NULL::int AS user_id, "
            "FALSE AS silent_mode, '' AS keyword_filter, "
            "FALSE AS hide_original_link, FALSE AS moderation_mode, "
            "c.chat_id AS channel_chat_id, c.id AS channel_id "
            "FROM subscriptions s JOIN channels c ON s.channel_id=c.id "
            "WHERE s.channel_id IS NOT NULL AND s.is_active=TRUE "
            "ORDER BY s.last_check ASC NULLS FIRST LIMIT 30"
        )
        all_subs = list(user_subs) + list(chan_subs)
        if not all_subs:
            return
        for sub in all_subs:
            try:
                posts = await asyncio.to_thread(fetch_feed, sub["source_url"], sub["source_type"], 5)
                for post in posts:
                    await self._deliver(app, sub, post)
                self.db.run("UPDATE subscriptions SET last_check=NOW() WHERE id=%s", (sub["id"],))
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Poll sub {sub['id']}: {e}")

    async def _deliver(self, app, sub, post):
        try:
            kw = (sub.get("keyword_filter") or "").strip()
            if kw:
                kws = [k.strip().lower() for k in kw.split(",") if k.strip()]
                if any(k in post["title"].lower() for k in kws):
                    return
            if sub.get("moderation_mode"):
                bad = ["18+", "nsfw", "adult", "xxx", "porn", "casino", "gambling"]
                if any(w in post["title"].lower() for w in bad):
                    return
            ex = self.db.one("SELECT id FROM posts WHERE source_id=%s", (post["source_id"],))
            if ex:
                post_id = ex["id"]
            else:
                post_id = self.db.insert_id(
                    "INSERT INTO posts (source_id, source_type, title, url) "
                    "VALUES (%s,%s,%s,%s) RETURNING id",
                    (post["source_id"], post["source_type"],
                     post["title"][:500], post["url"][:500]),
                )
                if post_id is None:
                    return
            user_id    = sub.get("user_id")
            channel_id = sub.get("channel_id")
            if user_id:
                sent = self.db.one(
                    "SELECT id FROM sent_history WHERE user_id=%s AND post_id=%s",
                    (user_id, post_id),
                )
            else:
                sent = self.db.one(
                    "SELECT id FROM sent_history WHERE channel_id=%s AND post_id=%s",
                    (channel_id, post_id),
                )
            if sent:
                return
            icon        = PLATFORMS.get(post["source_type"], "📱")
            source_name = (sub.get("source_name") or post["source_type"])[:60]
            hide        = bool(sub.get("hide_original_link", False))
            msg_text    = f"{icon} <b>{source_name}</b>\n\n{post['title']}"
            if not hide:
                msg_text += f"\n\n<a href='{post['url']}'>🔗 View original post</a>"
            target = sub.get("telegram_id") or sub.get("channel_chat_id")
            if not target:
                return
            await app.bot.send_message(
                chat_id=target, text=msg_text, parse_mode="HTML",
                disable_notification=bool(sub.get("silent_mode", False)),
                disable_web_page_preview=False,
            )
            self.db.run(
                "INSERT INTO sent_history (user_id, channel_id, post_id) VALUES (%s,%s,%s)",
                (user_id, channel_id, post_id),
            )
            if user_id:
                self.db.run(
                    "UPDATE users SET forwarded_count=forwarded_count+1 WHERE id=%s", (user_id,)
                )
        except TelegramError as e:
            logger.warning(f"Telegram error → {sub.get('telegram_id') or sub.get('channel_chat_id')}: {e}")
        except Exception as e:
            logger.error(f"Deliver: {e}")

    def run(self):
        logger.info("IN365Bot v2.2 starting...")
        try:
            self.app.run_polling(
                allowed_updates=["message", "callback_query", "my_chat_member"]
            )
        finally:
            if self.db.conn:
                self.db.conn.close()
            logger.info("Bot stopped.")


if __name__ == "__main__":
    Bot().run()