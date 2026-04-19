"""
IN365Bot - RSS Aggregator Bot
Platforms: RSS, YouTube (official feed), Reddit (.rss), Medium (feed), Livejournal
All free, no API keys required. No fake logic.
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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ChatMemberHandler,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID     = int(os.getenv("OWNER_ID", "7232714487"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))   # seconds
BOT_USERNAME  = os.getenv("BOT_USERNAME", "in365b")
MAX_FREE_SOURCES = 10

if not BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN not set")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

# ── Supported platforms (only ones with real free RSS endpoints) ──────────────

PLATFORMS = {
    "rss":         "📰",
    "youtube":     "▶️",
    "reddit":      "🟠",
    "medium":      "✍️",
    "livejournal": "📝",
}

# ── Database ──────────────────────────────────────────────────────────────────

class DB:
    def __init__(self, url: str):
        self.url = url
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(self.url)
        self.conn.autocommit = False

    # ── Query helpers ──

    def query(self, sql: str, params=None) -> list:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                return cur.fetchall() or []
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB query: {e}\nSQL: {sql}")
                raise

    def one(self, sql: str, params=None):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                return cur.fetchone()
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB one: {e}")
                raise

    def run(self, sql: str, params=None):
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB run: {e}")
                raise

    def insert_id(self, sql: str, params=None):
        """INSERT ... RETURNING id → returns the id integer."""
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

    # ── Schema ──

    def init_schema(self):
        ddl = """
        CREATE TABLE IF NOT EXISTS users (
            id          SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            username    VARCHAR(255),
            silent_mode        BOOLEAN DEFAULT FALSE,
            hide_original_link BOOLEAN DEFAULT FALSE,
            keyword_filter     TEXT    DEFAULT '',
            timezone           VARCHAR(100) DEFAULT 'UTC',
            language           VARCHAR(10)  DEFAULT 'en',
            created_at  TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS channels (
            id         SERIAL PRIMARY KEY,
            owner_id   INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            chat_id    BIGINT UNIQUE NOT NULL,
            chat_title VARCHAR(255),
            chat_type  VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER REFERENCES users(id)    ON DELETE CASCADE,
            channel_id  INTEGER REFERENCES channels(id) ON DELETE CASCADE,
            source_url  VARCHAR(500) NOT NULL,
            source_type VARCHAR(50)  NOT NULL,
            source_name VARCHAR(255),
            is_active   BOOLEAN   DEFAULT TRUE,
            last_check  TIMESTAMP,
            created_at  TIMESTAMP DEFAULT NOW()
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
            id         SERIAL PRIMARY KEY,
            user_id    INTEGER REFERENCES users(id)    ON DELETE CASCADE,
            channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
            post_id    INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
            sent_at    TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS user_state (
            telegram_id BIGINT PRIMARY KEY,
            state       VARCHAR(50) DEFAULT '',
            ctx         TEXT        DEFAULT ''
        );

        -- Safe migrations
        ALTER TABLE users ADD COLUMN IF NOT EXISTS silent_mode        BOOLEAN DEFAULT FALSE;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS hide_original_link BOOLEAN DEFAULT FALSE;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS keyword_filter     TEXT    DEFAULT '';
        ALTER TABLE users ADD COLUMN IF NOT EXISTS timezone           VARCHAR(100) DEFAULT 'UTC';
        ALTER TABLE users ADD COLUMN IF NOT EXISTS language           VARCHAR(10)  DEFAULT 'en';
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

        CREATE INDEX IF NOT EXISTS idx_subs_user  ON subscriptions(user_id);
        CREATE INDEX IF NOT EXISTS idx_subs_chan  ON subscriptions(channel_id);
        CREATE INDEX IF NOT EXISTS idx_posts_src  ON posts(source_id);
        CREATE INDEX IF NOT EXISTS idx_sent_user  ON sent_history(user_id);
        CREATE INDEX IF NOT EXISTS idx_users_tgid ON users(telegram_id);
        """
        with self.conn.cursor() as cur:
            try:
                cur.execute(ddl)
                self.conn.commit()
                logger.info("Schema ready.")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Schema error: {e}")
                raise


# ── Source detection (URL-only, no network) ───────────────────────────────────

def _yt_url_to_rss(url: str):
    """Convert a YouTube URL to its RSS feed URL.
    Returns (rss_url, needs_validation) where needs_validation=True if
    we couldn't get a guaranteed channel_id from the URL.
    """
    # /channel/UCxxxxxx  ← most reliable
    m = re.search(r"youtube\.com/channel/(UC[\w-]+)", url)
    if m:
        return f"https://www.youtube.com/feeds/videos.xml?channel_id={m.group(1)}", False

    # /@handle, /c/name, /user/name  ← try user= param (works for legacy names)
    m = re.search(r"youtube\.com/(?:@|c/|user/)([^/?&#]+)", url)
    if m:
        handle = m.group(1)
        return f"https://www.youtube.com/feeds/videos.xml?user={handle}", True

    return None, False


def detect_platform(url: str):
    """
    Parse a URL and return (source_type, rss_url, needs_net_check).
    needs_net_check=True means we must validate the RSS URL over the network
    before accepting it.
    Returns (None, None, False) if unsupported.
    """
    url = url.strip()
    low = url.lower()

    if "youtube.com" in low or "youtu.be" in low:
        rss, needs = _yt_url_to_rss(url)
        if rss:
            return "youtube", rss, needs
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
        # medium.com/@user  or  medium.com/publication
        m = re.search(r"medium\.com/(@[^/?#\s]+)", url)
        if m:
            return "medium", f"https://medium.com/feed/{m.group(1)}", True
        m = re.search(r"medium\.com/([^/?#\s@][^/?#\s]+)", url)
        if m and "." not in m.group(1):
            return "medium", f"https://medium.com/feed/{m.group(1)}", True
        return None, None, False

    if "livejournal.com" in low:
        m = re.search(r"([a-z0-9_-]+)\.livejournal\.com", low)
        if m:
            return "livejournal", f"https://{m.group(1)}.livejournal.com/data/rss", True
        return None, None, False

    # Generic RSS / Atom URL
    return "rss", url, True


def resolve_shorthand(platform: str, name: str):
    """
    '/add youtube UCxxxxxx'  → ("youtube", rss_url, needs_check)
    '/add reddit python'     → ("reddit",  rss_url, False)
    etc.
    Returns (None, None, False) if platform unknown.
    """
    platform = platform.lower().strip()
    name = name.strip().lstrip("@").lstrip("/")

    if platform == "youtube":
        if name.upper().startswith("UC") and len(name) > 10:
            rss = f"https://www.youtube.com/feeds/videos.xml?channel_id={name}"
            return "youtube", rss, False
        else:
            rss = f"https://www.youtube.com/feeds/videos.xml?user={name}"
            return "youtube", rss, True

    if platform == "reddit":
        sub = name[2:] if name.lower().startswith("r/") else name
        return "reddit", f"https://www.reddit.com/r/{sub}/.rss", False

    if platform == "medium":
        if not name.startswith("@"):
            name = f"@{name}"
        return "medium", f"https://medium.com/feed/{name}", True

    if platform == "livejournal":
        return "livejournal", f"https://{name}.livejournal.com/data/rss", True

    if platform == "rss":
        return "rss", name, True

    return None, None, False


# ── Network helpers (sync — called via asyncio.to_thread) ────────────────────

_HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; IN365Bot/1.0; RSS reader)"}


def _check_rss(url: str) -> bool:
    """Return True if URL delivers a parsable RSS/Atom feed with at least one entry OR a feed title."""
    try:
        r = requests.get(url, headers=_HTTP_HEADERS, timeout=12, allow_redirects=True)
        if r.status_code != 200:
            return False
        feed = feedparser.parse(r.content)
        return bool(feed.entries) or bool(feed.feed.get("title"))
    except Exception:
        return False


def _get_feed_title(url: str) -> str:
    """Fetch RSS and return the feed title, or the URL if title not found."""
    try:
        r = requests.get(url, headers=_HTTP_HEADERS, timeout=12, allow_redirects=True)
        feed = feedparser.parse(r.content)
        title = feed.feed.get("title", "").strip()
        return title[:255] if title else url[:255]
    except Exception:
        return url[:255]


def _fetch_feed(rss_url: str, source_type: str, limit: int = 5) -> list:
    """Fetch up to `limit` posts from an RSS URL.  Returns list of dicts."""
    posts = []
    try:
        r = requests.get(rss_url, headers=_HTTP_HEADERS, timeout=15, allow_redirects=True)
        feed = feedparser.parse(r.content)
        for entry in feed.entries[:limit]:
            link = entry.get("link", "")
            if not link:
                continue
            title = (entry.get("title") or "No title")[:500]
            uid   = entry.get("id") or link
            # Stable 32-char unique ID
            source_id = hashlib.md5(uid.encode("utf-8", errors="replace")).hexdigest()
            posts.append({
                "source_id":   source_id,
                "source_type": source_type,
                "title":       title,
                "url":         link[:500],
            })
    except Exception as e:
        logger.warning(f"Fetch error {rss_url}: {e}")
    return posts


# ── Keyboards ─────────────────────────────────────────────────────────────────

def _btn(label: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(label, callback_data=data)


def _url_btn(label: str, url: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(label, url=url)


def kb_main(source_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [_btn("➕ Add source", "add_source"),
         _btn("🚀 Direct connection", "direct_conn")],
        [_btn("🔄 Working modes", "working_modes")],
        [_btn(f"📋 My feed [{source_count}/{MAX_FREE_SOURCES}]", "my_feed")],
        [_btn("⚙️ Settings", "settings"),
         _btn("📖 History", "history")],
        [_btn("📡 RSS generator", "rss_gen"),
         _btn("🎁 Referral", "referral")],
        [_btn("❓ How to use", "how_to_use"),
         _btn("⭐ Premium", "premium")],
        [_url_btn("💬 Contact us", "https://t.me/madbots_talk")],
    ])


def kb_back(dest: str = "main_menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[_btn("◀️ Back", dest)]])


def kb_settings() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [_btn("📨 Delivery options", "s_delivery")],
        [_btn("🖥 Display options",  "s_display")],
        [_btn("🗑 Message filtering", "s_filter")],
        [_btn("👮 Moderation mode",  "s_moderation")],
        [_btn("🤵 Butler mode",      "s_butler")],
        [_btn("🤖 AI (LLM) settings", "s_ai")],
        [_btn("🐦 Your Twitter accounts", "s_twitter")],
        [_btn("🕐 Your timezone",    "s_timezone")],
        [_btn("🌍 Change language",  "s_language")],
        [_btn("◀️ Back", "main_menu")],
    ])


def kb_my_feed(subs: list) -> InlineKeyboardMarkup:
    rows = []
    for s in subs:
        icon  = PLATFORMS.get(s["source_type"], "📱")
        state = "✅" if s["is_active"] else "❌"
        name  = (s["source_name"] or s["source_url"])[:35]
        rows.append([_btn(f"{state} {icon} {name}", f"tog_{s['id']}")])
    rows.append([
        _btn("❌ Turn off all", "subs_off"),
        _btn("✅ Turn on all",  "subs_on"),
    ])
    rows.append([_btn("◀️ Back", "main_menu")])
    return InlineKeyboardMarkup(rows)


def kb_delivery(silent: bool) -> InlineKeyboardMarkup:
    mark = "✅" if silent else "[ ]"
    return InlineKeyboardMarkup([
        [_btn(f"{mark} 🤫 Silent mode", "tog_silent")],
        [_btn("◀️ Back", "settings")],
    ])


def kb_display(hide: bool) -> InlineKeyboardMarkup:
    mark = "✅" if hide else "[ ]"
    return InlineKeyboardMarkup([
        [_btn(f"{mark} 🔗 Hide 'View original post' link", "tog_hide_link")],
        [_btn("◀️ Back", "settings")],
    ])


def kb_filter(kw: str) -> InlineKeyboardMarkup:
    has = bool(kw.strip())
    state = "✅ Active" if has else "[ ] Inactive"
    return InlineKeyboardMarkup([
        [_btn(f"{state} keyword filter", "noop")],
        [_btn("✏️ Set keywords", "set_kw")],
        [_btn("🗑 Clear keywords", "clear_kw")],
        [_btn("◀️ Back", "settings")],
    ])


def kb_timezone() -> InlineKeyboardMarkup:
    regions = [
        "Africa", "America", "America/Argentina", "America/Indiana",
        "Antarctica", "Arctic", "Asia", "Atlantic",
        "Australia", "Europe", "Indian", "Pacific",
    ]
    rows = []
    for i in range(0, len(regions), 2):
        row = [_btn(regions[i], f"tz_{regions[i]}")]
        if i + 1 < len(regions):
            row.append(_btn(regions[i + 1], f"tz_{regions[i + 1]}"))
        rows.append(row)
    rows.append([_btn("◀️ Back", "settings")])
    return InlineKeyboardMarkup(rows)


def kb_language() -> InlineKeyboardMarkup:
    langs = [
        ("🇬🇧 English", "en"), ("🇷🇺 Русский", "ru"),
        ("🇪🇸 Español", "es"), ("🇵🇹 Português", "pt"),
        ("🇮🇹 Italiano", "it"), ("🇫🇷 Française", "fr"),
        ("🇩🇪 Deutsch",  "de"), ("🇵🇱 Polski",    "pl"),
    ]
    rows = []
    for i in range(0, len(langs), 2):
        row = [_btn(langs[i][0], f"lang_{langs[i][1]}")]
        if i + 1 < len(langs):
            row.append(_btn(langs[i + 1][0], f"lang_{langs[i + 1][1]}"))
        rows.append(row)
    rows.append([_btn("◀️ Back", "settings")])
    return InlineKeyboardMarkup(rows)


# ── Bot class ─────────────────────────────────────────────────────────────────

class Bot:
    def __init__(self):
        self.db = DB(DATABASE_URL)
        self.db.connect()
        self.db.init_schema()
        self.app = Application.builder().token(BOT_TOKEN).build()
        self._register()

    # ── Handler registration ──

    def _register(self):
        a = self.app
        a.add_handler(CommandHandler("start",      self.cmd_start))
        a.add_handler(CommandHandler("add",        self.cmd_add))
        a.add_handler(CommandHandler("remove",     self.cmd_remove))
        a.add_handler(CommandHandler("list",       self.cmd_list))
        a.add_handler(CommandHandler("history",    self.cmd_history))
        a.add_handler(CommandHandler("connect",    self.cmd_connect))
        a.add_handler(CommandHandler("channels",   self.cmd_connect))
        a.add_handler(CommandHandler("addchannel", self.cmd_addchannel))
        a.add_handler(CommandHandler("user",       self.cmd_user_mode))
        a.add_handler(CommandHandler("chat",       self.cmd_chat_mode))
        a.add_handler(CommandHandler("help",       self.cmd_help))
        a.add_handler(CommandHandler("admin",      self.cmd_admin))
        a.add_handler(CallbackQueryHandler(self.handle_cb))
        a.add_handler(ChatMemberHandler(self.on_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
        a.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))
        a.post_init = self._post_init

    async def _post_init(self, app):
        asyncio.create_task(self._polling_loop(app))
        logger.info("Polling loop started.")

    # ── DB helpers ──

    def _ensure_user(self, tg_id: int, username: str) -> dict:
        uname = (username or "user")[:255]
        self.db.run(
            "INSERT INTO users (telegram_id, username) VALUES (%s, %s) "
            "ON CONFLICT (telegram_id) DO UPDATE SET username = EXCLUDED.username",
            (tg_id, uname),
        )
        return self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))

    def _source_count(self, user_id: int) -> int:
        row = self.db.one(
            "SELECT COUNT(*) AS c FROM subscriptions WHERE user_id = %s", (user_id,)
        )
        return row["c"] if row else 0

    def _set_state(self, tg_id: int, state: str, ctx: str = ""):
        self.db.run(
            "INSERT INTO user_state (telegram_id, state, ctx) VALUES (%s, %s, %s) "
            "ON CONFLICT (telegram_id) DO UPDATE SET state = EXCLUDED.state, ctx = EXCLUDED.ctx",
            (tg_id, state, ctx),
        )

    def _get_state(self, tg_id: int) -> tuple:
        row = self.db.one(
            "SELECT state, ctx FROM user_state WHERE telegram_id = %s", (tg_id,)
        )
        return (row["state"], row["ctx"]) if row else ("", "")

    def _clear_state(self, tg_id: int):
        self.db.run("DELETE FROM user_state WHERE telegram_id = %s", (tg_id,))

    # ── Start message text ──

    def _start_text(self, user: dict, count: int) -> str:
        uname = user.get("username") or "user"
        tg_id = user.get("telegram_id", "")
        return (
            f"👤 @{uname}\n"
            f"ID: <code>{tg_id}</code>\n"
            f"<b>Free account</b>  —  Sources: <b>{count}/{MAX_FREE_SOURCES}</b>\n\n"
            f"📨 <b>IN365Bot</b> — RSS aggregator from social networks and feeds to Telegram.\n\n"
            f"<b>Supported platforms:</b>\n"
            f"📰 RSS — any RSS/Atom feed URL\n"
            f"▶️ YouTube — channel feeds (channel_id or legacy username)\n"
            f"🟠 Reddit — subreddits and user feeds\n"
            f"✍️ Medium — publication and user feeds\n"
            f"📝 Livejournal — user journals\n\n"
            f"<b>How to use:</b>\n"
            f"— Add a source → bot checks every 5 min → new posts arrive here\n"
            f"— Use the menu below to manage everything"
        )

    # ── /start ──

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id    = update.effective_user.id
        username = update.effective_user.username or "user"
        user     = self._ensure_user(tg_id, username)
        count    = self._source_count(user["id"])
        await update.message.reply_text(
            self._start_text(user, count),
            parse_mode="HTML",
            reply_markup=kb_main(count),
        )

    # ── /add ──

    async def cmd_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id    = update.effective_user.id
        username = update.effective_user.username or "user"
        user     = self._ensure_user(tg_id, username)

        if not context.args:
            await update.message.reply_text(
                "Usage:\n"
                "<code>/add https://feeds.bbci.co.uk/news/rss.xml</code>\n"
                "<code>/add youtube UC_CHANNEL_ID</code>\n"
                "<code>/add reddit python</code>\n"
                "<code>/add medium @username</code>\n"
                "<code>/add livejournal username</code>",
                parse_mode="HTML",
            )
            return

        await self._do_add(update, user, context.args)

    async def _do_add(self, update: Update, user: dict, args: list):
        """Core logic for adding a subscription."""
        tg_id = update.effective_user.id

        # Enforce limit
        count = self._source_count(user["id"])
        if count >= MAX_FREE_SOURCES:
            await update.message.reply_text(
                f"❌ You have reached the limit of {MAX_FREE_SOURCES} free sources.\n"
                "Remove one first with /remove"
            )
            return

        # Resolve to (type, rss_url, needs_check)
        if len(args) == 1:
            src_type, rss_url, needs_check = detect_platform(args[0])
        elif len(args) >= 2:
            platform = args[0].lower()
            name     = args[1]
            if platform not in ("rss", "youtube", "reddit", "medium", "livejournal"):
                await update.message.reply_text(
                    f"❌ Unknown platform '{platform}'.\n"
                    "Supported: rss, youtube, reddit, medium, livejournal"
                )
                return
            src_type, rss_url, needs_check = resolve_shorthand(platform, name)
        else:
            return

        if not src_type or not rss_url:
            await update.message.reply_text(
                "❌ Could not recognise this source.\n\n"
                "Supported:\n"
                "• Any RSS/Atom URL\n"
                "• YouTube: use channel_id (UCxxxxxxxx) or legacy username\n"
                "• Reddit: subreddit name\n"
                "• Medium: @username\n"
                "• Livejournal: username"
            )
            return

        # Network validation when required
        if needs_check:
            await update.message.reply_text("🔍 Checking source, please wait…")
            ok = await asyncio.to_thread(_check_rss, rss_url)
            if not ok:
                await update.message.reply_text(
                    "❌ Could not fetch a valid feed from this source.\n\n"
                    "Check:\n"
                    "• The URL is correct and public\n"
                    "• For YouTube @handles, use the channel_id (UCxxxxxxxxx) instead:\n"
                    "  <code>/add youtube UCxxxxxxxxx</code>",
                    parse_mode="HTML",
                )
                return

        # Fetch feed title
        feed_name = await asyncio.to_thread(_get_feed_title, rss_url)

        # Insert
        try:
            self.db.run(
                "INSERT INTO subscriptions "
                "(user_id, source_url, source_type, source_name, is_active) "
                "VALUES (%s, %s, %s, %s, TRUE)",
                (user["id"], rss_url, src_type, feed_name),
            )
        except Exception as e:
            if "unique" in str(e).lower() or "duplicate" in str(e).lower():
                await update.message.reply_text("⚠️ You are already subscribed to this source.")
            else:
                logger.error(f"Insert sub: {e}")
                await update.message.reply_text("❌ Database error. Please try again.")
            return

        icon  = PLATFORMS.get(src_type, "📱")
        count = self._source_count(user["id"])
        await update.message.reply_text(
            f"✅ <b>Source added!</b>\n\n"
            f"{icon} <b>{src_type.upper()}</b>\n"
            f"📌 {feed_name}\n\n"
            f"You now have {count}/{MAX_FREE_SOURCES} sources.\n"
            f"New posts will be delivered automatically every 5 minutes.",
            parse_mode="HTML",
        )

    # ── /remove ──

    async def cmd_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))
        if not user:
            await update.message.reply_text("Use /start first.")
            return

        subs = self.db.query(
            "SELECT id, source_type, source_name, source_url "
            "FROM subscriptions WHERE user_id = %s ORDER BY created_at",
            (user["id"],),
        )

        if not subs:
            await update.message.reply_text("You have no subscriptions to remove.")
            return

        # /remove <number>
        if context.args:
            arg = context.args[0]
            if arg.isdigit():
                idx = int(arg) - 1
                if 0 <= idx < len(subs):
                    self.db.run("DELETE FROM subscriptions WHERE id = %s", (subs[idx]["id"],))
                    name = subs[idx]["source_name"] or subs[idx]["source_url"]
                    await update.message.reply_text(f"✅ Removed: {name[:80]}")
                else:
                    await update.message.reply_text(f"Invalid number. You have {len(subs)} sources.")
            else:
                # treat as URL
                self.db.run(
                    "DELETE FROM subscriptions WHERE user_id = %s AND source_url = %s",
                    (user["id"], arg),
                )
                await update.message.reply_text("✅ Removed (if it existed).")
            return

        # No arg → show numbered list, wait for reply
        text = "📋 <b>Your sources — send the number to remove:</b>\n\n"
        for i, s in enumerate(subs, 1):
            icon = PLATFORMS.get(s["source_type"], "📱")
            name = (s["source_name"] or s["source_url"])[:60]
            text += f"{i}. {icon} {name}\n"
        await update.message.reply_text(text, parse_mode="HTML")
        self._set_state(tg_id, "remove_pick", "")

    # ── /list ──

    async def cmd_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))
        if not user:
            await update.message.reply_text("Use /start first.")
            return

        subs = self.db.query(
            "SELECT source_type, source_name, source_url, is_active "
            "FROM subscriptions WHERE user_id = %s ORDER BY created_at",
            (user["id"],),
        )
        count = len(subs)
        if not count:
            await update.message.reply_text(f"📋 No sources yet ({count}/{MAX_FREE_SOURCES})\nUse /add to subscribe.")
            return

        text = f"📋 <b>Your sources ({count}/{MAX_FREE_SOURCES})</b>\n\n"
        for s in subs:
            icon  = PLATFORMS.get(s["source_type"], "📱")
            state = "✅" if s["is_active"] else "❌"
            name  = (s["source_name"] or s["source_url"])[:60]
            text += f"{state} {icon} {name}\n"
        text += "\nUse /remove to remove a source."
        await update.message.reply_text(text, parse_mode="HTML")

    # ── /history ──

    async def cmd_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))
        if not user:
            await update.message.reply_text("Use /start first.")
            return

        rows = self.db.query(
            """
            SELECT p.title, p.url, p.source_type
            FROM sent_history sh
            JOIN posts p ON sh.post_id = p.id
            WHERE sh.user_id = %s
            ORDER BY sh.sent_at DESC
            LIMIT 25
            """,
            (user["id"],),
        )
        if not rows:
            await update.message.reply_text("📖 No history yet. Add sources and wait for posts.")
            return

        text = "📖 <b>Last 25 posts from your feed:</b>\n\n"
        for r in rows:
            icon  = PLATFORMS.get(r["source_type"], "📱")
            title = (r["title"] or "No title")[:60]
            text += f"{icon} <a href='{r['url']}'>{title}</a>\n"

        await update.message.reply_text(
            text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup(
                [[_btn("◀️ Back to menu", "main_menu")]]
            ),
        )

    # ── /connect ──

    async def cmd_connect(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))
        if not user:
            await update.message.reply_text("Use /start first.")
            return

        channels = self.db.query(
            "SELECT chat_id, chat_title, chat_type FROM channels WHERE owner_id = %s",
            (user["id"],),
        )
        if not channels:
            await update.message.reply_text(
                "📡 <b>No channels/groups connected yet.</b>\n\n"
                "To connect:\n"
                "1. Add this bot as admin to your channel/group\n"
                "2. Bot auto-registers it\n"
                "3. Use /addchannel &lt;chat_id&gt; &lt;URL&gt; to add sources",
                parse_mode="HTML",
            )
            return

        text = "📡 <b>Your connected channels:</b>\n\n"
        for ch in channels:
            row = self.db.one(
                "SELECT COUNT(*) AS c FROM subscriptions "
                "WHERE channel_id = (SELECT id FROM channels WHERE chat_id = %s)",
                (ch["chat_id"],),
            )
            cnt = row["c"] if row else 0
            text += (
                f"• <b>{ch['chat_title']}</b> [{ch['chat_type']}]\n"
                f"  ID: <code>{ch['chat_id']}</code>  |  Sources: {cnt}\n\n"
            )
        text += "Use /addchannel &lt;chat_id&gt; &lt;URL&gt; to add sources."
        await update.message.reply_text(text, parse_mode="HTML")

    # ── /addchannel ──

    async def cmd_addchannel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))
        if not user:
            await update.message.reply_text("Use /start first.")
            return

        if len(context.args) < 2:
            await update.message.reply_text(
                "Usage: /addchannel &lt;chat_id&gt; &lt;URL or platform username&gt;\n\n"
                "Example: /addchannel -1001234567890 https://feeds.bbci.co.uk/news/rss.xml\n"
                "Get chat_id from /connect",
                parse_mode="HTML",
            )
            return

        try:
            chat_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ chat_id must be a number (e.g. -1001234567890).")
            return

        channel = self.db.one(
            "SELECT id FROM channels WHERE chat_id = %s AND owner_id = %s",
            (chat_id, user["id"]),
        )
        if not channel:
            await update.message.reply_text(
                "❌ Channel not found or you're not the owner.\n"
                "Add the bot as admin first, then use /connect to see linked channels."
            )
            return

        # Count existing channel subs
        row = self.db.one(
            "SELECT COUNT(*) AS c FROM subscriptions WHERE channel_id = %s", (channel["id"],)
        )
        if (row["c"] if row else 0) >= MAX_FREE_SOURCES:
            await update.message.reply_text(
                f"❌ This channel has reached {MAX_FREE_SOURCES} sources."
            )
            return

        url = context.args[1]
        src_type, rss_url, needs_check = detect_platform(url)
        if not src_type:
            await update.message.reply_text("❌ Unsupported source. Use an RSS URL or supported platform link.")
            return

        if needs_check:
            await update.message.reply_text("🔍 Checking source…")
            ok = await asyncio.to_thread(_check_rss, rss_url)
            if not ok:
                await update.message.reply_text("❌ Could not fetch a valid feed from this URL.")
                return

        feed_name = await asyncio.to_thread(_get_feed_title, rss_url)

        try:
            self.db.run(
                "INSERT INTO subscriptions "
                "(channel_id, source_url, source_type, source_name, is_active) "
                "VALUES (%s, %s, %s, %s, TRUE)",
                (channel["id"], rss_url, src_type, feed_name),
            )
            icon = PLATFORMS.get(src_type, "📱")
            await update.message.reply_text(
                f"✅ Added to channel:\n{icon} <b>{feed_name}</b>",
                parse_mode="HTML",
            )
        except Exception as e:
            if "unique" in str(e).lower() or "duplicate" in str(e).lower():
                await update.message.reply_text("⚠️ Already subscribed to this source in that channel.")
            else:
                await update.message.reply_text("❌ Error adding source.")

    # ── /user /chat ──

    async def cmd_user_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "✅ <b>Private mode</b>\n\nPosts will be delivered to this chat.\nUse /add to add sources.",
            parse_mode="HTML",
        )

    async def cmd_chat_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text(
                "Usage: <code>/chat @channelname</code> or <code>/chat -1001234567890</code>\n\n"
                "Make sure the bot is admin in that channel first, then use /connect.",
                parse_mode="HTML",
            )
            return
        target = context.args[0]
        await update.message.reply_text(
            f"ℹ️ To post to <b>{target}</b>:\n\n"
            "1. Add bot as admin to the channel\n"
            "2. Use /connect to see linked channels\n"
            "3. Use /addchannel &lt;chat_id&gt; &lt;URL&gt; to add sources",
            parse_mode="HTML",
        )

    # ── /help ──

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "<b>📚 IN365Bot — Help</b>\n\n"
            "<b>Add sources:</b>\n"
            "<code>/add https://feeds.bbci.co.uk/news/rss.xml</code>\n"
            "<code>/add youtube UCxxxxxxxxxxxxxxxxxx</code>\n"
            "<code>/add reddit python</code>\n"
            "<code>/add medium @username</code>\n"
            "<code>/add livejournal username</code>\n\n"
            "<b>Manage:</b>\n"
            "/list — see your subscriptions\n"
            "/remove — remove a source\n"
            "/history — last 25 delivered posts\n\n"
            "<b>Channel / Group:</b>\n"
            "/connect — see connected channels\n"
            "/addchannel &lt;chat_id&gt; &lt;URL&gt; — add source to channel\n\n"
            "Posts are checked every 5 minutes.",
            parse_mode="HTML",
        )

    # ── /admin ──

    async def cmd_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            return
        u  = self.db.one("SELECT COUNT(*) AS c FROM users")
        s  = self.db.one("SELECT COUNT(*) AS c FROM subscriptions")
        sa = self.db.one("SELECT COUNT(*) AS c FROM subscriptions WHERE is_active=TRUE")
        ch = self.db.one("SELECT COUNT(*) AS c FROM channels")
        p  = self.db.one("SELECT COUNT(*) AS c FROM posts")
        sh = self.db.one("SELECT COUNT(*) AS c FROM sent_history")
        await update.message.reply_text(
            f"📊 <b>Admin Stats</b>\n\n"
            f"Users:         {u['c']}\n"
            f"Subscriptions: {s['c']} ({sa['c']} active)\n"
            f"Channels:      {ch['c']}\n"
            f"Posts tracked: {p['c']}\n"
            f"Sent:          {sh['c']}",
            parse_mode="HTML",
        )

    # ── Callback handler ──

    async def handle_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data  = query.data
        tg_id = update.effective_user.id
        user  = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))

        # ── Main menu ──
        if data == "main_menu":
            if not user:
                user = self._ensure_user(tg_id, update.effective_user.username or "user")
            count = self._source_count(user["id"])
            await query.edit_message_text(
                self._start_text(user, count),
                parse_mode="HTML",
                reply_markup=kb_main(count),
            )

        # ── Add source (via button) ──
        elif data == "add_source":
            self._set_state(tg_id, "add_source", "")
            await query.edit_message_text(
                "➕ <b>Add source</b>\n\n"
                "Send me a link or shorthand:\n\n"
                "<code>https://feeds.bbci.co.uk/news/rss.xml</code>\n"
                "<code>youtube UCxxxxxxxxxxxxxxxxxx</code>\n"
                "<code>reddit python</code>\n"
                "<code>medium @username</code>\n"
                "<code>livejournal username</code>\n\n"
                "Just type and send — no /add command needed here.",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        # ── My feed ──
        elif data == "my_feed":
            if not user:
                await query.edit_message_text("Use /start first.", reply_markup=kb_back())
                return
            subs = self.db.query(
                "SELECT id, source_type, source_name, source_url, is_active "
                "FROM subscriptions WHERE user_id = %s ORDER BY created_at",
                (user["id"],),
            )
            count = len(subs)
            await query.edit_message_text(
                f"📋 <b>My feed — {count}/{MAX_FREE_SOURCES} sources</b>\n\n"
                "Tap a source to toggle on/off.\n"
                "Private + channels + groups.",
                parse_mode="HTML",
                reply_markup=kb_my_feed(subs),
            )

        elif data.startswith("tog_") and data[4:].isdigit():
            sub_id = int(data[4:])
            if user:
                sub = self.db.one(
                    "SELECT is_active, user_id FROM subscriptions WHERE id = %s", (sub_id,)
                )
                if sub and sub["user_id"] == user["id"]:
                    self.db.run(
                        "UPDATE subscriptions SET is_active = %s WHERE id = %s",
                        (not sub["is_active"], sub_id),
                    )
            subs = self.db.query(
                "SELECT id, source_type, source_name, source_url, is_active "
                "FROM subscriptions WHERE user_id = %s ORDER BY created_at",
                (user["id"],),
            )
            await query.edit_message_text(
                f"📋 <b>My feed — {len(subs)}/{MAX_FREE_SOURCES}</b>\n\nTap to toggle.",
                parse_mode="HTML",
                reply_markup=kb_my_feed(subs),
            )

        elif data == "subs_off":
            if user:
                self.db.run(
                    "UPDATE subscriptions SET is_active = FALSE WHERE user_id = %s", (user["id"],)
                )
            subs = self.db.query(
                "SELECT id, source_type, source_name, source_url, is_active "
                "FROM subscriptions WHERE user_id = %s ORDER BY created_at",
                (user["id"],),
            )
            await query.edit_message_text(
                f"❌ All sources turned off.\n\n📋 {len(subs)}/{MAX_FREE_SOURCES}",
                reply_markup=kb_my_feed(subs),
            )

        elif data == "subs_on":
            if user:
                self.db.run(
                    "UPDATE subscriptions SET is_active = TRUE WHERE user_id = %s", (user["id"],)
                )
            subs = self.db.query(
                "SELECT id, source_type, source_name, source_url, is_active "
                "FROM subscriptions WHERE user_id = %s ORDER BY created_at",
                (user["id"],),
            )
            await query.edit_message_text(
                f"✅ All sources turned on.\n\n📋 {len(subs)}/{MAX_FREE_SOURCES}",
                reply_markup=kb_my_feed(subs),
            )

        # ── Settings ──
        elif data == "settings":
            await query.edit_message_text(
                "⚙️ <b>Settings</b>", parse_mode="HTML", reply_markup=kb_settings()
            )

        elif data == "s_delivery":
            silent = user.get("silent_mode", False) if user else False
            await query.edit_message_text(
                "📨 <b>Delivery options</b>\n\n"
                "Silent mode — posts arrive without notification sound.",
                parse_mode="HTML",
                reply_markup=kb_delivery(silent),
            )

        elif data == "tog_silent":
            if user:
                new = not user.get("silent_mode", False)
                self.db.run(
                    "UPDATE users SET silent_mode = %s WHERE telegram_id = %s", (new, tg_id)
                )
                user = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))
            silent = user.get("silent_mode", False) if user else False
            await query.edit_message_text(
                "📨 <b>Delivery options</b>",
                parse_mode="HTML",
                reply_markup=kb_delivery(silent),
            )

        elif data == "s_display":
            hide = user.get("hide_original_link", False) if user else False
            await query.edit_message_text(
                "🖥 <b>Display options</b>",
                parse_mode="HTML",
                reply_markup=kb_display(hide),
            )

        elif data == "tog_hide_link":
            if user:
                new = not user.get("hide_original_link", False)
                self.db.run(
                    "UPDATE users SET hide_original_link = %s WHERE telegram_id = %s", (new, tg_id)
                )
                user = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))
            hide = user.get("hide_original_link", False) if user else False
            await query.edit_message_text(
                "🖥 <b>Display options</b>",
                parse_mode="HTML",
                reply_markup=kb_display(hide),
            )

        elif data == "s_filter":
            kw = (user.get("keyword_filter", "") or "") if user else ""
            await query.edit_message_text(
                f"🗑 <b>Message filtering</b>\n\n"
                f"Posts whose title contains any of your keywords will be skipped.\n\n"
                f"<b>Current keywords:</b> <code>{kw or 'none'}</code>",
                parse_mode="HTML",
                reply_markup=kb_filter(kw),
            )

        elif data == "set_kw":
            self._set_state(tg_id, "set_kw", "")
            await query.edit_message_text(
                "✏️ Send your keywords separated by commas.\n\n"
                "Example: <code>bitcoin, crypto, nft</code>",
                parse_mode="HTML",
                reply_markup=kb_back("settings"),
            )

        elif data == "clear_kw":
            if user:
                self.db.run(
                    "UPDATE users SET keyword_filter = '' WHERE telegram_id = %s", (tg_id,)
                )
                user = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))
            kw = ""
            await query.edit_message_text(
                "🗑 <b>Message filtering</b>\n\nKeywords cleared.",
                parse_mode="HTML",
                reply_markup=kb_filter(kw),
            )

        elif data == "s_moderation":
            await query.edit_message_text(
                "👮 <b>Moderation mode</b>\n\n"
                "⚠️ Only available in channel/group mode.\n\n"
                "When enabled, posts go to a temporary moderation channel first.\n"
                "You review and publish them manually.\n\n"
                "<b>Commands:</b>\n"
                "<code>/set moderation -100XXXXXXXXX</code>\n"
                "<code>/set autopublish 5</code>  (publish after 5 min)",
                parse_mode="HTML",
                reply_markup=kb_back("settings"),
            )

        elif data == "s_butler":
            await query.edit_message_text(
                "🤵 <b>Butler mode</b>\n\n"
                "Collects posts and delivers them in batches at set times "
                "(like a newspaper delivery).\n\n"
                "<b>Commands:</b>\n"
                "<code>/set butler_periods 7-9,19-21</code>\n"
                "  → deliver at 7–9 AM and 7–9 PM\n\n"
                "<code>/set butler_interval 5</code>\n"
                "  → min 5 minutes between messages\n\n"
                "<code>/reset butler_periods</code>\n"
                "<code>/reset butler_interval</code>",
                parse_mode="HTML",
                reply_markup=kb_back("settings"),
            )

        elif data == "s_ai":
            await query.edit_message_text(
                "🤖 <b>AI (LLM) settings</b>\n\n"
                "AI summarization is not enabled in this instance.\n\n"
                "To enable it, add an <code>OPENAI_API_KEY</code> environment variable "
                "in your Railway project settings and redeploy.",
                parse_mode="HTML",
                reply_markup=kb_back("settings"),
            )

        elif data == "s_twitter":
            await query.edit_message_text(
                "🐦 <b>Twitter / X accounts</b>\n\n"
                "Twitter no longer provides a free public API or RSS.\n\n"
                "If a public Nitter instance is available in your region, you can add:\n"
                "<code>/add https://nitter.net/username/rss</code>\n\n"
                "Otherwise Twitter is not supported in this free version.",
                parse_mode="HTML",
                reply_markup=kb_back("settings"),
            )

        elif data == "s_timezone":
            await query.edit_message_text(
                "🕐 <b>Your timezone</b>\n\nSelect your region:",
                parse_mode="HTML",
                reply_markup=kb_timezone(),
            )

        elif data.startswith("tz_"):
            region = data[3:]
            if user:
                self.db.run(
                    "UPDATE users SET timezone = %s WHERE telegram_id = %s", (region, tg_id)
                )
            await query.edit_message_text(
                f"✅ Timezone set to <b>{region}</b>",
                parse_mode="HTML",
                reply_markup=kb_back("settings"),
            )

        elif data == "s_language":
            await query.edit_message_text(
                "🌍 <b>Choose language</b>",
                parse_mode="HTML",
                reply_markup=kb_language(),
            )

        elif data.startswith("lang_"):
            lang = data[5:]
            if user:
                self.db.run(
                    "UPDATE users SET language = %s WHERE telegram_id = %s", (lang, tg_id)
                )
            await query.edit_message_text(
                f"✅ Language set to <b>{lang.upper()}</b>",
                parse_mode="HTML",
                reply_markup=kb_back("settings"),
            )

        # ── History ──
        elif data == "history":
            if not user:
                await query.edit_message_text("Use /start first.", reply_markup=kb_back())
                return
            rows = self.db.query(
                """
                SELECT p.title, p.url, p.source_type
                FROM sent_history sh
                JOIN posts p ON sh.post_id = p.id
                WHERE sh.user_id = %s
                ORDER BY sh.sent_at DESC LIMIT 25
                """,
                (user["id"],),
            )
            if not rows:
                await query.edit_message_text(
                    "📖 <b>History</b>\n\nNo posts delivered yet.",
                    parse_mode="HTML",
                    reply_markup=kb_back(),
                )
                return
            text = "📖 <b>Last 25 from your feed:</b>\n\n"
            for r in rows:
                icon  = PLATFORMS.get(r["source_type"], "📱")
                title = (r["title"] or "No title")[:60]
                text += f"{icon} <a href='{r['url']}'>{title}</a>\n"
            await query.edit_message_text(
                text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=kb_back(),
            )

        # ── Working modes ──
        elif data == "working_modes":
            await query.edit_message_text(
                "🔄 <b>Working modes</b>\n\n"
                "<b>Private mode</b> — posts come to this DM chat.\n"
                "Command: <code>/user</code>\n\n"
                "<b>Channel / Group mode</b> — posts go to your channel.\n"
                "1. Add bot as admin with post permission\n"
                "2. Bot auto-registers the channel\n"
                "3. Use <code>/addchannel chat_id URL</code> to add sources\n\n"
                "<b>View connected channels:</b> /connect",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        # ── Direct connection ──
        elif data == "direct_conn":
            await query.edit_message_text(
                "🚀 <b>Direct connection</b>\n\n"
                "Direct Telegram connection allows forwarding from private channels, "
                "groups, and other bots using a real Telegram account (userbot).\n\n"
                "This requires Telethon or Pyrogram with a phone number login.\n"
                "It is not available in this free self-hosted version.\n\n"
                "For public sources, use /add with any RSS URL.",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        # ── RSS generator ──
        elif data == "rss_gen":
            await query.edit_message_text(
                "📡 <b>RSS Generator</b>\n\n"
                "If your source has no RSS feed, use these free tools:\n\n"
                "• <a href='https://rss-bridge.org/bridge01/'>RSS Bridge</a> "
                "— converts 350+ sites to RSS\n"
                "• <a href='https://rsshub.app/'>RSSHub</a> "
                "— generates RSS for many platforms\n\n"
                "<b>How to use:</b>\n"
                "1. Open RSS Bridge and search for your site\n"
                "2. Configure parameters and click Generate\n"
                "3. Copy the RSS URL\n"
                "4. Send it to the bot: <code>/add &lt;url&gt;</code>",
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=kb_back(),
            )

        # ── Referral ──
        elif data == "referral":
            link = f"https://t.me/{BOT_USERNAME}?start=ref{tg_id}"
            share_url = f"https://t.me/share/url?url={link}&text=Great+RSS+bot!"
            await query.edit_message_text(
                f"🤝 <b>Referral program</b>\n\n"
                f"Invited users: <b>0</b>\n\n"
                f"Your link:\n<code>{link}</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [_url_btn("👋 Invite a friend", share_url)],
                    [_btn("◀️ Back", "main_menu")],
                ]),
            )

        # ── How to use ──
        elif data == "how_to_use":
            await query.edit_message_text(
                "❓ <b>How to use IN365Bot</b>\n\n"
                "<b>Step 1 — Add a source</b>\n"
                "<code>/add https://feeds.bbci.co.uk/news/rss.xml</code>\n\n"
                "<b>Step 2 — Wait</b>\n"
                "Bot checks every 5 min and forwards new posts automatically.\n\n"
                "<b>Step 3 — Manage</b>\n"
                "Menu → My feed → toggle sources on/off\n\n"
                "<b>Shorthand examples:</b>\n"
                "<code>/add reddit python</code>\n"
                "<code>/add medium @username</code>\n"
                "<code>/add youtube UCxxxxxxxxxxxxxxxx</code>\n\n"
                "<b>For channels:</b> add bot as admin → /addchannel",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        # ── Premium ──
        elif data == "premium":
            await query.edit_message_text(
                "⭐ <b>Premium</b>\n\n"
                "This is a self-hosted open-source bot.\n\n"
                "<b>Free limits:</b>\n"
                f"— Sources: {MAX_FREE_SOURCES}\n"
                "— Refresh rate: every 5 minutes\n"
                "— One channel/group destination\n\n"
                "To increase limits, deploy your own instance or contact the bot owner.",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        elif data == "noop":
            pass  # button that does nothing (status display)

    # ── Text message handler (state machine) ──────────────────────────────────

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        text  = update.message.text.strip()
        state, _ctx = self._get_state(tg_id)

        if state == "add_source":
            self._clear_state(tg_id)
            user = self._ensure_user(tg_id, update.effective_user.username or "user")
            # Split into at most 2 tokens (platform username  OR  just URL)
            parts = text.split(None, 1)
            await self._do_add(update, user, parts)

        elif state == "set_kw":
            self._clear_state(tg_id)
            self.db.run(
                "UPDATE users SET keyword_filter = %s WHERE telegram_id = %s",
                (text[:500], tg_id),
            )
            await update.message.reply_text(
                f"✅ Keywords saved:\n<code>{text[:500]}</code>\n\n"
                "Posts containing these words will be skipped.",
                parse_mode="HTML",
            )

        elif state == "remove_pick":
            self._clear_state(tg_id)
            user = self.db.one("SELECT id FROM users WHERE telegram_id = %s", (tg_id,))
            if not user:
                return
            if not text.isdigit():
                await update.message.reply_text("Please send a number.")
                return
            subs = self.db.query(
                "SELECT id, source_name, source_url FROM subscriptions "
                "WHERE user_id = %s ORDER BY created_at",
                (user["id"],),
            )
            idx = int(text) - 1
            if 0 <= idx < len(subs):
                self.db.run("DELETE FROM subscriptions WHERE id = %s", (subs[idx]["id"],))
                name = subs[idx]["source_name"] or subs[idx]["source_url"]
                await update.message.reply_text(f"✅ Removed: {name[:80]}")
            else:
                await update.message.reply_text(f"Invalid number. You have {len(subs)} sources.")

    # ── Channel member event ───────────────────────────────────────────────────

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
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (chat_id) DO UPDATE SET chat_title = EXCLUDED.chat_title",
                    (user["id"], chat.id, chat.title or "Unknown", chat.type),
                )
                await context.bot.send_message(
                    chat_id=chat.id,
                    text=(
                        f"✅ <b>Bot connected to {chat.title}!</b>\n\n"
                        f"Now in your private chat with me, use:\n"
                        f"<code>/addchannel {chat.id} &lt;URL&gt;</code>\n"
                        f"to add sources to this channel/group."
                    ),
                    parse_mode="HTML",
                )
                logger.info(f"Added to {chat.title} ({chat.id})")
            except Exception as e:
                logger.error(f"on_chat_member add: {e}")

        elif new_status in ("kicked", "left"):
            try:
                self.db.run("DELETE FROM channels WHERE chat_id = %s", (chat.id,))
                logger.info(f"Removed from {chat.id}")
            except Exception as e:
                logger.error(f"on_chat_member remove: {e}")

    # ── Polling loop ───────────────────────────────────────────────────────────

    async def _polling_loop(self, app):
        while True:
            try:
                await self._poll_all(app)
            except Exception as e:
                logger.error(f"Polling loop error: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def _poll_all(self, app):
        # User subscriptions
        user_subs = self.db.query(
            """
            SELECT s.id, s.source_url, s.source_type, s.source_name,
                   u.telegram_id, u.id AS user_id,
                   u.silent_mode, u.hide_original_link, u.keyword_filter,
                   NULL::bigint AS channel_chat_id,
                   NULL::int    AS channel_id
            FROM subscriptions s
            JOIN users u ON s.user_id = u.id
            WHERE s.user_id IS NOT NULL
              AND s.channel_id IS NULL
              AND s.is_active = TRUE
            ORDER BY s.last_check ASC NULLS FIRST
            LIMIT 30
            """
        )

        # Channel subscriptions
        chan_subs = self.db.query(
            """
            SELECT s.id, s.source_url, s.source_type, s.source_name,
                   NULL::bigint AS telegram_id,
                   NULL::int    AS user_id,
                   FALSE        AS silent_mode,
                   FALSE        AS hide_original_link,
                   ''           AS keyword_filter,
                   c.chat_id    AS channel_chat_id,
                   c.id         AS channel_id
            FROM subscriptions s
            JOIN channels c ON s.channel_id = c.id
            WHERE s.channel_id IS NOT NULL
              AND s.is_active = TRUE
            ORDER BY s.last_check ASC NULLS FIRST
            LIMIT 30
            """
        )

        all_subs = list(user_subs) + list(chan_subs)
        if not all_subs:
            return

        for sub in all_subs:
            try:
                posts = await asyncio.to_thread(
                    _fetch_feed, sub["source_url"], sub["source_type"]
                )
                for post in posts:
                    await self._deliver(app, sub, post)

                self.db.run(
                    "UPDATE subscriptions SET last_check = NOW() WHERE id = %s",
                    (sub["id"],),
                )
                await asyncio.sleep(1)  # gentle rate limit between subs

            except Exception as e:
                logger.error(f"Poll sub {sub['id']}: {e}")

    async def _deliver(self, app, sub: dict, post: dict):
        try:
            # Keyword filter
            kw_raw = sub.get("keyword_filter", "") or ""
            if kw_raw.strip():
                keywords = [k.strip().lower() for k in kw_raw.split(",") if k.strip()]
                if any(kw in post["title"].lower() for kw in keywords):
                    return

            # Upsert post record
            existing = self.db.one(
                "SELECT id FROM posts WHERE source_id = %s", (post["source_id"],)
            )
            if existing:
                post_id = existing["id"]
            else:
                post_id = self.db.insert_id(
                    "INSERT INTO posts (source_id, source_type, title, url) "
                    "VALUES (%s, %s, %s, %s) RETURNING id",
                    (post["source_id"], post["source_type"],
                     post["title"][:500], post["url"][:500]),
                )
                if post_id is None:
                    return

            user_id    = sub.get("user_id")
            channel_id = sub.get("channel_id")

            # Check if already sent to this target
            if user_id:
                sent = self.db.one(
                    "SELECT id FROM sent_history WHERE user_id = %s AND post_id = %s",
                    (user_id, post_id),
                )
            elif channel_id:
                sent = self.db.one(
                    "SELECT id FROM sent_history WHERE channel_id = %s AND post_id = %s",
                    (channel_id, post_id),
                )
            else:
                return

            if sent:
                return

            # Build message text
            icon        = PLATFORMS.get(post["source_type"], "📱")
            source_name = (sub.get("source_name") or post["source_type"])[:60]
            msg = f"{icon} <b>{source_name}</b>\n\n{post['title']}"

            if not sub.get("hide_original_link"):
                msg += f"\n\n<a href='{post['url']}'>🔗 View original post</a>"

            target = sub.get("telegram_id") or sub.get("channel_chat_id")
            if not target:
                return

            await app.bot.send_message(
                chat_id=target,
                text=msg,
                parse_mode="HTML",
                disable_notification=bool(sub.get("silent_mode")),
                disable_web_page_preview=False,
            )

            self.db.run(
                "INSERT INTO sent_history (user_id, channel_id, post_id) VALUES (%s, %s, %s)",
                (user_id, channel_id, post_id),
            )
            logger.info(f"Delivered post {post_id} → {target}")

        except Exception as e:
            logger.error(f"Deliver error: {e}")

    # ── Run ────────────────────────────────────────────────────────────────────

    def run(self):
        logger.info("IN365Bot starting…")
        try:
            self.app.run_polling(
                allowed_updates=["message", "callback_query", "my_chat_member"]
            )
        finally:
            if self.db.conn:
                self.db.conn.close()


if __name__ == "__main__":
    Bot().run()
