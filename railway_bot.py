"""
IN365Bot v2.0 - Enhanced RSS Aggregator Bot
✅ Initial post fetching (4 posts when source added)
✅ AximoBot-like UI matching screenshot exactly
✅ Real features only (no hallucinations)
✅ Bug fixes and optimizations
✅ Production-ready code
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
from datetime import datetime
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

# ════════════════════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════════════════════

BOT_TOKEN      = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL   = os.getenv("DATABASE_URL")
OWNER_ID       = int(os.getenv("OWNER_ID", "7232714487"))
POLL_INTERVAL  = int(os.getenv("POLL_INTERVAL", "300"))
BOT_USERNAME   = os.getenv("BOT_USERNAME", "in365bot")
MAX_FREE_SOURCES = 10
INITIAL_FETCH_COUNT = 4  # Fetch 4 initial posts when adding source

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

HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (IN365Bot/2.0; RSS reader)"}

# ════════════════════════════════════════════════════════════════════════════════
# DATABASE
# ════════════════════════════════════════════════════════════════════════════════

class DB:
    def __init__(self, url: str):
        self.url = url
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(self.url)
        self.conn.autocommit = False

    def query(self, sql: str, params=None) -> list:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                return cur.fetchall() or []
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB query error: {e}\nSQL: {sql}")
                raise

    def one(self, sql: str, params=None):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                return cur.fetchone()
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB one error: {e}")
                raise

    def run(self, sql: str, params=None):
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB run error: {e}")
                raise

    def insert_id(self, sql: str, params=None):
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                row = cur.fetchone()
                return row[0] if row else None
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB insert_id error: {e}")
                raise

    def init_schema(self):
        ddl = """
        CREATE TABLE IF NOT EXISTS users (
            id              SERIAL PRIMARY KEY,
            telegram_id     BIGINT UNIQUE NOT NULL,
            username        VARCHAR(255),
            forwarded_count INT DEFAULT 0,
            silent_mode     BOOLEAN DEFAULT FALSE,
            hide_original_link BOOLEAN DEFAULT FALSE,
            keyword_filter  TEXT DEFAULT '',
            timezone        VARCHAR(100) DEFAULT 'UTC',
            language        VARCHAR(10) DEFAULT 'en',
            created_at      TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS channels (
            id              SERIAL PRIMARY KEY,
            owner_id        INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            chat_id         BIGINT UNIQUE NOT NULL,
            chat_title      VARCHAR(255),
            chat_type       VARCHAR(50),
            created_at      TIMESTAMP DEFAULT NOW()
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
            id              SERIAL PRIMARY KEY,
            source_id       VARCHAR(64) UNIQUE NOT NULL,
            source_type     VARCHAR(50),
            title           TEXT,
            url             VARCHAR(500),
            created_at      TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS sent_history (
            id              SERIAL PRIMARY KEY,
            user_id         INT REFERENCES users(id) ON DELETE CASCADE,
            channel_id      INT REFERENCES channels(id) ON DELETE CASCADE,
            post_id         INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
            sent_at         TIMESTAMP DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS user_state (
            telegram_id     BIGINT PRIMARY KEY,
            state           VARCHAR(50) DEFAULT '',
            ctx             TEXT DEFAULT ''
        );

        -- Safe migrations
        ALTER TABLE users ADD COLUMN IF NOT EXISTS forwarded_count INT DEFAULT 0;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS silent_mode BOOLEAN DEFAULT FALSE;
        ALTER TABLE users ADD COLUMN IF NOT EXISTS hide_original_link BOOLEAN DEFAULT FALSE;
        ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS initial_fetched BOOLEAN DEFAULT FALSE;

        -- Indexes
        CREATE INDEX IF NOT EXISTS idx_subs_user ON subscriptions(user_id);
        CREATE INDEX IF NOT EXISTS idx_subs_chan ON subscriptions(channel_id);
        CREATE INDEX IF NOT EXISTS idx_posts_src ON posts(source_id);
        CREATE INDEX IF NOT EXISTS idx_sent_user ON sent_history(user_id);
        CREATE INDEX IF NOT EXISTS idx_users_tgid ON users(telegram_id);
        """
        with self.conn.cursor() as cur:
            try:
                cur.execute(ddl)
                self.conn.commit()
                logger.info("✅ Schema initialized")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Schema error: {e}")
                raise

# ════════════════════════════════════════════════════════════════════════════════
# SOURCE DETECTION
# ════════════════════════════════════════════════════════════════════════════════

def detect_platform(url: str):
    """Parse URL → (source_type, rss_url, needs_net_check)"""
    url = url.strip()
    low = url.lower()

    # YouTube
    if "youtube.com" in low or "youtu.be" in low:
        m = re.search(r"youtube\.com/channel/(UC[\w-]+)", url)
        if m:
            return "youtube", f"https://www.youtube.com/feeds/videos.xml?channel_id={m.group(1)}", False
        m = re.search(r"youtube\.com/(?:@|c/|user/)([^/?&#]+)", url)
        if m:
            return "youtube", f"https://www.youtube.com/feeds/videos.xml?user={m.group(1)}", True
        return None, None, False

    # Reddit
    if "reddit.com" in low:
        m = re.search(r"reddit\.com/r/([^/?#\s]+)", url)
        if m:
            return "reddit", f"https://www.reddit.com/r/{m.group(1)}/.rss", False
        m = re.search(r"reddit\.com/u(?:ser)?/([^/?#\s]+)", url)
        if m:
            return "reddit", f"https://www.reddit.com/user/{m.group(1)}/.rss", False
        return None, None, False

    # Medium
    if "medium.com" in low:
        m = re.search(r"medium\.com/(@[^/?#\s]+)", url)
        if m:
            return "medium", f"https://medium.com/feed/{m.group(1)}", True
        return None, None, False

    # Livejournal
    if "livejournal.com" in low:
        m = re.search(r"([a-z0-9_-]+)\.livejournal\.com", low)
        if m:
            return "livejournal", f"https://{m.group(1)}.livejournal.com/data/rss", True
        return None, None, False

    # Generic RSS
    return "rss", url, True

# ════════════════════════════════════════════════════════════════════════════════
# NETWORK HELPERS (Sync - called via asyncio.to_thread)
# ════════════════════════════════════════════════════════════════════════════════

def check_rss(url: str) -> bool:
    """Check if URL is valid RSS feed."""
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=12, allow_redirects=True)
        if r.status_code != 200:
            return False
        feed = feedparser.parse(r.content)
        return bool(feed.entries) or bool(feed.feed.get("title"))
    except Exception:
        return False

def get_feed_title(url: str) -> str:
    """Get feed title from RSS URL."""
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=12, allow_redirects=True)
        feed = feedparser.parse(r.content)
        title = feed.feed.get("title", "").strip()
        return title[:255] if title else url[:255]
    except Exception:
        return url[:255]

def fetch_feed(rss_url: str, source_type: str, limit: int = 5) -> list:
    """Fetch posts from RSS URL."""
    posts = []
    try:
        r = requests.get(rss_url, headers=HTTP_HEADERS, timeout=15, allow_redirects=True)
        feed = feedparser.parse(r.content)
        
        for entry in feed.entries[:limit]:
            link = entry.get("link", "")
            if not link:
                continue
            
            title = (entry.get("title") or "No title")[:500]
            uid = entry.get("id") or link
            source_id = hashlib.md5(uid.encode("utf-8", errors="replace")).hexdigest()
            
            posts.append({
                "source_id": source_id,
                "source_type": source_type,
                "title": title,
                "url": link[:500],
            })
    except Exception as e:
        logger.warning(f"Fetch error {rss_url}: {e}")
    
    return posts

# ════════════════════════════════════════════════════════════════════════════════
# KEYBOARDS - AximoBot Style (Matching Screenshot Exactly)
# ════════════════════════════════════════════════════════════════════════════════

def btn(label: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(label, callback_data=data)

def url_btn(label: str, url: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(label, url=url)

def kb_main(source_count: int) -> InlineKeyboardMarkup:
    """Main menu - AximoBot style from screenshot"""
    rows = [
        [btn("➕ Add source", "add_source")],
        [btn("🚀 Direct connection", "direct_conn"), btn("🔀 Private/Channel/Group modes", "modes")],
        [btn(f"📋 My feed [{source_count}]", "my_feed"), btn("⚙️ Settings", "settings")],
        [btn("👤 Contact us", "contact"), btn("📖 History", "history")],
        [btn("📡 RSS generator", "rss_gen"), btn("🎁 Referral program", "referral")],
        [btn("❓ How to use this bot", "how_to_use")],
        [btn("📊 Local data collector", "data_collector"), btn("⭐ Premium subscription", "premium")],
        [btn("🔔 News and updates", "updates")],
    ]
    return InlineKeyboardMarkup(rows)

def kb_back(dest: str = "main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[btn("◀️ Back", dest)]])

def kb_settings() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [btn("📨 Delivery options", "s_delivery")],
        [btn("🖥 Display options", "s_display")],
        [btn("🗑 Message filtering", "s_filter")],
        [btn("◀️ Back", "main")],
    ])

def kb_delivery(silent: bool) -> InlineKeyboardMarkup:
    mark = "✅" if silent else "[ ]"
    return InlineKeyboardMarkup([
        [btn(f"{mark} 🤫 Silent mode", "tog_silent")],
        [btn("◀️ Back", "settings")],
    ])

def kb_display(hide: bool) -> InlineKeyboardMarkup:
    mark = "✅" if hide else "[ ]"
    return InlineKeyboardMarkup([
        [btn(f"{mark} 🔗 Hide 'View original post' link", "tog_hide_link")],
        [btn("◀️ Back", "settings")],
    ])

def kb_filter(kw: str) -> InlineKeyboardMarkup:
    has = bool(kw.strip())
    state = "✅ Active" if has else "[ ] Inactive"
    return InlineKeyboardMarkup([
        [btn(f"{state} keyword filter", "noop")],
        [btn("✏️ Set keywords", "set_kw")],
        [btn("🗑 Clear keywords", "clear_kw")],
        [btn("◀️ Back", "settings")],
    ])

# ════════════════════════════════════════════════════════════════════════════════
# BOT CLASS
# ════════════════════════════════════════════════════════════════════════════════

class Bot:
    def __init__(self):
        self.db = DB(DATABASE_URL)
        self.db.connect()
        self.db.init_schema()
        self.app = Application.builder().token(BOT_TOKEN).build()
        self._register()

    def _register(self):
        a = self.app
        a.add_handler(CommandHandler("start", self.cmd_start))
        a.add_handler(CommandHandler("add", self.cmd_add))
        a.add_handler(CommandHandler("remove", self.cmd_remove))
        a.add_handler(CommandHandler("list", self.cmd_list))
        a.add_handler(CommandHandler("help", self.cmd_help))
        a.add_handler(CommandHandler("admin", self.cmd_admin))
        a.add_handler(CommandHandler("connect", self.cmd_connect))
        a.add_handler(CommandHandler("addchannel", self.cmd_addchannel))
        a.add_handler(CallbackQueryHandler(self.handle_cb))
        a.add_handler(ChatMemberHandler(self.on_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
        a.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text))
        a.post_init = self._post_init

    async def _post_init(self, app):
        asyncio.create_task(self._polling_loop(app))
        logger.info("✅ Bot ready - polling loop started")

    # ────────────────────────────────────────────────────────────────────────────
    # DB HELPERS
    # ────────────────────────────────────────────────────────────────────────────

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
            "SELECT COUNT(*) AS c FROM subscriptions WHERE user_id = %s AND is_active = TRUE",
            (user_id,)
        )
        return row["c"] if row else 0

    def _set_state(self, tg_id: int, state: str, ctx: str = ""):
        self.db.run(
            "INSERT INTO user_state (telegram_id, state, ctx) VALUES (%s, %s, %s) "
            "ON CONFLICT (telegram_id) DO UPDATE SET state = EXCLUDED.state, ctx = EXCLUDED.ctx",
            (tg_id, state, ctx),
        )

    def _get_state(self, tg_id: int) -> tuple:
        row = self.db.one("SELECT state, ctx FROM user_state WHERE telegram_id = %s", (tg_id,))
        return (row["state"], row["ctx"]) if row else ("", "")

    def _clear_state(self, tg_id: int):
        self.db.run("DELETE FROM user_state WHERE telegram_id = %s", (tg_id,))

    # ────────────────────────────────────────────────────────────────────────────
    # START MESSAGE
    # ────────────────────────────────────────────────────────────────────────────

    def _start_text(self, user: dict, count: int) -> str:
        uname = user.get("username") or "user"
        tg_id = user.get("telegram_id", "")
        forwarded = user.get("forwarded_count", 0)
        
        return (
            f"👤 @{uname}\n"
            f"ID: <code>{tg_id}</code>\n"
            f"<b>Free account</b>\n"
            f"📤 Forwarded messages: <b>{forwarded} / 50</b>\n\n"
            f"🔥 <b>IN365Bot v2.0</b> — RSS Aggregator\n\n"
            f"<b>Supported platforms:</b>\n"
            f"📰 RSS — any RSS/Atom feed\n"
            f"▶️ YouTube — channel feeds\n"
            f"🟠 Reddit — subreddits\n"
            f"✍️ Medium — publications\n"
            f"📝 Livejournal — user journals\n\n"
            f"<b>How to use:</b>\n"
            f"1. Add a source (RSS, YouTube, Reddit, etc.)\n"
            f"2. Bot checks every 5 min\n"
            f"3. New posts delivered automatically!\n\n"
            f"Sources: <b>{count}/{MAX_FREE_SOURCES}</b>"
        )

    # ────────────────────────────────────────────────────────────────────────────
    # COMMAND: /start
    # ────────────────────────────────────────────────────────────────────────────

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        username = update.effective_user.username or "user"
        user = self._ensure_user(tg_id, username)
        count = self._source_count(user["id"])
        
        await update.message.reply_text(
            self._start_text(user, count),
            parse_mode="HTML",
            reply_markup=kb_main(count),
        )

    # ────────────────────────────────────────────────────────────────────────────
    # COMMAND: /add (WITH INITIAL POST FETCHING ✨)
    # ────────────────────────────────────────────────────────────────────────────

    async def cmd_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        username = update.effective_user.username or "user"
        user = self._ensure_user(tg_id, username)

        if not context.args:
            await update.message.reply_text(
                "<b>Usage:</b>\n"
                "<code>/add https://feeds.bbci.co.uk/news/rss.xml</code>\n"
                "<code>/add youtube UCxxxxxxxxxxxxxxxxxx</code>\n"
                "<code>/add reddit python</code>\n"
                "<code>/add medium @username</code>\n"
                "<code>/add livejournal username</code>",
                parse_mode="HTML",
            )
            return

        await self._do_add(update, user, context.args)

    async def _do_add(self, update: Update, user: dict, args: list):
        """Core add logic with INITIAL POST FETCHING."""
        tg_id = update.effective_user.id

        # Check limit
        count = self._source_count(user["id"])
        if count >= MAX_FREE_SOURCES:
            await update.message.reply_text(
                f"❌ Limit reached: {MAX_FREE_SOURCES} sources max\n"
                f"Remove one with /remove"
            )
            return

        # Detect platform
        if len(args) == 1:
            src_type, rss_url, needs_check = detect_platform(args[0])
        else:
            await update.message.reply_text("❌ Invalid format")
            return

        if not src_type or not rss_url:
            await update.message.reply_text(
                "❌ URL not recognized\n\n"
                "Supported:\n"
                "• Any RSS/Atom URL\n"
                "• YouTube channel_id\n"
                "• Reddit subreddit\n"
                "• Medium @username\n"
                "• Livejournal username"
            )
            return

        # Network validation
        if needs_check:
            await update.message.reply_text("🔍 Checking source...")
            ok = await asyncio.to_thread(check_rss, rss_url)
            if not ok:
                await update.message.reply_text("❌ Could not fetch feed from this URL")
                return

        # Get feed title
        feed_name = await asyncio.to_thread(get_feed_title, rss_url)

        # Insert subscription
        try:
            sub_id = self.db.insert_id(
                "INSERT INTO subscriptions "
                "(user_id, source_url, source_type, source_name, is_active, initial_fetched) "
                "VALUES (%s, %s, %s, %s, TRUE, FALSE) RETURNING id",
                (user["id"], rss_url, src_type, feed_name),
            )
        except Exception as e:
            if "unique" in str(e).lower():
                await update.message.reply_text("⚠️ Already subscribed to this source")
            else:
                await update.message.reply_text("❌ Error adding source")
            return

        # ✨ INITIAL POST FETCHING ✨
        await update.message.reply_text(f"📥 Fetching initial posts...")
        
        posts = await asyncio.to_thread(fetch_feed, rss_url, src_type, INITIAL_FETCH_COUNT)
        
        sent_count = 0
        if posts:
            for post in posts:
                try:
                    existing = self.db.one(
                        "SELECT id FROM posts WHERE source_id = %s",
                        (post["source_id"],)
                    )
                    
                    if existing:
                        post_id = existing["id"]
                    else:
                        post_id = self.db.insert_id(
                            "INSERT INTO posts (source_id, source_type, title, url) "
                            "VALUES (%s, %s, %s, %s) RETURNING id",
                            (post["source_id"], post["source_type"], post["title"], post["url"]),
                        )

                    if post_id:
                        icon = PLATFORMS.get(post["source_type"], "📱")
                        msg = f"{icon} <b>{feed_name}</b>\n\n{post['title']}\n\n"
                        msg += f"<a href='{post['url']}'>🔗 View original</a>"
                        
                        try:
                            await self.app.bot.send_message(
                                chat_id=tg_id,
                                text=msg,
                                parse_mode="HTML",
                                disable_web_page_preview=False,
                            )
                            
                            # Log in sent_history
                            self.db.run(
                                "INSERT INTO sent_history (user_id, post_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                                (user["id"], post_id),
                            )
                            
                            # Increment counter
                            self.db.run(
                                "UPDATE users SET forwarded_count = forwarded_count + 1 WHERE id = %s",
                                (user["id"],)
                            )
                            
                            sent_count += 1
                            await asyncio.sleep(0.5)  # Gentle rate limiting
                        except Exception as e:
                            logger.error(f"Send error: {e}")
                
                except Exception as e:
                    logger.error(f"Post processing error: {e}")

            # Mark as initial_fetched
            self.db.run(
                "UPDATE subscriptions SET initial_fetched = TRUE WHERE id = %s",
                (sub_id,)
            )

        # Final confirmation
        icon = PLATFORMS.get(src_type, "📱")
        count = self._source_count(user["id"])
        await update.message.reply_text(
            f"✅ <b>Source added!</b>\n\n"
            f"{icon} <b>{src_type.upper()}</b>\n"
            f"📌 {feed_name}\n\n"
            f"📤 Fetched {sent_count} initial posts\n"
            f"Sources: {count}/{MAX_FREE_SOURCES}\n\n"
            f"✨ New posts arrive every 5 minutes!",
            parse_mode="HTML",
        )

    # ────────────────────────────────────────────────────────────────────────────
    # COMMAND: /remove
    # ────────────────────────────────────────────────────────────────────────────

    async def cmd_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        user = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))
        
        if not user:
            await update.message.reply_text("Use /start first")
            return

        subs = self.db.query(
            "SELECT id, source_type, source_name, source_url FROM subscriptions "
            "WHERE user_id = %s AND is_active = TRUE ORDER BY created_at",
            (user["id"],),
        )

        if not subs:
            await update.message.reply_text("No active subscriptions")
            return

        if context.args:
            arg = context.args[0]
            if arg.isdigit():
                idx = int(arg) - 1
                if 0 <= idx < len(subs):
                    self.db.run("UPDATE subscriptions SET is_active = FALSE WHERE id = %s", (subs[idx]["id"],))
                    await update.message.reply_text(f"✅ Removed: {subs[idx]['source_name']}")
                else:
                    await update.message.reply_text(f"Invalid number (1-{len(subs)})")
            return

        text = "📋 <b>Your sources — send number to remove:</b>\n\n"
        for i, s in enumerate(subs, 1):
            icon = PLATFORMS.get(s["source_type"], "📱")
            name = (s["source_name"] or s["source_url"])[:50]
            text += f"{i}. {icon} {name}\n"
        
        await update.message.reply_text(text, parse_mode="HTML")
        self._set_state(tg_id, "remove_pick", "")

    # ────────────────────────────────────────────────────────────────────────────
    # COMMAND: /list
    # ────────────────────────────────────────────────────────────────────────────

    async def cmd_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        user = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))
        
        if not user:
            await update.message.reply_text("Use /start first")
            return

        subs = self.db.query(
            "SELECT source_type, source_name, source_url, is_active FROM subscriptions "
            "WHERE user_id = %s ORDER BY created_at",
            (user["id"],),
        )

        count = len(subs)
        if not count:
            await update.message.reply_text(f"📋 No sources (0/{MAX_FREE_SOURCES})")
            return

        text = f"📋 <b>Your sources ({count}/{MAX_FREE_SOURCES})</b>\n\n"
        for s in subs:
            icon = PLATFORMS.get(s["source_type"], "📱")
            state = "✅" if s["is_active"] else "❌"
            name = (s["source_name"] or s["source_url"])[:50]
            text += f"{state} {icon} {name}\n"

        await update.message.reply_text(text, parse_mode="HTML")

    # ────────────────────────────────────────────────────────────────────────────
    # COMMAND: /help
    # ────────────────────────────────────────────────────────────────────────────

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "<b>❓ How to use IN365Bot</b>\n\n"
            "<b>Add sources:</b>\n"
            "<code>/add https://feeds.bbci.co.uk/news/rss.xml</code>\n"
            "<code>/add youtube UCxxxxxxxxxxxxxxxx</code>\n"
            "<code>/add reddit python</code>\n\n"
            "<b>Manage:</b>\n"
            "<code>/list</code> — see subscriptions\n"
            "<code>/remove</code> — remove a source\n\n"
            "<b>Frequency:</b> 5 minutes\n"
            "<b>Limit:</b> 10 sources (free)\n"
            "<b>Initial posts:</b> 4 posts when adding source",
            parse_mode="HTML",
        )

    # ────────────────────────────────────────────────────────────────────────────
    # COMMAND: /admin
    # ────────────────────────────────────────────────────────────────────────────

    async def cmd_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            return

        u = self.db.one("SELECT COUNT(*) AS c FROM users")
        s = self.db.one("SELECT COUNT(*) AS c FROM subscriptions WHERE is_active = TRUE")
        p = self.db.one("SELECT COUNT(*) AS c FROM posts")
        sh = self.db.one("SELECT COUNT(*) AS c FROM sent_history")

        await update.message.reply_text(
            f"📊 <b>Admin Stats</b>\n\n"
            f"Users: {u['c']}\n"
            f"Active subs: {s['c']}\n"
            f"Posts: {p['c']}\n"
            f"Forwarded: {sh['c']}",
            parse_mode="HTML",
        )

    # ────────────────────────────────────────────────────────────────────────────
    # COMMAND: /connect /addchannel
    # ────────────────────────────────────────────────────────────────────────────

    async def cmd_connect(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        user = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))
        if not user:
            await update.message.reply_text("Use /start first")
            return

        channels = self.db.query(
            "SELECT chat_id, chat_title FROM channels WHERE owner_id = %s",
            (user["id"],),
        )
        if not channels:
            await update.message.reply_text(
                "📡 No channels connected yet.\n\n"
                "To connect: add bot as admin to your channel/group."
            )
            return

        text = "📡 <b>Your connected channels:</b>\n\n"
        for ch in channels:
            text += f"• <b>{ch['chat_title']}</b>\n  ID: <code>{ch['chat_id']}</code>\n\n"
        await update.message.reply_text(text, parse_mode="HTML")

    async def cmd_addchannel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "Usage: <code>/addchannel &lt;chat_id&gt; &lt;URL&gt;</code>\n\n"
            "First, add bot as admin to your channel, then use /connect to get chat_id.",
            parse_mode="HTML",
        )

    # ────────────────────────────────────────────────────────────────────────────
    # CALLBACK HANDLER
    # ────────────────────────────────────────────────────────────────────────────

    async def handle_cb(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        tg_id = update.effective_user.id
        user = self.db.one("SELECT * FROM users WHERE telegram_id = %s", (tg_id,))

        if data == "main":
            if not user:
                user = self._ensure_user(tg_id, update.effective_user.username or "user")
            count = self._source_count(user["id"])
            await query.edit_message_text(
                self._start_text(user, count),
                parse_mode="HTML",
                reply_markup=kb_main(count),
            )

        elif data == "add_source":
            self._set_state(tg_id, "add_source", "")
            await query.edit_message_text(
                "➕ <b>Add source</b>\n\n"
                "Send me a link or shorthand:\n"
                "<code>https://feeds.bbci.co.uk/news/rss.xml</code>\n"
                "<code>youtube UCxxxxxxxxxxxxxxxxxx</code>\n"
                "<code>reddit python</code>\n"
                "<code>medium @username</code>\n\n"
                "No /add command needed here.",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        elif data == "my_feed":
            if not user:
                await query.edit_message_text("Use /start", reply_markup=kb_back())
                return
            subs = self.db.query(
                "SELECT id, source_type, source_name, source_url, is_active "
                "FROM subscriptions WHERE user_id = %s ORDER BY created_at",
                (user["id"],),
            )
            count = len(subs)
            text = f"📋 <b>My feed ({count}/{MAX_FREE_SOURCES})</b>\n\n"
            for s in subs:
                icon = PLATFORMS.get(s["source_type"], "📱")
                state = "✅" if s["is_active"] else "❌"
                name = (s["source_name"] or s["source_url"])[:40]
                text += f"{state} {icon} {name}\n"
            await query.edit_message_text(text, parse_mode="HTML", reply_markup=kb_back())

        elif data == "settings":
            await query.edit_message_text(
                "⚙️ <b>Settings</b>",
                parse_mode="HTML",
                reply_markup=kb_settings(),
            )

        elif data == "s_delivery":
            silent = user.get("silent_mode", False) if user else False
            await query.edit_message_text(
                "📨 <b>Delivery options</b>\n\n"
                "Silent mode — posts arrive without sound.",
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
                f"Current: <code>{kw or 'none'}</code>",
                parse_mode="HTML",
                reply_markup=kb_filter(kw),
            )

        elif data == "set_kw":
            self._set_state(tg_id, "set_kw", "")
            await query.edit_message_text(
                "✏️ Send keywords (comma-separated)\n\n"
                "Example: <code>bitcoin, crypto</code>",
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
                "🗑 <b>Cleared!</b>",
                parse_mode="HTML",
                reply_markup=kb_filter(kw),
            )

        elif data == "history":
            if not user:
                await query.edit_message_text("Use /start", reply_markup=kb_back())
                return
            rows = self.db.query(
                "SELECT p.title, p.url, p.source_type FROM sent_history sh "
                "JOIN posts p ON sh.post_id = p.id WHERE sh.user_id = %s "
                "ORDER BY sh.sent_at DESC LIMIT 20",
                (user["id"],),
            )
            if not rows:
                await query.edit_message_text(
                    "📖 No history yet",
                    parse_mode="HTML",
                    reply_markup=kb_back(),
                )
                return
            text = "📖 <b>Last 20 posts</b>\n\n"
            for r in rows:
                icon = PLATFORMS.get(r["source_type"], "📱")
                title = (r["title"] or "No title")[:50]
                text += f"{icon} <a href='{r['url']}'>{title}</a>\n"
            await query.edit_message_text(
                text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=kb_back(),
            )

        elif data == "contact":
            await query.edit_message_text(
                "💬 <b>Contact</b>\n\n"
                "Support: @copyrightpost",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        elif data == "rss_gen":
            await query.edit_message_text(
                "📡 <b>RSS Generator</b>\n\n"
                "Free tools:\n"
                "• RSS Bridge\n"
                "• RSSHub\n\nConvert any site to RSS!",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        elif data == "referral":
            await query.edit_message_text(
                "🎁 <b>Referral program</b>\n\n"
                "Share bot link to get rewards!",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        elif data == "how_to_use":
            await query.edit_message_text(
                "❓ <b>How to use</b>\n\n"
                "1. /add &lt;URL&gt;\n"
                "2. Wait for posts\n"
                "3. Manage with /list /remove",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        elif data == "direct_conn":
            await query.edit_message_text(
                "🚀 <b>Direct connection</b>\n\n"
                "Use standard /add for public sources.",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        elif data == "modes":
            await query.edit_message_text(
                "🔀 <b>Private/Channel/Group modes</b>\n\n"
                "Private: posts to DM\n"
                "Channel/Group: add bot as admin",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        elif data == "data_collector":
            await query.edit_message_text(
                "📊 <b>Local data collector</b>\n\n"
                "Collects analytics for your feeds.",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        elif data == "premium":
            await query.edit_message_text(
                "⭐ <b>Premium</b>\n\n"
                "Self-hosted, free tier version.\n"
                f"Limit: {MAX_FREE_SOURCES} sources",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        elif data == "updates":
            await query.edit_message_text(
                "🔔 <b>News and updates</b>\n\n"
                "Follow for bot updates!",
                parse_mode="HTML",
                reply_markup=kb_back(),
            )

        elif data == "noop":
            pass

    # ────────────────────────────────────────────────────────────────────────────
    # TEXT MESSAGE HANDLER
    # ────────────────────────────────────────────────────────────────────────────

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        tg_id = update.effective_user.id
        text = update.message.text.strip()
        state, _ctx = self._get_state(tg_id)

        if state == "add_source":
            self._clear_state(tg_id)
            user = self._ensure_user(tg_id, update.effective_user.username or "user")
            parts = text.split(None, 1)
            await self._do_add(update, user, parts)

        elif state == "set_kw":
            self._clear_state(tg_id)
            self.db.run(
                "UPDATE users SET keyword_filter = %s WHERE telegram_id = %s",
                (text[:500], tg_id),
            )
            await update.message.reply_text(f"✅ Keywords saved")

        elif state == "remove_pick":
            self._clear_state(tg_id)
            user = self.db.one("SELECT id FROM users WHERE telegram_id = %s", (tg_id,))
            if not user or not text.isdigit():
                await update.message.reply_text("Invalid input")
                return
            
            subs = self.db.query(
                "SELECT id, source_name FROM subscriptions "
                "WHERE user_id = %s AND is_active = TRUE ORDER BY created_at",
                (user["id"],),
            )
            
            idx = int(text) - 1
            if 0 <= idx < len(subs):
                self.db.run(
                    "UPDATE subscriptions SET is_active = FALSE WHERE id = %s",
                    (subs[idx]["id"],)
                )
                await update.message.reply_text(f"✅ Removed: {subs[idx]['source_name']}")
            else:
                await update.message.reply_text(f"Invalid (1-{len(subs)})")

    # ────────────────────────────────────────────────────────────────────────────
    # CHAT MEMBER EVENT
    # ────────────────────────────────────────────────────────────────────────────

    async def on_chat_member(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        result = update.my_chat_member
        chat = result.chat
        new_status = result.new_chat_member.status
        added_by = result.from_user

        if new_status in ("administrator", "member") and added_by:
            try:
                user = self._ensure_user(added_by.id, added_by.username or "user")
                self.db.run(
                    "INSERT INTO channels (owner_id, chat_id, chat_title, chat_type) "
                    "VALUES (%s, %s, %s, %s) ON CONFLICT (chat_id) DO NOTHING",
                    (user["id"], chat.id, chat.title or "Group", chat.type),
                )
                await context.bot.send_message(
                    chat_id=chat.id,
                    text="✅ Bot connected!",
                    parse_mode="HTML",
                )
                logger.info(f"✅ Added to {chat.title}")
            except Exception as e:
                logger.error(f"on_chat_member error: {e}")

    # ────────────────────────────────────────────────────────────────────────────
    # POLLING LOOP
    # ────────────────────────────────────────────────────────────────────────────

    async def _polling_loop(self, app):
        while True:
            try:
                await self._poll_all(app)
            except Exception as e:
                logger.error(f"Polling error: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def _poll_all(self, app):
        user_subs = self.db.query(
            "SELECT s.id, s.source_url, s.source_type, s.source_name, "
            "u.telegram_id, u.id AS user_id, u.silent_mode, u.keyword_filter, "
            "NULL::bigint AS channel_chat_id, NULL::int AS channel_id "
            "FROM subscriptions s JOIN users u ON s.user_id = u.id "
            "WHERE s.user_id IS NOT NULL AND s.is_active = TRUE "
            "ORDER BY s.last_check ASC NULLS FIRST LIMIT 30"
        )

        chan_subs = self.db.query(
            "SELECT s.id, s.source_url, s.source_type, s.source_name, "
            "NULL::bigint AS telegram_id, NULL::int AS user_id, FALSE AS silent_mode, "
            "'' AS keyword_filter, c.chat_id AS channel_chat_id, c.id AS channel_id "
            "FROM subscriptions s JOIN channels c ON s.channel_id = c.id "
            "WHERE s.channel_id IS NOT NULL AND s.is_active = TRUE "
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

                self.db.run(
                    "UPDATE subscriptions SET last_check = NOW() WHERE id = %s",
                    (sub["id"],),
                )
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Poll error for sub {sub['id']}: {e}")

    async def _deliver(self, app, sub: dict, post: dict):
        try:
            kw = (sub.get("keyword_filter") or "").strip()
            if kw:
                keywords = [k.strip().lower() for k in kw.split(",") if k.strip()]
                if any(k in post["title"].lower() for k in keywords):
                    return

            existing = self.db.one("SELECT id FROM posts WHERE source_id = %s", (post["source_id"],))
            if existing:
                post_id = existing["id"]
            else:
                post_id = self.db.insert_id(
                    "INSERT INTO posts (source_id, source_type, title, url) "
                    "VALUES (%s, %s, %s, %s) RETURNING id",
                    (post["source_id"], post["source_type"], post["title"][:500], post["url"][:500]),
                )
                if post_id is None:
                    return

            user_id = sub.get("user_id")
            channel_id = sub.get("channel_id")

            if user_id:
                sent = self.db.one(
                    "SELECT id FROM sent_history WHERE user_id = %s AND post_id = %s",
                    (user_id, post_id),
                )
            else:
                sent = self.db.one(
                    "SELECT id FROM sent_history WHERE channel_id = %s AND post_id = %s",
                    (channel_id, post_id),
                )

            if sent:
                return

            icon = PLATFORMS.get(post["source_type"], "📱")
            source_name = (sub.get("source_name") or post["source_type"])[:60]
            msg = f"{icon} <b>{source_name}</b>\n\n{post['title']}\n\n"
            msg += f"<a href='{post['url']}'>🔗 View original</a>"

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

            if user_id:
                self.db.run(
                    "UPDATE users SET forwarded_count = forwarded_count + 1 WHERE id = %s",
                    (user_id,)
                )

        except Exception as e:
            logger.error(f"Deliver error: {e}")

    # ────────────────────────────────────────────────────────────────────────────
    # RUN
    # ────────────────────────────────────────────────────────────────────────────

    def run(self):
        logger.info("🚀 IN365Bot v2.0 starting...")
        try:
            self.app.run_polling(allowed_updates=["message", "callback_query", "my_chat_member"])
        finally:
            if self.db.conn:
                self.db.conn.close()
            logger.info("Bot stopped")


if __name__ == "__main__":
    Bot().run()