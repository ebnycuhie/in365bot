"""
Aggregator Bot - Production Ready for Railway
Supports: RSS, YouTube, Twitter/X, Instagram (via Picuki), TikTok (via ProxiTok)
Channel/Group posting with private group connection system
"""

import os
import re
import asyncio
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from telegram import Update, Chat
from telegram.ext import Application, CommandHandler, ContextTypes, ChatMemberHandler
import feedparser

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_ID = int(os.getenv("OWNER_ID", "7232714487"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))

if not BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN environment variable not set")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

logger.info(f"Configuration loaded | Owner: {OWNER_ID} | Poll: {POLL_INTERVAL}s")


# ─────────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────────

class DatabaseManager:

    def __init__(self, database_url):
        self.database_url = database_url
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(self.database_url)
            self.conn.autocommit = False
            logger.info("Database connected")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()

    def query(self, sql, params=None):
        """SELECT — returns list"""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                return cur.fetchall()
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB query error: {e}")
                raise

    def query_one(self, sql, params=None):
        """SELECT — returns single row or None"""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                return cur.fetchone()
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB query_one error: {e}")
                raise

    def run(self, sql, params=None):
        """INSERT/UPDATE/DELETE — returns nothing"""
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB run error: {e}")
                raise

    def insert_returning(self, sql, params=None):
        """INSERT RETURNING id — returns the id"""
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
                result = cur.fetchone()
                return result[0] if result else None
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB insert error: {e}")
                raise

    def init_schema(self):
        """Create all tables if they don't exist"""
        schema = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            username VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS channels (
            id SERIAL PRIMARY KEY,
            owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            chat_id BIGINT UNIQUE NOT NULL,
            chat_title VARCHAR(255),
            chat_type VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
            source_url VARCHAR(500) NOT NULL,
            source_type VARCHAR(50) NOT NULL,
            source_name VARCHAR(255),
            last_check TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, source_url),
            UNIQUE(channel_id, source_url)
        );

        CREATE TABLE IF NOT EXISTS posts (
            id SERIAL PRIMARY KEY,
            source_id VARCHAR(500) UNIQUE NOT NULL,
            source_type VARCHAR(50),
            title TEXT,
            url VARCHAR(500),
            published_at TIMESTAMP,
            content_hash VARCHAR(64),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS sent_history (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            channel_id INTEGER REFERENCES channels(id) ON DELETE CASCADE,
            post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON subscriptions(user_id);
        CREATE INDEX IF NOT EXISTS idx_subscriptions_channel ON subscriptions(channel_id);
        CREATE INDEX IF NOT EXISTS idx_posts_source ON posts(source_id);
        CREATE INDEX IF NOT EXISTS idx_sent_user ON sent_history(user_id);
        CREATE INDEX IF NOT EXISTS idx_users_telegram ON users(telegram_id);
        """
        with self.conn.cursor() as cur:
            try:
                cur.execute(schema)
                self.conn.commit()
                logger.info("Schema ready")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Schema error: {e}")
                raise


# ─────────────────────────────────────────────
#  BOT
# ─────────────────────────────────────────────

class AggregatorBot:

    def __init__(self):
        self.db = DatabaseManager(DATABASE_URL)
        self.db.connect()
        self.db.init_schema()
        self.app = Application.builder().token(BOT_TOKEN).build()
        self.polling_task = None
        self.setup_handlers()

    def setup_handlers(self):
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("add", self.cmd_add))
        self.app.add_handler(CommandHandler("addchannel", self.cmd_add_channel_sub))
        self.app.add_handler(CommandHandler("remove", self.cmd_remove))
        self.app.add_handler(CommandHandler("list", self.cmd_list))
        self.app.add_handler(CommandHandler("channels", self.cmd_channels))
        self.app.add_handler(CommandHandler("connect", self.cmd_connect))
        self.app.add_handler(CommandHandler("help", self.cmd_help))
        self.app.add_handler(CommandHandler("admin", self.cmd_admin))
        self.app.add_handler(ChatMemberHandler(self.on_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
        self.app.post_init = self.post_init

    async def post_init(self, app):
        if not self.polling_task:
            self.polling_task = asyncio.create_task(self.polling_loop(app))
            logger.info("Polling engine started")

    # ── Polling ──────────────────────────────

    async def polling_loop(self, app):
        while True:
            try:
                await self.poll_all_sources(app)
            except Exception as e:
                logger.error(f"Polling error: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def poll_all_sources(self, app):
        try:
            # Personal subscriptions
            rows = self.db.query("""
                SELECT s.id, s.source_url, s.source_type, u.telegram_id, u.id as user_id,
                       NULL as channel_chat_id, NULL as channel_id
                FROM subscriptions s
                JOIN users u ON s.user_id = u.id
                WHERE s.user_id IS NOT NULL AND s.channel_id IS NULL
                ORDER BY s.last_check ASC NULLS FIRST
                LIMIT 20
            """)

            # Channel subscriptions
            channel_rows = self.db.query("""
                SELECT s.id, s.source_url, s.source_type, NULL as telegram_id, NULL as user_id,
                       c.chat_id as channel_chat_id, c.id as channel_id
                FROM subscriptions s
                JOIN channels c ON s.channel_id = c.id
                WHERE s.channel_id IS NOT NULL
                ORDER BY s.last_check ASC NULLS FIRST
                LIMIT 20
            """)

            all_rows = list(rows or []) + list(channel_rows or [])
            if not all_rows:
                return

            for row in all_rows:
                try:
                    url = row['source_url']
                    source_type = row['source_type']

                    if source_type == "rss":
                        posts = self.fetch_rss(url)
                    elif source_type == "youtube":
                        posts = self.fetch_youtube(url)
                    elif source_type == "twitter":
                        posts = self.fetch_twitter(url)
                    elif source_type == "instagram":
                        posts = self.fetch_instagram(url)
                    elif source_type == "tiktok":
                        posts = self.fetch_tiktok(url)
                    else:
                        continue

                    for post in posts:
                        await self.process_post(
                            app,
                            row['id'],
                            row['user_id'],
                            row['telegram_id'],
                            row['channel_id'],
                            row['channel_chat_id'],
                            post
                        )

                    self.db.run(
                        "UPDATE subscriptions SET last_check = NOW() WHERE id = %s",
                        (row['id'],)
                    )
                    await asyncio.sleep(2)

                except Exception as e:
                    logger.error(f"Error polling {row.get('source_url')}: {e}")

        except Exception as e:
            logger.error(f"Poll loop error: {e}")

    async def process_post(self, app, sub_id, user_id, telegram_id, channel_id, channel_chat_id, post):
        try:
            existing = self.db.query_one(
                "SELECT id FROM posts WHERE source_id = %s",
                (post['source_id'],)
            )
            if existing:
                post_id = existing['id']
                # Check if already sent to this target
                if user_id:
                    sent = self.db.query_one(
                        "SELECT id FROM sent_history WHERE user_id=%s AND post_id=%s",
                        (user_id, post_id)
                    )
                elif channel_id:
                    sent = self.db.query_one(
                        "SELECT id FROM sent_history WHERE channel_id=%s AND post_id=%s",
                        (channel_id, post_id)
                    )
                else:
                    return
                if sent:
                    return
            else:
                post_id = self.db.insert_returning("""
                    INSERT INTO posts (source_id, source_type, title, url, published_at, content_hash)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    post['source_id'],
                    post['source_type'],
                    post['title'][:255],
                    post['url'][:500],
                    post.get('published_at', datetime.now()),
                    post.get('hash', '')
                ))

            emoji = {'rss': '📰', 'youtube': '▶️', 'twitter': '🐦', 'instagram': '📸', 'tiktok': '🎵'}.get(post['source_type'], '📱')
            text = f"{emoji} <b>{post['source_type'].upper()}</b>\n\n"
            text += f"{post['title']}\n\n"
            text += f"<a href='{post['url']}'>🔗 Open</a>"

            # Send to personal chat or channel/group
            target_chat = telegram_id if telegram_id else channel_chat_id
            if not target_chat:
                return

            await app.bot.send_message(
                chat_id=target_chat,
                text=text,
                parse_mode='HTML'
            )

            self.db.run(
                "INSERT INTO sent_history (user_id, channel_id, post_id) VALUES (%s, %s, %s)",
                (user_id, channel_id, post_id)
            )
            logger.info(f"Sent post {post_id} to {target_chat}")

        except Exception as e:
            logger.error(f"Error processing post: {e}")

    # ── Channel connection ────────────────────

    async def on_chat_member(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Fires when bot is added/removed from a group or channel"""
        result = update.my_chat_member
        chat = result.chat
        new_status = result.new_chat_member.status

        if new_status in ("administrator", "member"):
            # Bot was added — register who added it
            added_by = result.from_user
            if not added_by:
                return
            try:
                # Ensure user exists
                self.db.run(
                    "INSERT INTO users (telegram_id, username) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (added_by.id, added_by.username or "user")
                )
                user_row = self.db.query_one(
                    "SELECT id FROM users WHERE telegram_id = %s", (added_by.id,)
                )
                # Register the channel/group
                self.db.run("""
                    INSERT INTO channels (owner_id, chat_id, chat_title, chat_type)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (chat_id) DO UPDATE SET chat_title = EXCLUDED.chat_title
                """, (user_row['id'], chat.id, chat.title or "Unknown", chat.type))

                await context.bot.send_message(
                    chat_id=chat.id,
                    text=f"✅ <b>Bot connected to {chat.title}!</b>\n\nNow go to your private chat with me and use:\n<code>/addchannel {chat.id} &lt;URL&gt;</code>\nto add sources to this channel.",
                    parse_mode='HTML'
                )
                logger.info(f"Bot added to {chat.title} ({chat.id}) by {added_by.id}")
            except Exception as e:
                logger.error(f"on_chat_member error: {e}")

        elif new_status in ("kicked", "left"):
            # Bot was removed — clean up
            try:
                self.db.run("DELETE FROM channels WHERE chat_id = %s", (chat.id,))
                logger.info(f"Bot removed from {chat.id}, channel deleted")
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def cmd_connect(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show user their connected channels"""
        user_id = update.effective_user.id
        try:
            user_row = self.db.query_one(
                "SELECT id FROM users WHERE telegram_id = %s", (user_id,)
            )
            if not user_row:
                await update.message.reply_text("Use /start first.")
                return

            channels = self.db.query(
                "SELECT chat_id, chat_title, chat_type FROM channels WHERE owner_id = %s",
                (user_row['id'],)
            )

            if not channels:
                text = "📡 <b>No channels/groups connected yet.</b>\n\n"
                text += "To connect a group or channel:\n"
                text += "1. Add this bot as admin to your group/channel\n"
                text += "2. The bot will auto-register it\n"
                text += "3. Use /addchannel &lt;chat_id&gt; &lt;URL&gt; to add sources"
            else:
                text = "📡 <b>Your Connected Channels/Groups:</b>\n\n"
                for ch in channels:
                    text += f"• <b>{ch['chat_title']}</b> [{ch['chat_type']}]\n"
                    text += f"  ID: <code>{ch['chat_id']}</code>\n\n"
                text += "Use: /addchannel &lt;chat_id&gt; &lt;URL&gt; to add sources"

            await update.message.reply_text(text, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Connect error: {e}")
            await update.message.reply_text("Error.")

    async def cmd_channels(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Alias for /connect"""
        await self.cmd_connect(update, context)

    async def cmd_add_channel_sub(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add a source to a channel: /addchannel <chat_id> <url>"""
        if len(context.args) < 2:
            await update.message.reply_text(
                "Usage: /addchannel &lt;chat_id&gt; &lt;URL&gt;\n\n"
                "Get your chat_id from /connect",
                parse_mode='HTML'
            )
            return

        user_id = update.effective_user.id
        try:
            chat_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Invalid chat_id — must be a number.")
            return

        url = context.args[1]
        source_type = self.detect_type(url)
        if not source_type:
            await update.message.reply_text("URL not supported. Use RSS, YouTube, Twitter, Instagram, or TikTok.")
            return

        try:
            user_row = self.db.query_one(
                "SELECT id FROM users WHERE telegram_id = %s", (user_id,)
            )
            if not user_row:
                await update.message.reply_text("Use /start first.")
                return

            channel_row = self.db.query_one(
                "SELECT id FROM channels WHERE chat_id = %s AND owner_id = %s",
                (chat_id, user_row['id'])
            )
            if not channel_row:
                await update.message.reply_text(
                    "❌ Channel not found or you're not the owner.\n"
                    "Make sure you added the bot as admin first."
                )
                return

            name = url[:100]
            if source_type == "rss":
                try:
                    feed = feedparser.parse(url)
                    name = feed.feed.get('title', url)[:100]
                except Exception:
                    pass

            self.db.run(
                "INSERT INTO subscriptions (channel_id, source_url, source_type, source_name) VALUES (%s, %s, %s, %s)",
                (channel_row['id'], url, source_type, name)
            )
            await update.message.reply_text(f"✅ Added {source_type.upper()} to channel — {name}")

        except psycopg2.IntegrityError:
            self.db.conn.rollback()
            await update.message.reply_text("Already subscribed to this source in that channel.")
        except Exception as e:
            logger.error(f"addchannel error: {e}")
            await update.message.reply_text("Error adding source to channel.")

    # ── User commands ─────────────────────────

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        username = update.effective_user.username or "user"
        try:
            self.db.run(
                "INSERT INTO users (telegram_id, username) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (user_id, username)
            )
            text = "🤖 <b>Content Aggregator Bot</b>\n\n"
            text += "Collect updates from RSS, YouTube, Twitter, Instagram, TikTok.\n\n"
            text += "<b>Personal commands:</b>\n"
            text += "/add &lt;URL&gt; — Add source to your DM\n"
            text += "/list — Your subscriptions\n"
            text += "/remove &lt;URL&gt; — Remove source\n\n"
            text += "<b>Channel/Group commands:</b>\n"
            text += "/connect — See connected channels\n"
            text += "/addchannel &lt;chat_id&gt; &lt;URL&gt; — Add source to channel\n\n"
            text += "/help — Full help"
            await update.message.reply_text(text, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Start error: {e}")
            await update.message.reply_text("Welcome! Use /help to see commands.")

    async def cmd_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /add &lt;URL&gt;", parse_mode='HTML')
            return

        url = context.args[0]
        user_id = update.effective_user.id
        source_type = self.detect_type(url)

        if not source_type:
            await update.message.reply_text(
                "URL not supported.\n\nSupported: RSS, YouTube, Twitter/X, Instagram, TikTok"
            )
            return

        name = url[:100]
        if source_type == "rss":
            try:
                feed = feedparser.parse(url)
                name = feed.feed.get('title', url)[:100]
            except Exception:
                pass

        try:
            user_row = self.db.query_one(
                "SELECT id FROM users WHERE telegram_id = %s", (user_id,)
            )
            if not user_row:
                self.db.run(
                    "INSERT INTO users (telegram_id, username) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (user_id, update.effective_user.username or "user")
                )
                user_row = self.db.query_one(
                    "SELECT id FROM users WHERE telegram_id = %s", (user_id,)
                )

            self.db.run(
                "INSERT INTO subscriptions (user_id, source_url, source_type, source_name) VALUES (%s, %s, %s, %s)",
                (user_row['id'], url, source_type, name)
            )
            await update.message.reply_text(f"✅ Added {source_type.upper()} — {name}")

        except psycopg2.IntegrityError:
            self.db.conn.rollback()
            await update.message.reply_text("Already subscribed to this source.")
        except Exception as e:
            logger.error(f"Add error: {e}")
            await update.message.reply_text("Error adding source.")

    async def cmd_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /remove &lt;URL&gt;", parse_mode='HTML')
            return

        url = context.args[0]
        user_id = update.effective_user.id
        try:
            user_row = self.db.query_one(
                "SELECT id FROM users WHERE telegram_id = %s", (user_id,)
            )
            if user_row:
                self.db.run(
                    "DELETE FROM subscriptions WHERE user_id = %s AND source_url = %s",
                    (user_row['id'], url)
                )
                await update.message.reply_text("✅ Removed.")
            else:
                await update.message.reply_text("Source not found.")
        except Exception as e:
            logger.error(f"Remove error: {e}")
            await update.message.reply_text("Error removing source.")

    async def cmd_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        try:
            user_row = self.db.query_one(
                "SELECT id FROM users WHERE telegram_id = %s", (user_id,)
            )
            if not user_row:
                await update.message.reply_text("No subscriptions yet. Use /add to subscribe.")
                return

            rows = self.db.query(
                "SELECT source_type, source_url, source_name FROM subscriptions WHERE user_id = %s ORDER BY created_at DESC",
                (user_row['id'],)
            )

            if not rows:
                await update.message.reply_text("No subscriptions yet. Use /add to subscribe.")
                return

            text = f"📋 <b>Your Subscriptions ({len(rows)})</b>\n\n"
            for row in rows:
                emoji = {'rss': '📰', 'youtube': '▶️', 'twitter': '🐦', 'instagram': '📸', 'tiktok': '🎵'}.get(row['source_type'], '📱')
                text += f"{emoji} <b>{row['source_type'].upper()}</b> — {row['source_name'][:40]}\n"

            await update.message.reply_text(text, parse_mode='HTML')
        except Exception as e:
            logger.error(f"List error: {e}")
            await update.message.reply_text("Error fetching subscriptions.")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = """<b>📚 How to Use</b>

<b>Personal subscriptions (DM):</b>
/add &lt;URL&gt; — Subscribe to a source
/remove &lt;URL&gt; — Unsubscribe
/list — Your subscriptions

<b>Channel/Group posting:</b>
1. Add this bot as admin to your channel/group
2. The bot will auto-register it
3. /connect — See your connected channels
4. /addchannel &lt;chat_id&gt; &lt;URL&gt; — Post to channel

<b>Supported sources:</b>
📰 RSS: /add https://feeds.bbci.co.uk/news/rss.xml
▶️ YouTube: /add https://youtube.com/@ChannelName
🐦 Twitter/X: /add https://twitter.com/username
📸 Instagram: /add https://www.instagram.com/username/
🎵 TikTok: /add https://www.tiktok.com/@username

Updates are checked every 5 minutes.
"""
        await update.message.reply_text(text, parse_mode='HTML')

    async def cmd_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("Access denied.")
            return
        try:
            users = self.db.query_one("SELECT COUNT(*) as c FROM users")
            subs = self.db.query_one("SELECT COUNT(*) as c FROM subscriptions")
            channels = self.db.query_one("SELECT COUNT(*) as c FROM channels")
            posts = self.db.query_one("SELECT COUNT(*) as c FROM posts")
            sent = self.db.query_one("SELECT COUNT(*) as c FROM sent_history")

            text = f"📊 <b>Bot Stats</b>\n\n"
            text += f"Users: {users['c']}\n"
            text += f"Subscriptions: {subs['c']}\n"
            text += f"Connected channels: {channels['c']}\n"
            text += f"Posts tracked: {posts['c']}\n"
            text += f"Messages sent: {sent['c']}\n"
            await update.message.reply_text(text, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Admin error: {e}")
            await update.message.reply_text("Error fetching stats.")

    # ── Source detection ──────────────────────

    def detect_type(self, url: str) -> str:
        url_lower = url.lower()
        if "youtube.com" in url_lower or "youtu.be" in url_lower:
            return "youtube"
        elif "twitter.com" in url_lower or "x.com" in url_lower:
            return "twitter"
        elif "instagram.com" in url_lower:
            return "instagram"
        elif "tiktok.com" in url_lower:
            return "tiktok"
        elif url_lower.endswith(".xml") or "rss" in url_lower or "feed" in url_lower:
            return "rss"
        return None

    # ── Fetchers ──────────────────────────────

    def fetch_rss(self, url: str) -> list:
        posts = []
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:5]:
                posts.append({
                    'source_id': entry.get('id', entry.get('link', '')),
                    'source_type': 'rss',
                    'title': entry.get('title', 'No title')[:255],
                    'url': entry.get('link', ''),
                    'published_at': datetime.now(),
                    'hash': str(hash(entry.get('summary', '')))
                })
        except Exception as e:
            logger.error(f"RSS error: {e}")
        return posts

    def fetch_youtube(self, url: str) -> list:
        posts = []
        try:
            # Handle @handle format
            match = re.search(r'youtube\.com/@([a-zA-Z0-9_-]+)', url)
            if match:
                handle = match.group(1)
                rss_url = f"https://www.youtube.com/feeds/videos.xml?user={handle}"
                feed = feedparser.parse(rss_url)
                if not feed.entries:
                    # Try channel_id format via search
                    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={handle}"
                    feed = feedparser.parse(rss_url)
            else:
                match = re.search(r'youtube\.com/(?:c|user|channel)/([a-zA-Z0-9_-]+)', url)
                if match:
                    cid = match.group(1)
                    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={cid}"
                    feed = feedparser.parse(rss_url)
                    if not feed.entries:
                        rss_url = f"https://www.youtube.com/feeds/videos.xml?user={cid}"
                        feed = feedparser.parse(rss_url)
                else:
                    return []

            for entry in feed.entries[:5]:
                posts.append({
                    'source_id': entry.get('id', ''),
                    'source_type': 'youtube',
                    'title': entry.get('title', 'Video')[:255],
                    'url': entry.get('link', ''),
                    'published_at': datetime.now(),
                    'hash': entry.get('id', '')
                })
        except Exception as e:
            logger.error(f"YouTube error: {e}")
        return posts

    def fetch_twitter(self, url: str) -> list:
        posts = []
        try:
            match = re.search(r'(?:twitter|x)\.com/([a-zA-Z0-9_]+)', url)
            if not match:
                return []
            username = match.group(1)
            # Try multiple Nitter instances
            for instance in ["https://nitter.net", "https://nitter.privacydev.net", "https://nitter.poast.org"]:
                try:
                    rss_url = f"{instance}/{username}/rss"
                    feed = feedparser.parse(rss_url)
                    if feed.entries:
                        break
                except Exception:
                    continue

            for entry in feed.entries[:5]:
                posts.append({
                    'source_id': entry.get('id', ''),
                    'source_type': 'twitter',
                    'title': entry.get('title', 'Tweet')[:255],
                    'url': entry.get('link', ''),
                    'published_at': datetime.now(),
                    'hash': entry.get('id', '')
                })
        except Exception as e:
            logger.error(f"Twitter error: {e}")
        return posts

    def fetch_instagram(self, url: str) -> list:
        posts = []
        try:
            match = re.search(r'instagram\.com/([a-zA-Z0-9_.]+)', url)
            if not match:
                return []
            username = match.group(1).rstrip('/')
            # Use Picuki as RSS bridge
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            resp = requests.get(f"https://www.picuki.com/profile/{username}", headers=headers, timeout=10)
            titles = re.findall(r'<div class="photo-description">(.*?)</div>', resp.text, re.DOTALL)
            links = re.findall(r'href="(https://www\.picuki\.com/media/[^"]+)"', resp.text)
            for i, (title, link) in enumerate(zip(titles[:5], links[:5])):
                clean = re.sub(r'<[^>]+>', '', title).strip()[:255] or f"Instagram post by @{username}"
                posts.append({
                    'source_id': f"ig_{username}_{link.split('/')[-1]}",
                    'source_type': 'instagram',
                    'title': clean,
                    'url': link,
                    'published_at': datetime.now(),
                    'hash': link
                })
        except Exception as e:
            logger.error(f"Instagram error: {e}")
        return posts

    def fetch_tiktok(self, url: str) -> list:
        posts = []
        try:
            match = re.search(r'tiktok\.com/@([a-zA-Z0-9_.]+)', url)
            if not match:
                return []
            username = match.group(1)
            # ProxiTok is a public TikTok RSS proxy
            rss_url = f"https://proxitok.pabloferreiro.es/@{username}/rss"
            feed = feedparser.parse(rss_url)
            if not feed.entries:
                # Fallback instance
                rss_url = f"https://proxitok.privacyredirect.com/@{username}/rss"
                feed = feedparser.parse(rss_url)
            for entry in feed.entries[:5]:
                posts.append({
                    'source_id': entry.get('id', entry.get('link', '')),
                    'source_type': 'tiktok',
                    'title': entry.get('title', f'TikTok by @{username}')[:255],
                    'url': entry.get('link', ''),
                    'published_at': datetime.now(),
                    'hash': entry.get('id', '')
                })
        except Exception as e:
            logger.error(f"TikTok error: {e}")
        return posts

    def run(self):
        logger.info("Starting bot...")
        try:
            self.app.run_polling(allowed_updates=['message', 'callback_query', 'my_chat_member'])
        finally:
            self.db.close()


if __name__ == "__main__":
    bot = AggregatorBot()
    bot.run()