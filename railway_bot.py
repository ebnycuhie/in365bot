"""
Aggregator Bot - Production Ready for Railway
Uses polling mode (no webhooks needed)
Supports: RSS, YouTube, Twitter
"""

import os
import asyncio
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
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

logger.info("Configuration loaded")
logger.info(f"Owner: {OWNER_ID}")
logger.info(f"Poll interval: {POLL_INTERVAL}s")


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
        """For SELECT - returns list of rows"""
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
        """For SELECT - returns single row or None"""
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
        """For INSERT/UPDATE/DELETE - returns nothing"""
        with self.conn.cursor() as cur:
            try:
                cur.execute(sql, params or ())
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB run error: {e}")
                raise

    def insert_returning(self, sql, params=None):
        """For INSERT RETURNING id - returns the id"""
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


class AggregatorBot:

    def __init__(self):
        self.db = DatabaseManager(DATABASE_URL)
        self.db.connect()
        self.app = Application.builder().token(BOT_TOKEN).build()
        self.polling_task = None
        self.setup_handlers()

    def setup_handlers(self):
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("add", self.cmd_add))
        self.app.add_handler(CommandHandler("remove", self.cmd_remove))
        self.app.add_handler(CommandHandler("list", self.cmd_list))
        self.app.add_handler(CommandHandler("help", self.cmd_help))
        self.app.add_handler(CommandHandler("admin", self.cmd_admin))
        self.app.post_init = self.post_init

    async def post_init(self, app):
        if not self.polling_task:
            self.polling_task = asyncio.create_task(self.polling_loop(app))
            logger.info("Polling engine started")

    async def polling_loop(self, app):
        while True:
            try:
                await self.poll_all_sources(app)
            except Exception as e:
                logger.error(f"Polling error: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def poll_all_sources(self, app):
        try:
            rows = self.db.query("""
                SELECT s.id, s.source_url, s.source_type, u.telegram_id, u.id as user_id
                FROM subscriptions s
                JOIN users u ON s.user_id = u.id
                ORDER BY s.last_check ASC NULLS FIRST
                LIMIT 10
            """)

            if not rows:
                return

            for row in rows:
                try:
                    sub_id = row['id']
                    url = row['source_url']
                    source_type = row['source_type']
                    telegram_id = row['telegram_id']
                    user_id = row['user_id']

                    if source_type == "rss":
                        posts = self.fetch_rss(url)
                    elif source_type == "youtube":
                        posts = self.fetch_youtube(url)
                    elif source_type == "twitter":
                        posts = self.fetch_twitter(url)
                    else:
                        continue

                    for post in posts:
                        await self.process_post(app, sub_id, user_id, telegram_id, post)

                    self.db.run(
                        "UPDATE subscriptions SET last_check = NOW() WHERE id = %s",
                        (sub_id,)
                    )
                    await asyncio.sleep(2)

                except Exception as e:
                    logger.error(f"Error polling {row.get('source_url')}: {e}")

        except Exception as e:
            logger.error(f"Poll loop error: {e}")

    async def process_post(self, app, sub_id, user_id, telegram_id, post):
        try:
            existing = self.db.query_one(
                "SELECT id FROM posts WHERE source_id = %s",
                (post['source_id'],)
            )
            if existing:
                return

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

            text = f"📱 <b>{post['source_type'].upper()}</b>\n\n"
            text += f"{post['title']}\n\n"
            text += f"<a href='{post['url']}'>🔗 Open</a>"

            await app.bot.send_message(
                chat_id=telegram_id,
                text=text,
                parse_mode='HTML'
            )

            self.db.run(
                "INSERT INTO sent_history (user_id, post_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (user_id, post_id)
            )
            logger.info(f"Sent post {post_id} to {telegram_id}")

        except Exception as e:
            logger.error(f"Error processing post: {e}")

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        username = update.effective_user.username or "user"
        try:
            self.db.run(
                "INSERT INTO users (telegram_id, username) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (user_id, username)
            )
            text = "🤖 <b>Content Aggregator Bot</b>\n\n"
            text += "Collect updates from RSS, YouTube, Twitter.\n\n"
            text += "<b>Commands:</b>\n"
            text += "/add &lt;URL&gt; - Add source\n"
            text += "/list - Your subscriptions\n"
            text += "/remove &lt;URL&gt; - Remove source\n"
            text += "/help - Help\n"
            await update.message.reply_text(text, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error in start: {e}")
            await update.message.reply_text("Welcome! Use /help to see commands.")

    async def cmd_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /add <URL>")
            return

        url = context.args[0]
        user_id = update.effective_user.id
        source_type = self.detect_type(url)

        if not source_type:
            await update.message.reply_text("URL not supported. Use RSS, YouTube, or Twitter links.")
            return

        try:
            if source_type == "rss":
                feed = feedparser.parse(url)
                name = feed.feed.get('title', url)[:100]
            else:
                name = url[:100]
        except Exception:
            await update.message.reply_text("Cannot access URL.")
            return

        try:
            user_row = self.db.query_one(
                "SELECT id FROM users WHERE telegram_id = %s",
                (user_id,)
            )
            if not user_row:
                self.db.run(
                    "INSERT INTO users (telegram_id, username) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (user_id, update.effective_user.username or "user")
                )
                user_row = self.db.query_one(
                    "SELECT id FROM users WHERE telegram_id = %s",
                    (user_id,)
                )

            self.db.run(
                "INSERT INTO subscriptions (user_id, source_url, source_type, source_name) VALUES (%s, %s, %s, %s)",
                (user_row['id'], url, source_type, name)
            )
            await update.message.reply_text(f"Added {source_type.upper()} — {name}")

        except psycopg2.IntegrityError:
            self.db.conn.rollback()
            await update.message.reply_text("Already subscribed to this source.")
        except Exception as e:
            logger.error(f"Error adding source: {e}")
            await update.message.reply_text("Error adding source.")

    async def cmd_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("Usage: /remove <URL>")
            return

        url = context.args[0]
        user_id = update.effective_user.id

        try:
            user_row = self.db.query_one(
                "SELECT id FROM users WHERE telegram_id = %s",
                (user_id,)
            )
            if user_row:
                self.db.run(
                    "DELETE FROM subscriptions WHERE user_id = %s AND source_url = %s",
                    (user_row['id'], url)
                )
                await update.message.reply_text("Removed.")
            else:
                await update.message.reply_text("Source not found.")
        except Exception as e:
            logger.error(f"Error removing: {e}")
            await update.message.reply_text("Error removing source.")

    async def cmd_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        try:
            user_row = self.db.query_one(
                "SELECT id FROM users WHERE telegram_id = %s",
                (user_id,)
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
                text += f"<b>{row['source_type'].upper()}</b> — {row['source_name'][:40]}\n"

            await update.message.reply_text(text, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error listing: {e}")
            await update.message.reply_text("Error fetching subscriptions.")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = """<b>How to Use</b>

/add &lt;URL&gt; — Subscribe to a source
/remove &lt;URL&gt; — Unsubscribe
/list — Your subscriptions
/help — This message

<b>Supported sources:</b>
RSS feeds: /add https://feeds.bbci.co.uk/news/rss.xml
YouTube: /add https://youtube.com/c/ChannelName
Twitter: /add https://twitter.com/username

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
            posts = self.db.query_one("SELECT COUNT(*) as c FROM posts")
            sent = self.db.query_one("SELECT COUNT(*) as c FROM sent_history")

            text = f"📊 <b>Bot Stats</b>\n\n"
            text += f"Users: {users['c']}\n"
            text += f"Subscriptions: {subs['c']}\n"
            text += f"Posts tracked: {posts['c']}\n"
            text += f"Messages sent: {sent['c']}\n"
            await update.message.reply_text(text, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Admin error: {e}")
            await update.message.reply_text("Error fetching stats.")

    def detect_type(self, url: str) -> str:
        url_lower = url.lower()
        if "youtube.com" in url_lower or "youtu.be" in url_lower:
            return "youtube"
        elif "twitter.com" in url_lower or "x.com" in url_lower:
            return "twitter"
        elif url_lower.endswith(".xml") or "rss" in url_lower or "feed" in url_lower:
            return "rss"
        return None

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
        import re
        try:
            match = re.search(r'(?:youtube\.com/(?:c|@|user|channel)/|youtu\.be/)([a-zA-Z0-9_-]+)', url)
            if not match:
                return []
            channel_id = match.group(1)
            rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
            feed = feedparser.parse(rss_url)
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
        import re
        try:
            match = re.search(r'twitter\.com/([a-zA-Z0-9_]+)', url)
            if not match:
                return []
            username = match.group(1)
            rss_url = f"https://nitter.net/{username}/rss"
            feed = feedparser.parse(rss_url)
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

    def run(self):
        logger.info("Starting bot...")
        try:
            self.app.run_polling(allowed_updates=['message', 'callback_query'])
        finally:
            self.db.close()


if __name__ == "__main__":
    bot = AggregatorBot()
    bot.run()