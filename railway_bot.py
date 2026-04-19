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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get environment variables (Railway injects these)
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")  # Railway auto-injects this for PostgreSQL
OWNER_ID = int(os.getenv("OWNER_ID", "7232714487"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))

# Validate required env vars
if not BOT_TOKEN:
    raise ValueError("❌ TELEGRAM_BOT_TOKEN environment variable not set")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL environment variable not set")

logger.info(f"✅ Configuration loaded")
logger.info(f"   Token: {'***' + BOT_TOKEN[-4:]}")
logger.info(f"   Owner: {OWNER_ID}")
logger.info(f"   Poll interval: {POLL_INTERVAL}s")


class DatabaseManager:
    """Handle all database operations safely"""
    
    def __init__(self, database_url):
        self.database_url = database_url
        self.conn = None
    
    def connect(self):
        """Establish connection"""
        try:
            self.conn = psycopg2.connect(self.database_url)
            logger.info("✅ Database connected")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def close(self):
        """Close connection"""
        if self.conn:
            self.conn.close()
    
    def execute(self, query, params=None):
        """Execute query and return results"""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute(query, params or ())
            self.conn.commit()
            return cursor.fetchall()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"DB Error: {e}")
            raise
        finally:
            cursor.close()
    
    def execute_single(self, query, params=None):
        """Execute query and return single row"""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute(query, params or ())
            self.conn.commit()
            return cursor.fetchone()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"DB Error: {e}")
            raise
        finally:
            cursor.close()
    
    def insert(self, query, params=None):
        """Insert and return ID"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params or ())
            self.conn.commit()
            # If query has RETURNING id, fetch it
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            self.conn.rollback()
            logger.error(f"DB Error: {e}")
            raise
        finally:
            cursor.close()


class AggregatorBot:
    """Main bot class"""
    
    def __init__(self):
        self.db = DatabaseManager(DATABASE_URL)
        self.db.connect()
        self.app = Application.builder().token(BOT_TOKEN).build()
        self.polling_task = None
        self.setup_handlers()
    
    def setup_handlers(self):
        """Register all command handlers"""
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("add", self.cmd_add))
        self.app.add_handler(CommandHandler("remove", self.cmd_remove))
        self.app.add_handler(CommandHandler("list", self.cmd_list))
        self.app.add_handler(CommandHandler("help", self.cmd_help))
        self.app.add_handler(CommandHandler("admin", self.cmd_admin))
        
        # Post-init callback
        self.app.post_init = self.post_init
    
    async def post_init(self, app):
        """Start polling after bot init"""
        if not self.polling_task:
            self.polling_task = asyncio.create_task(self.polling_loop(app))
            logger.info("🔄 Polling engine started")
    
    async def polling_loop(self, app):
        """Background polling - ONLY runs once per bot instance"""
        while True:
            try:
                await self.poll_all_sources(app)
            except Exception as e:
                logger.error(f"Polling error: {e}")
            
            await asyncio.sleep(POLL_INTERVAL)
    
    async def poll_all_sources(self, app):
        """Check all subscriptions for new posts"""
        try:
            # Get all subscriptions
            rows = self.db.execute("""
                SELECT s.id, s.source_url, s.source_type, u.telegram_id, u.id as user_id
                FROM subscriptions s
                JOIN users u ON s.user_id = u.id
                ORDER BY s.last_check ASC NULLS FIRST
                LIMIT 10
            """)
            
            if not rows:
                return
            
            for row in rows:
                sub_id = row['id']
                url = row['source_url']
                source_type = row['source_type']
                telegram_id = row['telegram_id']
                user_id = row['user_id']
                
                try:
                    # Fetch posts
                    if source_type == "rss":
                        posts = self.fetch_rss(url)
                    elif source_type == "youtube":
                        posts = self.fetch_youtube(url)
                    elif source_type == "twitter":
                        posts = self.fetch_twitter(url)
                    else:
                        continue
                    
                    # Send new posts
                    for post in posts:
                        await self.process_post(app, sub_id, user_id, telegram_id, post)
                    
                    # Update last check
                    self.db.execute(
                        "UPDATE subscriptions SET last_check = NOW() WHERE id = %s",
                        (sub_id,)
                    )
                    
                    await asyncio.sleep(2)  # Don't hammer Telegram API
                
                except Exception as e:
                    logger.error(f"Error polling {url}: {e}")
        
        except Exception as e:
            logger.error(f"Poll loop error: {e}")
    
    async def process_post(self, app, sub_id, user_id, telegram_id, post):
        """Save post and send to user if new"""
        try:
            # Check if post exists
            existing = self.db.execute_single(
                "SELECT id FROM posts WHERE source_id = %s",
                (post['source_id'],)
            )
            
            if existing:
                return  # Already exists
            
            # Insert post
            post_id = self.db.insert("""
                INSERT INTO posts 
                (source_id, source_type, title, url, published_at, content_hash)
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
            
            # Send message
            text = f"📱 <b>{post['source_type'].upper()}</b>\n\n"
            text += f"{post['title']}\n\n"
            text += f"<a href='{post['url']}'>🔗 Open</a>"
            
            await app.bot.send_message(
                chat_id=telegram_id,
                text=text,
                parse_mode='HTML'
            )
            
            # Log sent
            self.db.execute(
                "INSERT INTO sent_history (user_id, post_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (user_id, post_id)
            )
            
            logger.info(f"✅ Sent post {post_id} to {telegram_id}")
        
        except Exception as e:
            logger.error(f"Error processing post: {e}")
    
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Register user"""
        user_id = update.effective_user.id
        username = update.effective_user.username or "user"
        
        try:
            self.db.execute(
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
            await update.message.reply_text("❌ Error")
    
    async def cmd_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Add source"""
        if not context.args:
            await update.message.reply_text("Usage: /add <URL>")
            return
        
        url = context.args[0]
        user_id = update.effective_user.id
        
        # Detect type
        source_type = self.detect_type(url)
        if not source_type:
            await update.message.reply_text("❌ URL not supported (RSS, YouTube, Twitter)")
            return
        
        # Validate
        try:
            if source_type == "rss":
                feed = feedparser.parse(url)
                if feed.bozo:
                    await update.message.reply_text("❌ Invalid RSS")
                    return
                name = feed.feed.get('title', url)[:100]
            else:
                name = url[:100]
        except:
            await update.message.reply_text("❌ Cannot access URL")
            return
        
        # Add to DB
        try:
            # Get user internal ID
            user_row = self.db.execute_single(
                "SELECT id FROM users WHERE telegram_id = %s",
                (user_id,)
            )
            
            if not user_row:
                await update.message.reply_text("❌ User not found")
                return
            
            self.db.execute(
                "INSERT INTO subscriptions (user_id, source_url, source_type, source_name) VALUES (%s, %s, %s, %s)",
                (user_row['id'], url, source_type, name)
            )
            
            await update.message.reply_text(f"✅ Added {source_type.upper()}")
        
        except psycopg2.IntegrityError:
            self.db.conn.rollback()
            await update.message.reply_text("⚠️ Already subscribed")
        except Exception as e:
            logger.error(f"Error adding source: {e}")
            await update.message.reply_text("❌ Error")
    
    async def cmd_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Remove source"""
        if not context.args:
            await update.message.reply_text("Usage: /remove <URL>")
            return
        
        url = context.args[0]
        user_id = update.effective_user.id
        
        try:
            user_row = self.db.execute_single(
                "SELECT id FROM users WHERE telegram_id = %s",
                (user_id,)
            )
            
            if user_row:
                self.db.execute(
                    "DELETE FROM subscriptions WHERE user_id = %s AND source_url = %s",
                    (user_row['id'], url)
                )
                await update.message.reply_text("✅ Removed")
            else:
                await update.message.reply_text("❌ Not found")
        except Exception as e:
            logger.error(f"Error removing: {e}")
            await update.message.reply_text("❌ Error")
    
    async def cmd_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List subscriptions"""
        user_id = update.effective_user.id
        
        try:
            user_row = self.db.execute_single(
                "SELECT id FROM users WHERE telegram_id = %s",
                (user_id,)
            )
            
            if not user_row:
                await update.message.reply_text("User not found")
                return
            
            rows = self.db.execute(
                "SELECT source_type, source_url, source_name FROM subscriptions WHERE user_id = %s ORDER BY created_at DESC",
                (user_row['id'],)
            )
            
            if not rows:
                await update.message.reply_text("No subscriptions")
                return
            
            text = f"📋 <b>Subscriptions ({len(rows)})</b>\n\n"
            for row in rows:
                text += f"<b>{row['source_type'].upper()}</b>\n"
                text += f"{row['source_name'][:30]}\n\n"
            
            await update.message.reply_text(text, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error listing: {e}")
            await update.message.reply_text("❌ Error")
    
    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show help"""
        text = """<b>📚 How to Use</b>

/add &lt;URL&gt; - Subscribe
/remove &lt;URL&gt; - Unsubscribe  
/list - Your sources
/help - This message

<b>Supported:</b>
✅ RSS: /add https://example.com/feed.xml
✅ YouTube: /add https://youtube.com/c/ChannelName
✅ Twitter: /add https://twitter.com/username

Updates every 5 minutes.
"""
        await update.message.reply_text(text, parse_mode='HTML')
    
    async def cmd_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin stats"""
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("❌ Access denied")
            return
        
        try:
            users = self.db.execute_single("SELECT COUNT(*) as c FROM users")
            subs = self.db.execute_single("SELECT COUNT(*) as c FROM subscriptions")
            posts = self.db.execute_single("SELECT COUNT(*) as c FROM posts")
            sent = self.db.execute_single("SELECT COUNT(*) as c FROM sent_history")
            
            text = f"""📊 <b>Bot Stats</b>

👥 Users: {users['c']}
📌 Subs: {subs['c']}
📰 Posts: {posts['c']}
✉️ Sent: {sent['c']}
"""
            await update.message.reply_text(text, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Admin error: {e}")
            await update.message.reply_text("❌ Error")
    
    def detect_type(self, url: str) -> str:
        """Detect source type"""
        url_lower = url.lower()
        if "youtube.com" in url_lower or "youtu.be" in url_lower:
            return "youtube"
        elif "twitter.com" in url_lower or "x.com" in url_lower:
            return "twitter"
        elif url_lower.endswith(".xml") or "rss" in url_lower or "feed" in url_lower:
            return "rss"
        return None
    
    def fetch_rss(self, url: str) -> list:
        """Fetch RSS posts"""
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
        """Fetch YouTube posts via RSS"""
        posts = []
        import re
        try:
            # Extract channel ID
            match = re.search(r'(?:youtube\.com/(?:c|@|user|channel)/|youtu\.be/)([a-zA-Z0-9_-]+)', url)
            if not match:
                return []
            
            channel_id = match.group(1)
            rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
            
            feed = feedparser.parse(rss_url)
            if feed.bozo:
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
        """Fetch Twitter posts via Nitter RSS"""
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
        """Start bot"""
        logger.info("🚀 Starting bot...")
        try:
            self.app.run_polling(allowed_updates=['message', 'callback_query'])
        finally:
            self.db.close()


if __name__ == "__main__":
    bot = AggregatorBot()
    bot.run()
