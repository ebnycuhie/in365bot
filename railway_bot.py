"""
IN365Bot — AximoBot-style Content Aggregator
Supports: RSS, YouTube, Twitter/X, Instagram, TikTok, Reddit, Medium,
          Telegram, Threads, Facebook, Twitch, VK, Livejournal, OK.ru, Coub, Likee
"""

import os
import re
import asyncio
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import feedparser
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    ChatMemberHandler, CallbackQueryHandler
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL  = os.getenv("DATABASE_URL")
OWNER_ID      = int(os.getenv("OWNER_ID", "7232714487"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "300"))
MAX_FREE      = 10   # free source limit per user

if not BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN not set")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set")

# ─────────────────────────────────────────────
# PLATFORM HELPERS
# ─────────────────────────────────────────────

EMOJI = {
    'rss': '📰', 'youtube': '▶️', 'twitter': '🐦', 'instagram': '📸',
    'tiktok': '🎵', 'reddit': '🤖', 'telegram': '✈️', 'medium': '📝',
    'threads': '🧵', 'facebook': '👤', 'twitch': '🎮', 'vk': '💬',
    'livejournal': '📓', 'ok': '👥', 'coub': '🎬', 'likee': '❤️',
    'rutube': '📹',
}
def emo(t): return EMOJI.get(t, '📱')

# ─────────────────────────────────────────────
# KEYBOARDS
# ─────────────────────────────────────────────

def main_kb(total, max_s=MAX_FREE):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add source", callback_data="add_source")],
        [InlineKeyboardButton("🚀 Direct connection", callback_data="direct_conn")],
        [InlineKeyboardButton("📡 Private/Channel/Group modes", callback_data="channel_mode")],
        [InlineKeyboardButton(f"📋 My feed [{total}]", callback_data="my_feed"),
         InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
        [InlineKeyboardButton("📞 Contact us", callback_data="contact"),
         InlineKeyboardButton("📖 History", callback_data="history")],
        [InlineKeyboardButton("📡 RSS generator", callback_data="rss_gen")],
        [InlineKeyboardButton("🎁 Referral program", callback_data="referral")],
        [InlineKeyboardButton("❓ How to use this bot ↗", callback_data="howto")],
        [InlineKeyboardButton("🗄 Local data collector", callback_data="local")],
        [InlineKeyboardButton("⭐ Premium subscription", callback_data="premium")],
        [InlineKeyboardButton("🔥 News and updates [04/19/2026] 🔥", callback_data="news")],
    ])

def settings_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📬 Delivery options", callback_data="s_delivery")],
        [InlineKeyboardButton("📺 Display options", callback_data="s_display")],
        [InlineKeyboardButton("🔍 Message filtering", callback_data="s_filter")],
        [InlineKeyboardButton("🎭 Moderation mode", callback_data="s_moderation")],
        [InlineKeyboardButton("🤵 Butler mode", callback_data="s_butler")],
        [InlineKeyboardButton("🤖 AI (LLM) settings", callback_data="s_ai")],
        [InlineKeyboardButton("🐦 Your Twitter accounts", callback_data="s_twitter")],
        [InlineKeyboardButton("🕐 Your timezone GMT+00:00", callback_data="s_timezone")],
        [InlineKeyboardButton("🌍 Change language", callback_data="s_language")],
        [InlineKeyboardButton("◀️ Back", callback_data="back_main")],
    ])

def channel_mode_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Turn off all data sources", callback_data="sources_off")],
        [InlineKeyboardButton("✅ Turn on all data sources", callback_data="sources_on")],
        [InlineKeyboardButton("◀️ Back", callback_data="back_main")],
    ])

def back_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_main")]])

# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────

class DB:
    def __init__(self, url):
        self.url = url
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(self.url)
        self.conn.autocommit = False
        logger.info("DB connected")

    def close(self):
        if self.conn:
            self.conn.close()

    def q(self, sql, p=None):
        with self.conn.cursor(cursor_factory=RealDictCursor) as c:
            try:
                c.execute(sql, p or ())
                self.conn.commit()
                return c.fetchall()
            except Exception as e:
                self.conn.rollback(); raise

    def q1(self, sql, p=None):
        with self.conn.cursor(cursor_factory=RealDictCursor) as c:
            try:
                c.execute(sql, p or ())
                self.conn.commit()
                return c.fetchone()
            except Exception as e:
                self.conn.rollback(); raise

    def run(self, sql, p=None):
        with self.conn.cursor() as c:
            try:
                c.execute(sql, p or ())
                self.conn.commit()
            except Exception as e:
                self.conn.rollback(); raise

    def ins(self, sql, p=None):
        with self.conn.cursor() as c:
            try:
                c.execute(sql, p or ())
                self.conn.commit()
                r = c.fetchone()
                return r[0] if r else None
            except Exception as e:
                self.conn.rollback(); raise

    def init_schema(self):
        self.run("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            username VARCHAR(255),
            is_premium BOOLEAN DEFAULT FALSE,
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
            is_active BOOLEAN DEFAULT TRUE,
            last_check TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sub_user_url
            ON subscriptions(user_id, source_url) WHERE user_id IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sub_channel_url
            ON subscriptions(channel_id, source_url) WHERE channel_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_subscriptions_channel ON subscriptions(channel_id);
        CREATE INDEX IF NOT EXISTS idx_posts_source ON posts(source_id);
        CREATE INDEX IF NOT EXISTS idx_sent_user ON sent_history(user_id);
        CREATE INDEX IF NOT EXISTS idx_users_telegram ON users(telegram_id);
        """)
        logger.info("Schema ready")

# ─────────────────────────────────────────────
# BOT
# ─────────────────────────────────────────────

class Bot:
    def __init__(self):
        self.db = DB(DATABASE_URL)
        self.db.connect()
        self.db.init_schema()
        self.app = Application.builder().token(BOT_TOKEN).build()
        self._setup()

    def _setup(self):
        add = self.app.add_handler
        add(CommandHandler("start",      self.cmd_start))
        add(CommandHandler("add",        self.cmd_add))
        add(CommandHandler("addchannel", self.cmd_addchannel))
        add(CommandHandler("remove",     self.cmd_remove))
        add(CommandHandler("list",       self.cmd_list))
        add(CommandHandler("feed",       self.cmd_feed))
        add(CommandHandler("history",    self.cmd_history))
        add(CommandHandler("connect",    self.cmd_connect))
        add(CommandHandler("channels",   self.cmd_connect))
        add(CommandHandler("help",       self.cmd_help))
        add(CommandHandler("admin",      self.cmd_admin))
        add(CallbackQueryHandler(self.on_cb))
        add(ChatMemberHandler(self.on_member, ChatMemberHandler.MY_CHAT_MEMBER))
        self.app.post_init = self._post_init

    async def _post_init(self, app):
        asyncio.create_task(self._poll_loop(app))
        logger.info("Polling started")

    # ── Helpers ──────────────────────────────

    def upsert_user(self, tg_id, username="user"):
        self.db.run(
            "INSERT INTO users (telegram_id, username) VALUES (%s,%s) ON CONFLICT DO NOTHING",
            (tg_id, username)
        )
        return self.db.q1("SELECT * FROM users WHERE telegram_id=%s", (tg_id,))

    def user_source_count(self, uid):
        r = self.db.q1("SELECT COUNT(*) AS c FROM subscriptions WHERE user_id=%s", (uid,))
        return r['c'] if r else 0

    def user_total(self, uid):
        """Personal + all channels owned"""
        personal = self.user_source_count(uid)
        chs = self.db.q("SELECT id FROM channels WHERE owner_id=%s", (uid,))
        ch_count = 0
        for ch in (chs or []):
            r = self.db.q1("SELECT COUNT(*) AS c FROM subscriptions WHERE channel_id=%s", (ch['id'],))
            ch_count += (r['c'] if r else 0)
        return personal + ch_count

    # ── Main menu text ────────────────────────

    def main_text(self, uid):
        total = self.user_total(uid)
        return (
            "🤖 <b>IN365Bot — Content Aggregator</b>\n\n"
            "Collect updates from RSS, YouTube, Instagram, TikTok, Reddit, and 15+ platforms.\n\n"
            f"📊 You have <b>{total}/{MAX_FREE}</b> data sources\n"
            "<i>Private + your channels + your groups subscriptions</i>"
        ), total

    # ── Callbacks ────────────────────────────

    async def on_cb(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        d = q.data
        u = update.effective_user
        row = self.upsert_user(u.id, u.username or "user")
        uid = row['id']

        if d == "back_main":
            text, total = self.main_text(uid)
            await q.edit_message_text(text, reply_markup=main_kb(total), parse_mode='HTML')

        elif d == "add_source":
            await q.edit_message_text(
                "➕ <b>Add a new source</b>\n\n"
                "Use /add command with a URL <b>or</b> platform + username:\n\n"
                "<b>By URL:</b>\n"
                "/add https://feeds.bbci.co.uk/news/rss.xml\n\n"
                "<b>By platform + username:</b>\n"
                "/add instagram therock\n"
                "/add tiktok charlidamelio\n"
                "/add twitter elonmusk\n"
                "/add youtube PewDiePie\n"
                "/add reddit apple\n"
                "/add telegram durov\n"
                "/add medium the-economist\n"
                "/add threads zuck\n"
                "/add facebook bmw\n"
                "/add twitch ninja\n"
                "/add vk durov\n"
                "/add livejournal tema\n"
                "/add coub oftheday\n"
                "/add ok smeysya\n"
                "/add likee kevwithin\n"
                "/add rutube 1627210",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "direct_conn":
            await q.edit_message_text(
                "🚀 <b>Direct connection</b>\n"
                "Connection status: [ — ]\n\n"
                "You can use your own or additional Telegram account to forward or resend messages. "
                "We highly recommend using a separate account.\n\n"
                "<b>Advantages of direct connection:</b>\n"
                "— Custom emojis and extended media caption (up to 4,096 symbols). "
                "The connected account should have ⭐ Telegram premium\n"
                "— Access to closed private channels and groups (without an invitation link)\n"
                "— Messages from other bots\n"
                "— Private chats with other users\n"
                "— Public channels support\n"
                "— Instant delivery with delay ~30 seconds\n"
                "— Media and files in original quality\n"
                "— Forwarding or resending messages\n"
                "— Filters, custom templates, translations, etc....\n\n"
                "<i>Please keep in mind that all messages will be sent from connected account!</i>\n\n"
                "<i>🔧 Direct connection (Userbot mode) coming in the next update!</i>",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "channel_mode":
            total = self.user_total(uid)
            await q.edit_message_text(
                "📡 <b>Private/Channel/Group modes</b>\n\n"
                f"You have total <b>{total}/{MAX_FREE}</b> data sources\n"
                "<i>Private + your channels + your groups subscriptions</i>\n\n"
                "• Add bot as admin to any channel/group\n"
                "• Use /connect to see registered channels\n"
                "• Use /addchannel &lt;chat_id&gt; &lt;URL&gt; to add sources",
                reply_markup=channel_mode_kb(), parse_mode='HTML'
            )

        elif d == "sources_off":
            self.db.run("UPDATE subscriptions SET is_active=FALSE WHERE user_id=%s", (uid,))
            await q.answer("❌ All personal sources turned OFF", show_alert=True)

        elif d == "sources_on":
            self.db.run("UPDATE subscriptions SET is_active=TRUE WHERE user_id=%s", (uid,))
            await q.answer("✅ All personal sources turned ON", show_alert=True)

        elif d == "my_feed":
            subs = self.db.q(
                "SELECT source_type, source_name, source_url, is_active "
                "FROM subscriptions WHERE user_id=%s ORDER BY created_at DESC", (uid,)
            )
            if not subs:
                text = "📋 <b>My feed [0]</b>\n\nNo sources yet.\nUse ➕ Add source to get started."
            else:
                text = f"📋 <b>My feed [{len(subs)}/{MAX_FREE}]</b>\n\n"
                for s in subs:
                    st = "✅" if s['is_active'] else "❌"
                    name = (s['source_name'] or s['source_url'])[:40]
                    text += f"{st} {emo(s['source_type'])} <b>{s['source_type'].upper()}</b> — {name}\n"
            await q.edit_message_text(text, reply_markup=back_kb(), parse_mode='HTML')

        elif d == "settings":
            await q.edit_message_text(
                "⚙️ <b>Settings</b>\n\nCustomize your bot experience:",
                reply_markup=settings_kb(), parse_mode='HTML'
            )

        elif d == "s_delivery":
            await q.edit_message_text(
                "📬 <b>Delivery options</b>\n\n"
                "• Polling interval: every 5 minutes\n"
                "• Max items per poll: 5 per source\n"
                "• Duplicate filtering: ✅ enabled\n"
                "• Auto-retry on failure: ✅ enabled\n\n"
                "<i>⭐ Instant delivery (30s) available with Premium</i>",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "s_display":
            await q.edit_message_text(
                "📺 <b>Display options</b>\n\n"
                "• Show source emoji: ✅ enabled\n"
                "• Show platform label: ✅ enabled\n"
                "• Show link preview: default Telegram behavior\n"
                "• Message format: emoji + title + link\n\n"
                "<i>⭐ Custom templates available with Premium</i>",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "s_filter":
            await q.edit_message_text(
                "🔍 <b>Message filtering</b>\n\n"
                "Filter posts by keywords, language, or similarity.\n\n"
                "<i>⭐ Message filtering is a Premium feature — coming soon!</i>",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "s_moderation":
            await q.edit_message_text(
                "🎭 <b>Moderation mode</b>\n\n"
                "Automatically moderate content in your channels and groups.\n\n"
                "<i>⭐ Moderation mode is a Premium feature — coming soon!</i>",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "s_butler":
            await q.edit_message_text(
                "🤵 <b>Butler mode</b>\n\n"
                "The bot will send you a daily/weekly digest instead of real-time posts.\n\n"
                "<i>⭐ Butler mode is a Premium feature — coming soon!</i>",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "s_ai":
            await q.edit_message_text(
                "🤖 <b>AI (LLM) settings</b>\n\n"
                "Use AI to summarize posts, translate content, or generate captions.\n\n"
                "<i>⭐ AI features are Premium — coming soon!</i>",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "s_twitter":
            await q.edit_message_text(
                "🐦 <b>Your Twitter accounts</b>\n\n"
                "Connect your Twitter/X account for better access and real-time tweets.\n\n"
                "<i>Currently using Nitter bridges. Direct API coming soon!</i>",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "s_timezone":
            await q.edit_message_text(
                "🕐 <b>Your timezone</b>\n\n"
                "Current: GMT+00:00\n\n"
                "<i>Timezone setting coming soon!</i>",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "s_language":
            await q.edit_message_text(
                "🌍 <b>Change language</b>\n\n"
                "Currently available: 🇬🇧 English\n\n"
                "<i>More languages coming soon!</i>",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "contact":
            await q.edit_message_text(
                "📞 <b>Contact us</b>\n\n"
                "Found a bug or have a suggestion?\n\n"
                "Use the thumbs down button on any response to send feedback,\n"
                "or contact the bot owner directly.",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "history":
            rows = self.db.q("""
                SELECT p.title, p.url, p.source_type, sh.sent_at
                FROM sent_history sh JOIN posts p ON sh.post_id = p.id
                WHERE sh.user_id = %s ORDER BY sh.sent_at DESC LIMIT 10
            """, (uid,))
            if not rows:
                text = "📖 <b>History</b>\n\nNo posts received yet."
            else:
                text = "📖 <b>History (last 10)</b>\n\n"
                for r in rows:
                    title = (r['title'] or 'No title')[:50]
                    text += f"{emo(r['source_type'])} <a href='{r['url']}'>{title}</a>\n"
            await q.edit_message_text(
                text, reply_markup=back_kb(), parse_mode='HTML',
                disable_web_page_preview=True
            )

        elif d == "rss_gen":
            await q.edit_message_text(
                "📡 <b>RSS Generator</b>\n\n"
                "Public RSS bridges used by this bot:\n\n"
                "• <a href='https://rsshub.app'>RSSHub</a> — 400+ platforms supported\n"
                "• <a href='https://nitter.net'>Nitter</a> — Twitter/X RSS\n"
                "• <a href='https://proxitok.pabloferreiro.es'>ProxiTok</a> — TikTok RSS\n"
                "• <a href='https://www.picuki.com'>Picuki</a> — Instagram viewer\n"
                "• Reddit native RSS — reddit.com/r/sub.rss\n"
                "• Medium native RSS — medium.com/feed/@user\n\n"
                "<i>Custom RSS generator for any website — coming soon!</i>",
                reply_markup=back_kb(), parse_mode='HTML',
                disable_web_page_preview=True
            )

        elif d == "referral":
            await q.edit_message_text(
                "🎁 <b>Referral program</b>\n\n"
                "Invite friends and earn bonus source slots!\n\n"
                "<i>Referral program launching soon!</i>",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "howto":
            await q.edit_message_text(
                "❓ <b>How to use this bot</b>\n\n"
                "<b>Add sources to your DM feed:</b>\n"
                "/add &lt;URL&gt; — subscribe by URL\n"
                "/add instagram username — shorthand\n\n"
                "<b>Manage your feed:</b>\n"
                "/list — see all subscriptions\n"
                "/remove &lt;URL&gt; — unsubscribe\n"
                "/feed — active feed summary\n"
                "/history — last 10 received posts\n\n"
                "<b>Post to channels/groups:</b>\n"
                "1. Add bot as admin to channel/group\n"
                "2. /connect — see connected channels with IDs\n"
                "3. /addchannel &lt;chat_id&gt; &lt;URL&gt;\n\n"
                "<b>15+ supported platforms:</b>\n"
                "📰 RSS  ▶️ YouTube  🐦 Twitter  📸 Instagram\n"
                "🎵 TikTok  🤖 Reddit  📝 Medium  ✈️ Telegram\n"
                "🧵 Threads  👤 Facebook  🎮 Twitch  💬 VK\n"
                "📓 Livejournal  👥 OK.ru  🎬 Coub  ❤️ Likee  📹 RuTube",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "local":
            await q.edit_message_text(
                "🗄 <b>Local data collector</b>\n\n"
                "Download and store your feed data locally.\n\n"
                "<i>Local data collector coming soon!</i>",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "premium":
            await q.edit_message_text(
                "⭐ <b>Premium subscription</b>\n\n"
                "<b>Free plan (current):</b>\n"
                f"• Up to {MAX_FREE} sources total\n"
                "• 5-minute update interval\n"
                "• 15+ platforms\n"
                "• Channel/Group posting\n\n"
                "<b>Premium plan (coming soon):</b>\n"
                "• Unlimited sources\n"
                "• 30-second instant delivery\n"
                "• Message filtering by keywords\n"
                "• Custom message templates\n"
                "• AI summarization & translation\n"
                "• Direct Telegram account connection\n"
                "• Moderation & Butler modes\n"
                "• Priority support",
                reply_markup=back_kb(), parse_mode='HTML'
            )

        elif d == "news":
            await q.edit_message_text(
                "🔥 <b>News and updates</b> 🔥\n\n"
                "🆕 <b>[04/19/2026]</b>\n"
                "✅ AximoBot-style inline menu\n"
                "✅ 15+ platforms: Reddit, Medium, Telegram, Threads,\n"
                "    Facebook, Twitch, VK, Livejournal, OK.ru, Coub, Likee, RuTube\n"
                "✅ /add platform username shorthand\n"
                "✅ Source counter (X/10)\n"
                "✅ Turn on/off all sources toggle\n"
                "✅ History command\n"
                "✅ Channel/Group posting system\n"
                "✅ Private group connection\n\n"
                "🔜 <b>Coming next:</b>\n"
                "• Direct connection (userbot mode)\n"
                "• Premium plan\n"
                "• Message filtering\n"
                "• AI summarization",
                reply_markup=back_kb(), parse_mode='HTML'
            )

    # ── Polling engine ────────────────────────

    async def _poll_loop(self, app):
        while True:
            try:
                await self._poll(app)
            except Exception as e:
                logger.error(f"Poll loop error: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    async def _poll(self, app):
        personal = self.db.q("""
            SELECT s.id, s.source_url, s.source_type,
                   u.telegram_id, u.id as user_id,
                   NULL::bigint as chan_chat_id, NULL::int as chan_id
            FROM subscriptions s JOIN users u ON s.user_id = u.id
            WHERE s.user_id IS NOT NULL AND s.channel_id IS NULL AND s.is_active=TRUE
            ORDER BY s.last_check ASC NULLS FIRST LIMIT 20
        """) or []
        channel = self.db.q("""
            SELECT s.id, s.source_url, s.source_type,
                   NULL::bigint as telegram_id, NULL::int as user_id,
                   c.chat_id as chan_chat_id, c.id as chan_id
            FROM subscriptions s JOIN channels c ON s.channel_id = c.id
            WHERE s.channel_id IS NOT NULL AND s.is_active=TRUE
            ORDER BY s.last_check ASC NULLS FIRST LIMIT 20
        """) or []

        for row in list(personal) + list(channel):
            try:
                posts = self._fetch(row['source_type'], row['source_url'])
                for post in posts:
                    await self._deliver(
                        app, row['user_id'], row['telegram_id'],
                        row['chan_id'], row['chan_chat_id'], post
                    )
                self.db.run("UPDATE subscriptions SET last_check=NOW() WHERE id=%s", (row['id'],))
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Poll error for {row.get('source_url')}: {e}")

    async def _deliver(self, app, uid, tg_id, chan_id, chan_chat, post):
        try:
            ex = self.db.q1("SELECT id FROM posts WHERE source_id=%s", (post['source_id'],))
            if ex:
                pid = ex['id']
                target_col = "user_id" if uid else "channel_id"
                target_val = uid if uid else chan_id
                if self.db.q1(
                    f"SELECT id FROM sent_history WHERE {target_col}=%s AND post_id=%s",
                    (target_val, pid)
                ):
                    return
            else:
                pid = self.db.ins("""
                    INSERT INTO posts (source_id, source_type, title, url, published_at, content_hash)
                    VALUES (%s,%s,%s,%s,%s,%s) RETURNING id
                """, (
                    post['source_id'], post['source_type'],
                    post['title'][:255], post['url'][:500],
                    post.get('published_at', datetime.now()), post.get('hash', '')
                ))

            e = emo(post['source_type'])
            text = (
                f"{e} <b>{post['source_type'].upper()}</b>\n\n"
                f"{post['title']}\n\n"
                f"<a href='{post['url']}'>🔗 Open</a>"
            )
            target = tg_id if tg_id else chan_chat
            if not target:
                return
            await app.bot.send_message(chat_id=target, text=text, parse_mode='HTML')
            self.db.run(
                "INSERT INTO sent_history (user_id, channel_id, post_id) VALUES (%s,%s,%s)",
                (uid, chan_id, pid)
            )
        except Exception as e:
            logger.error(f"Deliver error: {e}")

    # ── Source detection ──────────────────────

    PLATFORM_RESOLVERS = {
        'instagram':   lambda u: ('instagram',   f"https://www.instagram.com/{u}/"),
        'tiktok':      lambda u: ('tiktok',      f"https://www.tiktok.com/@{u}"),
        'twitter':     lambda u: ('twitter',     f"https://twitter.com/{u}"),
        'x':           lambda u: ('twitter',     f"https://twitter.com/{u}"),
        'youtube':     lambda u: ('youtube',     f"https://www.youtube.com/channel/{u}" if u.startswith('UC') else f"https://www.youtube.com/@{u}"),
        'reddit':      lambda u: ('reddit',      f"https://www.reddit.com/r/{u}"),
        'medium':      lambda u: ('medium',      f"https://medium.com/{u}" if u.startswith('/') else f"https://medium.com/@{u}"),
        'telegram':    lambda u: ('telegram',    f"https://t.me/{u}"),
        'threads':     lambda u: ('threads',     f"https://www.threads.net/@{u}"),
        'facebook':    lambda u: ('facebook',    f"https://www.facebook.com/{u}/"),
        'fb':          lambda u: ('facebook',    f"https://www.facebook.com/{u}/"),
        'twitch':      lambda u: ('twitch',      f"https://www.twitch.tv/{u}"),
        'vk':          lambda u: ('vk',          f"https://vk.com/{u}"),
        'livejournal': lambda u: ('livejournal', f"https://{u}.livejournal.com/"),
        'ok':          lambda u: ('ok',          f"https://ok.ru/{u}"),
        'coub':        lambda u: ('coub',        f"https://coub.com/{u}"),
        'likee':       lambda u: ('likee',       f"https://likee.video/@{u}"),
        'rutube':      lambda u: ('rutube',      f"https://rutube.ru/channel/{u}/"),
        'rss':         lambda u: ('rss',         u),
    }

    def detect(self, arg1: str, arg2: str = None):
        """Returns (source_type, resolved_url) or (None, None)"""
        low = arg1.lower().strip()
        # Shorthand: /add platform username
        if low in self.PLATFORM_RESOLVERS and arg2:
            return self.PLATFORM_RESOLVERS[low](arg2)
        # Full URL detection
        if "youtube.com" in low or "youtu.be" in low:
            return ('youtube', arg1)
        if "twitter.com" in low or "x.com" in low:
            return ('twitter', arg1)
        if "instagram.com" in low:
            return ('instagram', arg1)
        if "tiktok.com" in low:
            return ('tiktok', arg1)
        if "reddit.com" in low:
            return ('reddit', arg1)
        if "medium.com" in low:
            return ('medium', arg1)
        if "t.me/" in low:
            return ('telegram', arg1)
        if "threads.net" in low:
            return ('threads', arg1)
        if "facebook.com" in low or "fb.com" in low:
            return ('facebook', arg1)
        if "twitch.tv" in low:
            return ('twitch', arg1)
        if "vk.com" in low:
            return ('vk', arg1)
        if "livejournal.com" in low:
            return ('livejournal', arg1)
        if "ok.ru" in low:
            return ('ok', arg1)
        if "coub.com" in low:
            return ('coub', arg1)
        if "likee.video" in low:
            return ('likee', arg1)
        if "rutube.ru" in low:
            return ('rutube', arg1)
        if low.startswith("http") or low.endswith(".xml") or "rss" in low or "feed" in low:
            return ('rss', arg1)
        return (None, None)

    def _friendly_name(self, source_type, url):
        """Try to get a human-friendly name for the source"""
        try:
            if source_type == 'rss':
                feed = feedparser.parse(url)
                return feed.feed.get('title', url)[:100]
        except Exception:
            pass
        # Extract username from URL as fallback
        parts = url.rstrip('/').split('/')
        return parts[-1][:100] if parts else url[:100]

    # ── Fetchers ──────────────────────────────

    def _fetch(self, source_type, url):
        fn = {
            'rss':         self._rss,
            'youtube':     self._youtube,
            'twitter':     self._twitter,
            'instagram':   self._instagram,
            'tiktok':      self._tiktok,
            'reddit':      self._reddit,
            'medium':      self._medium,
            'telegram':    self._telegram_ch,
            'threads':     self._rsshub,
            'facebook':    self._rsshub,
            'twitch':      self._rsshub,
            'vk':          self._rsshub,
            'ok':          self._rsshub,
            'coub':        self._rsshub,
            'likee':       self._rsshub,
            'livejournal': self._livejournal,
            'rutube':      self._rutube,
        }.get(source_type)
        return fn(url) if fn else []

    def _make_post(self, st, entry, fallback_title="post"):
        return {
            'source_id':   entry.get('id', entry.get('link', '')),
            'source_type': st,
            'title':       entry.get('title', fallback_title)[:255],
            'url':         entry.get('link', ''),
            'published_at': datetime.now(),
            'hash':        entry.get('id', entry.get('link', '')),
        }

    def _rss(self, url):
        try:
            feed = feedparser.parse(url)
            return [self._make_post('rss', e) for e in feed.entries[:5]]
        except Exception as e:
            logger.error(f"RSS error: {e}"); return []

    def _youtube(self, url):
        try:
            m = re.search(r'youtube\.com/@([a-zA-Z0-9_-]+)', url)
            if m:
                handle = m.group(1)
                feed = feedparser.parse(f"https://www.youtube.com/feeds/videos.xml?user={handle}")
                if not feed.entries:
                    feed = feedparser.parse(f"https://www.youtube.com/feeds/videos.xml?channel_id={handle}")
            else:
                m2 = re.search(r'youtube\.com/(?:c|user|channel)/([a-zA-Z0-9_-]+)', url)
                if not m2: return []
                cid = m2.group(1)
                feed = feedparser.parse(f"https://www.youtube.com/feeds/videos.xml?channel_id={cid}")
                if not feed.entries:
                    feed = feedparser.parse(f"https://www.youtube.com/feeds/videos.xml?user={cid}")
            return [self._make_post('youtube', e, 'Video') for e in feed.entries[:5]]
        except Exception as e:
            logger.error(f"YouTube error: {e}"); return []

    def _twitter(self, url):
        try:
            m = re.search(r'(?:twitter|x)\.com/([a-zA-Z0-9_]+)', url)
            if not m: return []
            user = m.group(1)
            for inst in ["https://nitter.net", "https://nitter.privacydev.net", "https://nitter.poast.org"]:
                try:
                    feed = feedparser.parse(f"{inst}/{user}/rss")
                    if feed.entries:
                        return [self._make_post('twitter', e, 'Tweet') for e in feed.entries[:5]]
                except Exception:
                    continue
        except Exception as e:
            logger.error(f"Twitter error: {e}")
        return []

    def _instagram(self, url):
        posts = []
        try:
            m = re.search(r'instagram\.com/([a-zA-Z0-9_.]+)', url)
            if not m: return []
            user = m.group(1).rstrip('/')
            h = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            resp = requests.get(f"https://www.picuki.com/profile/{user}", headers=h, timeout=10)
            titles = re.findall(r'<div class="photo-description">(.*?)</div>', resp.text, re.DOTALL)
            links  = re.findall(r'href="(https://www\.picuki\.com/media/[^"]+)"', resp.text)
            for title, link in zip(titles[:5], links[:5]):
                clean = re.sub(r'<[^>]+>', '', title).strip()[:255] or f"Instagram post by @{user}"
                posts.append({
                    'source_id': f"ig_{user}_{link.split('/')[-1]}",
                    'source_type': 'instagram', 'title': clean,
                    'url': link, 'published_at': datetime.now(), 'hash': link
                })
        except Exception as e:
            logger.error(f"Instagram error: {e}")
        return posts

    def _tiktok(self, url):
        try:
            m = re.search(r'tiktok\.com/@([a-zA-Z0-9_.]+)', url)
            if not m: return []
            user = m.group(1)
            for inst in ["https://proxitok.pabloferreiro.es", "https://proxitok.privacyredirect.com"]:
                try:
                    feed = feedparser.parse(f"{inst}/@{user}/rss")
                    if feed.entries:
                        return [self._make_post('tiktok', e, f'TikTok @{user}') for e in feed.entries[:5]]
                except Exception:
                    continue
        except Exception as e:
            logger.error(f"TikTok error: {e}")
        return []

    def _reddit(self, url):
        try:
            m = re.search(r'reddit\.com/r/([a-zA-Z0-9_]+)', url)
            if not m: return []
            sub = m.group(1)
            feed = feedparser.parse(
                f"https://www.reddit.com/r/{sub}.rss",
                request_headers={'User-Agent': 'Mozilla/5.0'}
            )
            return [self._make_post('reddit', e, f'r/{sub} post') for e in feed.entries[:5]]
        except Exception as e:
            logger.error(f"Reddit error: {e}"); return []

    def _medium(self, url):
        try:
            m = re.search(r'medium\.com/@?([a-zA-Z0-9_-]+)', url)
            if not m: return []
            user = m.group(1)
            feed = feedparser.parse(f"https://medium.com/feed/@{user}")
            if not feed.entries:
                feed = feedparser.parse(f"https://medium.com/feed/{user}")
            return [self._make_post('medium', e, 'Medium post') for e in feed.entries[:5]]
        except Exception as e:
            logger.error(f"Medium error: {e}"); return []

    def _telegram_ch(self, url):
        try:
            m = re.search(r't\.me/([a-zA-Z0-9_]+)', url)
            if not m: return []
            ch = m.group(1)
            feed = feedparser.parse(f"https://rsshub.app/telegram/channel/{ch}")
            return [self._make_post('telegram', e, f'@{ch} post') for e in feed.entries[:5]]
        except Exception as e:
            logger.error(f"Telegram channel error: {e}"); return []

    def _livejournal(self, url):
        try:
            m = re.search(r'(?:https?://)?([a-zA-Z0-9_-]+)\.livejournal\.com', url)
            if not m: return []
            user = m.group(1)
            feed = feedparser.parse(f"https://{user}.livejournal.com/data/rss")
            return [self._make_post('livejournal', e, 'LJ post') for e in feed.entries[:5]]
        except Exception as e:
            logger.error(f"Livejournal error: {e}"); return []

    def _rutube(self, url):
        try:
            m = re.search(r'rutube\.ru/channel/(\d+)', url)
            if not m: return []
            cid = m.group(1)
            feed = feedparser.parse(f"https://rsshub.app/rutube/user/{cid}")
            return [self._make_post('rutube', e, 'RuTube video') for e in feed.entries[:5]]
        except Exception as e:
            logger.error(f"RuTube error: {e}"); return []

    def _rsshub(self, url):
        """Generic RSSHub fetcher for Threads, Facebook, Twitch, VK, OK, Coub, Likee"""
        try:
            low = url.lower()
            path, st = None, None
            if "threads.net" in low:
                st = 'threads'
                m = re.search(r'threads\.net/@([a-zA-Z0-9_.]+)', url)
                if m: path = f"threads/user/{m.group(1)}"
            elif "facebook.com" in low:
                st = 'facebook'
                m = re.search(r'facebook\.com/([a-zA-Z0-9_.]+)', url)
                if m: path = f"facebook/page/{m.group(1)}"
            elif "twitch.tv" in low:
                st = 'twitch'
                m = re.search(r'twitch\.tv/([a-zA-Z0-9_]+)', url)
                if m: path = f"twitch/blog/{m.group(1)}"
            elif "vk.com" in low:
                st = 'vk'
                m = re.search(r'vk\.com/([a-zA-Z0-9_]+)', url)
                if m: path = f"vk/user/{m.group(1)}"
            elif "ok.ru" in low:
                st = 'ok'
                m = re.search(r'ok\.ru/([a-zA-Z0-9_]+)', url)
                if m: path = f"okru/user/{m.group(1)}"
            elif "coub.com" in low:
                st = 'coub'
                m = re.search(r'coub\.com/([a-zA-Z0-9_-]+)', url)
                if m: path = f"coub/tag/{m.group(1)}"
            elif "likee.video" in low:
                st = 'likee'
                m = re.search(r'likee\.video/@([a-zA-Z0-9_.]+)', url)
                if m: path = f"likee/user/{m.group(1)}"
            if not path or not st:
                return []
            feed = feedparser.parse(f"https://rsshub.app/{path}")
            return [self._make_post(st, e, f'{st} post') for e in feed.entries[:5]]
        except Exception as e:
            logger.error(f"RSSHub error for {url}: {e}"); return []

    # ── Channel auto-register ─────────────────

    async def on_member(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        res = update.my_chat_member
        chat = res.chat
        status = res.new_chat_member.status
        if status in ("administrator", "member"):
            added = res.from_user
            if not added: return
            try:
                row = self.upsert_user(added.id, added.username or "user")
                self.db.run("""
                    INSERT INTO channels (owner_id, chat_id, chat_title, chat_type)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (chat_id) DO UPDATE SET chat_title=EXCLUDED.chat_title
                """, (row['id'], chat.id, chat.title or "Unknown", chat.type))
                await ctx.bot.send_message(
                    chat_id=chat.id,
                    text=(
                        f"✅ <b>Bot connected to {chat.title}!</b>\n\n"
                        f"Now in your private chat with me, use:\n"
                        f"<code>/addchannel {chat.id} &lt;URL&gt;</code>\n"
                        "to add sources to this channel/group."
                    ),
                    parse_mode='HTML'
                )
            except Exception as e:
                logger.error(f"on_member error: {e}")
        elif status in ("kicked", "left"):
            try:
                self.db.run("DELETE FROM channels WHERE chat_id=%s", (chat.id,))
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    # ── Commands ──────────────────────────────

    async def cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        u = update.effective_user
        row = self.upsert_user(u.id, u.username or "user")
        text, total = self.main_text(row['id'])
        await update.message.reply_text(text, reply_markup=main_kb(total), parse_mode='HTML')

    async def cmd_add(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not ctx.args:
            await update.message.reply_text(
                "Usage: /add &lt;URL&gt;\nor: /add instagram username",
                parse_mode='HTML'
            )
            return
        u = update.effective_user
        row = self.upsert_user(u.id, u.username or "user")
        uid = row['id']

        # Check limit
        if self.user_source_count(uid) >= MAX_FREE:
            await update.message.reply_text(
                f"❌ You've reached the {MAX_FREE}-source free limit.\n"
                "Use /remove &lt;URL&gt; to free a slot, or upgrade to ⭐ Premium.",
                parse_mode='HTML'
            )
            return

        arg1 = ctx.args[0]
        arg2 = ctx.args[1] if len(ctx.args) > 1 else None
        st, url = self.detect(arg1, arg2)
        if not st:
            await update.message.reply_text(
                "❌ Platform not recognized.\nTap ➕ Add source from the menu for examples."
            )
            return

        name = self._friendly_name(st, url)
        try:
            self.db.run(
                "INSERT INTO subscriptions (user_id, source_url, source_type, source_name) "
                "VALUES (%s,%s,%s,%s)",
                (uid, url, st, name)
            )
            count = self.user_source_count(uid)
            await update.message.reply_text(
                f"✅ {emo(st)} Added <b>{st.upper()}</b> — {name}\n"
                f"📊 Sources: {count}/{MAX_FREE}",
                parse_mode='HTML'
            )
        except psycopg2.IntegrityError:
            self.db.conn.rollback()
            await update.message.reply_text("Already subscribed to this source.")
        except Exception as e:
            logger.error(f"Add error: {e}")
            await update.message.reply_text("Error adding source.")

    async def cmd_remove(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if not ctx.args:
            await update.message.reply_text("Usage: /remove &lt;URL&gt;", parse_mode='HTML')
            return
        u = update.effective_user
        row = self.upsert_user(u.id, u.username or "user")
        url = ctx.args[0]
        try:
            self.db.run(
                "DELETE FROM subscriptions WHERE user_id=%s AND source_url=%s",
                (row['id'], url)
            )
            await update.message.reply_text("✅ Removed.")
        except Exception as e:
            logger.error(f"Remove error: {e}")
            await update.message.reply_text("Error removing source.")

    async def cmd_list(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        u = update.effective_user
        row = self.upsert_user(u.id, u.username or "user")
        subs = self.db.q(
            "SELECT source_type, source_name, source_url, is_active "
            "FROM subscriptions WHERE user_id=%s ORDER BY created_at DESC",
            (row['id'],)
        )
        if not subs:
            await update.message.reply_text("No subscriptions yet. Use /add to subscribe.")
            return
        text = f"📋 <b>Your Subscriptions ({len(subs)}/{MAX_FREE})</b>\n\n"
        for s in subs:
            st = "✅" if s['is_active'] else "❌"
            name = (s['source_name'] or s['source_url'])[:40]
            text += f"{st} {emo(s['source_type'])} <b>{s['source_type'].upper()}</b> — {name}\n"
        await update.message.reply_text(text, parse_mode='HTML')

    async def cmd_feed(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await self.cmd_list(update, ctx)

    async def cmd_history(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        u = update.effective_user
        row = self.upsert_user(u.id, u.username or "user")
        rows = self.db.q("""
            SELECT p.title, p.url, p.source_type, sh.sent_at
            FROM sent_history sh JOIN posts p ON sh.post_id = p.id
            WHERE sh.user_id=%s ORDER BY sh.sent_at DESC LIMIT 10
        """, (row['id'],))
        if not rows:
            await update.message.reply_text("📖 No history yet.")
            return
        text = "📖 <b>History (last 10)</b>\n\n"
        for r in rows:
            title = (r['title'] or 'No title')[:50]
            text += f"{emo(r['source_type'])} <a href='{r['url']}'>{title}</a>\n"
        await update.message.reply_text(text, parse_mode='HTML', disable_web_page_preview=True)

    async def cmd_connect(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        u = update.effective_user
        row = self.upsert_user(u.id, u.username or "user")
        chs = self.db.q(
            "SELECT chat_id, chat_title, chat_type FROM channels WHERE owner_id=%s",
            (row['id'],)
        )
        if not chs:
            text = (
                "📡 <b>No channels/groups connected yet.</b>\n\n"
                "Steps to connect:\n"
                "1. Add this bot as admin to your channel/group\n"
                "2. Bot auto-registers it and confirms\n"
                "3. Come back here — you'll see the chat_id\n"
                "4. Use /addchannel &lt;chat_id&gt; &lt;URL&gt; to add sources"
            )
        else:
            text = "📡 <b>Your Connected Channels/Groups:</b>\n\n"
            for ch in chs:
                subs = self.db.q1(
                    "SELECT COUNT(*) AS c FROM subscriptions s "
                    "JOIN channels c2 ON s.channel_id=c2.id WHERE c2.chat_id=%s",
                    (ch['chat_id'],)
                )
                count = subs['c'] if subs else 0
                text += (
                    f"• <b>{ch['chat_title']}</b> [{ch['chat_type']}]\n"
                    f"  ID: <code>{ch['chat_id']}</code>  |  Sources: {count}\n\n"
                )
            text += "Use /addchannel &lt;chat_id&gt; &lt;URL&gt; to add sources"
        await update.message.reply_text(text, parse_mode='HTML')

    async def cmd_addchannel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if len(ctx.args) < 2:
            await update.message.reply_text(
                "Usage: /addchannel &lt;chat_id&gt; &lt;URL&gt;\n\n"
                "Get your chat_id from /connect",
                parse_mode='HTML'
            )
            return
        u = update.effective_user
        try:
            chat_id = int(ctx.args[0])
        except ValueError:
            await update.message.reply_text("Invalid chat_id — must be a number like -1001234567890")
            return

        url = ctx.args[1]
        arg2 = ctx.args[2] if len(ctx.args) > 2 else None
        st, resolved = self.detect(url, arg2)
        if not st:
            await update.message.reply_text("❌ URL not supported.")
            return

        row = self.upsert_user(u.id, u.username or "user")
        ch = self.db.q1(
            "SELECT id FROM channels WHERE chat_id=%s AND owner_id=%s",
            (chat_id, row['id'])
        )
        if not ch:
            await update.message.reply_text(
                "❌ Channel not found or you're not the owner.\n"
                "Make sure you added the bot as admin first, then check /connect."
            )
            return

        name = self._friendly_name(st, resolved)
        try:
            self.db.run(
                "INSERT INTO subscriptions (channel_id, source_url, source_type, source_name) "
                "VALUES (%s,%s,%s,%s)",
                (ch['id'], resolved, st, name)
            )
            await update.message.reply_text(
                f"✅ {emo(st)} Added <b>{st.upper()}</b> to channel — {name}",
                parse_mode='HTML'
            )
        except psycopg2.IntegrityError:
            self.db.conn.rollback()
            await update.message.reply_text("Already subscribed to this source in that channel.")
        except Exception as e:
            logger.error(f"addchannel error: {e}")
            await update.message.reply_text("Error adding source to channel.")

    async def cmd_help(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "❓ <b>How to use IN365Bot</b>\n\n"
            "<b>Personal (DM):</b>\n"
            "/add &lt;URL&gt; — subscribe by URL\n"
            "/add instagram username — shorthand\n"
            "/remove &lt;URL&gt; — unsubscribe\n"
            "/list — your subscriptions\n"
            "/history — last 10 received posts\n\n"
            "<b>Channel/Group:</b>\n"
            "1. Add bot as admin\n"
            "2. /connect — see registered channels\n"
            "3. /addchannel &lt;chat_id&gt; &lt;URL&gt;\n\n"
            "<b>15+ platforms:</b>\n"
            "📰 rss  ▶️ youtube  🐦 twitter  📸 instagram\n"
            "🎵 tiktok  🤖 reddit  📝 medium  ✈️ telegram\n"
            "🧵 threads  👤 facebook  🎮 twitch  💬 vk\n"
            "📓 livejournal  👥 ok  🎬 coub  ❤️ likee  📹 rutube\n\n"
            "Updates checked every 5 minutes.",
            parse_mode='HTML'
        )

    async def cmd_admin(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("Access denied.")
            return
        try:
            u = self.db.q1("SELECT COUNT(*) AS c FROM users")
            s = self.db.q1("SELECT COUNT(*) AS c FROM subscriptions")
            c = self.db.q1("SELECT COUNT(*) AS c FROM channels")
            p = self.db.q1("SELECT COUNT(*) AS c FROM posts")
            sh = self.db.q1("SELECT COUNT(*) AS c FROM sent_history")
            await update.message.reply_text(
                f"📊 <b>Bot Stats</b>\n\n"
                f"Users: {u['c']}\nSubscriptions: {s['c']}\n"
                f"Channels: {c['c']}\nPosts tracked: {p['c']}\nSent: {sh['c']}",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Admin error: {e}")
            await update.message.reply_text("Error.")

    def run(self):
        logger.info("Starting IN365Bot...")
        try:
            self.app.run_polling(
                allowed_updates=['message', 'callback_query', 'my_chat_member']
            )
        finally:
            self.db.close()


if __name__ == "__main__":
    Bot().run()